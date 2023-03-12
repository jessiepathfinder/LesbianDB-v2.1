using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Diagnostics;

namespace LesbianDB
{
	public interface IAsyncDictionary
	{
		public Task<string> Read(string key);
		public Task Write(string key, string value);
	}
	public sealed class SequentialAccessAsyncDictionary : IAsyncDictionary{
		private readonly ISwapAllocator allocator;
		private readonly CompressionLevel compressionLevel;
		private readonly AsyncReaderWriterLock asyncReaderWriterLock = new AsyncReaderWriterLock();

		public SequentialAccessAsyncDictionary(ISwapAllocator allocator, CompressionLevel compressionLevel = CompressionLevel.Optimal)
		{
			this.allocator = allocator ?? throw new ArgumentNullException(nameof(allocator));
			this.compressionLevel = compressionLevel;
		}

		private Task<Func<Task<PooledReadOnlyMemoryStream>>> current;


		public async Task<string> Read(string key){
			if (key is null)
			{
				throw new ArgumentNullException(nameof(key));
			}
			await asyncReaderWriterLock.AcquireReaderLock();
			try{
				if (current is { }){
					using Stream deflateStream = new DeflateStream(await (await current)(), CompressionMode.Decompress, false);
					BsonDataReader bsonDataReader = new BsonDataReader(deflateStream, true, DateTimeKind.Unspecified);
					GC.SuppressFinalize(bsonDataReader);
					bsonDataReader.Read();
					while (true)
					{
						string temp = bsonDataReader.ReadAsString();

						if (temp is null)
						{
							break;
						}
						else if (temp == key)
						{
							return bsonDataReader.ReadAsString();
						} else{
							bsonDataReader.Read();
						}
					}
				}
			} finally{
				asyncReaderWriterLock.ReleaseReaderLock();
			}
			return null;
		}
		public async Task Write(string key, string value){
			if(key is null){
				throw new ArgumentNullException(nameof(key));
			}
			await asyncReaderWriterLock.AcquireWriterLock();
			try
			{
				using PooledMemoryStream output = new PooledMemoryStream(Misc.arrayPool);
				using (Stream outputDeflateStream = new DeflateStream(output, compressionLevel, true)){
					BsonDataWriter bsonDataWriter = new BsonDataWriter(outputDeflateStream);
					GC.SuppressFinalize(bsonDataWriter);
					bsonDataWriter.WriteStartArray();
					if (value is { })
					{
						bsonDataWriter.WriteValue(key);
						bsonDataWriter.WriteValue(value);
					}
					if (current is { })
					{
						using Stream input = new DeflateStream(await (await current)(), CompressionMode.Decompress, false);
						BsonDataReader bsonDataReader = new BsonDataReader(input, true, DateTimeKind.Unspecified);
						GC.SuppressFinalize(bsonDataReader);
						bsonDataReader.Read();
						while (true)
						{
							string temp = bsonDataReader.ReadAsString();

							if (temp is null){
								break;
							} else if (temp == key)
							{
								bsonDataReader.Read();
							}
							else
							{
								bsonDataWriter.WriteValue(temp);
								bsonDataWriter.WriteValue(bsonDataReader.ReadAsString());
							}
						}
					}
					bsonDataWriter.WriteEndArray();
				}
				current = allocator.Write(output.GetBuffer().AsMemory(0, (int)output.Position));
			}
			finally
			{
				asyncReaderWriterLock.ReleaseWriterLock();
			}
		}
	}
	public sealed class ShardedAsyncDictionary : IAsyncDictionary{
		private readonly IAsyncDictionary[] shards;
		public ShardedAsyncDictionary(Func<IAsyncDictionary> factory, int count){
			if(count < 1){
				throw new ArgumentOutOfRangeException("ShardedAsyncDictionary needs at least 1 shard");
			}
			shards = new IAsyncDictionary[count];
			for(int i = 0; i < count; ){
				shards[i++] = factory();
			}
		}
		private static async void Collect(WeakReference<IAsyncDictionary[]> weakReference, long memorylimit, int count)
		{
		start:
			await Task.Delay(1);
			if(weakReference.TryGetTarget(out IAsyncDictionary[] asyncDictionaries)){
				if(Misc.thisProcess.VirtualMemorySize64 > memorylimit){
					await ((IFlushableAsyncDictionary)asyncDictionaries[Misc.FastRandom(0, count)]).Flush();
				}
				goto start;
			}
		}
		public ShardedAsyncDictionary(Func<IFlushableAsyncDictionary> factory, int count, long memorylimit) : this(factory, count){
			Collect(new WeakReference<IAsyncDictionary[]>(shards, false), memorylimit, count);
		}
		private IAsyncDictionary GetUnderlying(string key){
			int hash = ("asmr yuri lesbian neck kissing" + key).GetHashCode() % shards.Length;
			if(hash < 0){
				return shards[-hash];
			} else{
				return shards[hash];
			}
		}

		public Task<string> Read(string key)
		{
			return GetUnderlying(key).Read(key);
		}

		public Task Write(string key, string value)
		{
			return GetUnderlying(key).Write(key, value);
		}
	}
	public interface IFlushableAsyncDictionary : IAsyncDictionary{
		/// <summary>
		/// Flushes all dirty keys to an underlying medium.
		/// </summary>
		public Task Flush();
	}
	public sealed class CachedAsyncDictionary : IFlushableAsyncDictionary{
		private readonly ConcurrentDictionary<string, CacheLine> cache = new ConcurrentDictionary<string, CacheLine>();
		private readonly AsyncReaderWriterLock asyncReaderWriterLock = new AsyncReaderWriterLock();
		private readonly IAsyncDictionary underlying;
		private readonly long softMemoryLimit;
		public async Task Flush(){
			await asyncReaderWriterLock.AcquireWriterLock();
			try{
				Queue<Task> tasks = new Queue<Task>();
				foreach(KeyValuePair<string, CacheLine> keyValuePair in cache.ToArray()){
					CacheLine value = keyValuePair.Value;
					if(value.dirty){
						string key = keyValuePair.Key;
						tasks.Enqueue(underlying.Write(key, value.value));

						//Unmark as dirty to avoid double flushing of the same value
						cache[key] = new CacheLine(false, value.value);
					}
				}
				await tasks.ToArray();
			} finally{
				asyncReaderWriterLock.ReleaseWriterLock();
			}
		}
		private static async void Collect(WeakReference<CachedAsyncDictionary> weakReference){
			AsyncManagedSemaphore asyncManagedSemaphore = new AsyncManagedSemaphore(0);
			Misc.RegisterGCListenerSemaphore(asyncManagedSemaphore);
		start:
			await asyncManagedSemaphore.Enter();
			if (weakReference.TryGetTarget(out CachedAsyncDictionary _this)){
				if(Misc.thisProcess.VirtualMemorySize64 < _this.softMemoryLimit){
					//No cache eviction until we hit memory limit
					goto start;
				}
				await _this.asyncReaderWriterLock.AcquireWriterLock();
				try
				{
					KeyValuePair<string, CacheLine>[] keyValuePairs = _this.cache.ToArray();
					int limit = keyValuePairs.Length;
					if(limit < 2){
						goto start;
					}
					Dictionary<string, CacheLine> select = new Dictionary<string, CacheLine>();
					while (select.Count * 100 < limit)
					{
						KeyValuePair<string, CacheLine> kvp = keyValuePairs[Misc.FastRandom(0, limit)];
						select.TryAdd(kvp.Key, kvp.Value);
					}
					Queue<Task> flushes = new Queue<Task>();
					foreach(string key in select.Keys){
						CacheLine cacheLine = select[key];
						if(cacheLine.dirty){
							flushes.Enqueue(_this.underlying.Write(key, cacheLine.value));
						}
						_this.cache.TryRemove(key, out _);
					}

					await flushes.ToArray();
				}
				finally
				{
					_this.asyncReaderWriterLock.ReleaseWriterLock();
				}
				goto start;
			}
		}

		public async Task<string> Read(string key)
		{
			await asyncReaderWriterLock.AcquireReaderLock();
			try{
			start:
				if(cache.TryGetValue(key, out CacheLine cacheLine)){
					return cacheLine.value;
				} else{
					string val = await underlying.Read(key);
					cacheLine = new CacheLine(false, val);
					//Optimistic locking
					if(cache.TryAdd(key, cacheLine)){
						return val;
					} else{
						goto start;
					}
				}
			} finally{
				asyncReaderWriterLock.ReleaseReaderLock();
			}
		}

		public Task Write(string key, string value)
		{
			CacheLine cacheLine = new CacheLine(true, value);
			//Optimization: if it doesn't exist in the cache, it can be added
			//synchronously, since the lock is only used to protect us from
			//the cache garbage collector
			if (cache.TryAdd(key, cacheLine)){
				return Misc.completed;
			} else{
				return Write2(key, cacheLine);
			}
		}
		private async Task Write2(string key, CacheLine cacheLine){
			await asyncReaderWriterLock.AcquireReaderLock();
			try
			{
				cache[key] = cacheLine;
			}
			finally
			{
				asyncReaderWriterLock.ReleaseReaderLock();
			}
		}

		public CachedAsyncDictionary(IAsyncDictionary underlying, long softMemoryLimit = 1073741824)
		{
			this.softMemoryLimit = softMemoryLimit;
			this.underlying = underlying ?? throw new ArgumentNullException(nameof(underlying));
			Collect(new WeakReference<CachedAsyncDictionary>(this, false));
		}

		private readonly struct CacheLine{
			public readonly bool dirty;
			public readonly string value;

			public CacheLine(bool dirty, string value)
			{
				this.dirty = dirty;
				this.value = value;
			}
		}
	}
}
