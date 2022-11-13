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
		private readonly AsyncReaderWriterLock asyncReaderWriterLock = new AsyncReaderWriterLock();

		public SequentialAccessAsyncDictionary(ISwapAllocator allocator)
		{
			this.allocator = allocator ?? throw new ArgumentNullException(nameof(allocator));
		}

		private Task<Func<Task<byte[]>>> current;


		public async Task<string> Read(string key){
			if (key is null)
			{
				throw new ArgumentNullException(nameof(key));
			}
			await asyncReaderWriterLock.AcquireReaderLock();
			try{
				if (current is { }){
					byte[] bytes1 = await (await current)();
					using Stream deflateStream = new DeflateStream(new MemoryStream(bytes1, 0, bytes1.Length, false, false), CompressionMode.Decompress, false);
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
				using MemoryStream output = new MemoryStream();
				using (Stream outputDeflateStream = new DeflateStream(output, CompressionLevel.Optimal, false)){
					BsonDataWriter bsonDataWriter = new BsonDataWriter(outputDeflateStream);
					GC.SuppressFinalize(bsonDataWriter);
					bsonDataWriter.WriteStartArray();

					if (current is { })
					{
						byte[] bytes1 = await (await current)();
						using Stream input = new DeflateStream(new MemoryStream(bytes1, 0, bytes1.Length, false, false), CompressionMode.Decompress, false);
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
					if(value is { }){
						bsonDataWriter.WriteValue(key);
						bsonDataWriter.WriteValue(value);
					}
					bsonDataWriter.WriteEndArray();
				}
				current = allocator.Write(output.ToArray());
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
		private IAsyncDictionary GetUnderlying(string key){
			int hash = ("Lesbians are cute " + key).GetHashCode() % shards.Length;
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
	public sealed class CachedAsyncDictionary : IAsyncDictionary{
		private readonly ConcurrentDictionary<string, CacheLine> cache = new ConcurrentDictionary<string, CacheLine>();
		private readonly AsyncReaderWriterLock asyncReaderWriterLock = new AsyncReaderWriterLock();
		private readonly IAsyncDictionary underlying;
		private readonly long softMemoryLimit;
		private static Task RandomWait()
		{
			//Prevents unwanted synchronization
			Span<byte> bytes = stackalloc byte[1];
			RandomNumberGenerator.Fill(bytes);
			return Task.Delay(bytes[0] + 1);
		}
		private static async void Collect(WeakReference<CachedAsyncDictionary> weakReference){
		start:
			await RandomWait();
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
						KeyValuePair<string, CacheLine> kvp = keyValuePairs[RandomNumberGenerator.GetInt32(0, limit)];
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

					while(flushes.TryDequeue(out Task tsk)){
						await tsk;
					}
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
