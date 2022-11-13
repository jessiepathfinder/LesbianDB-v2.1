using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using LesbianDB;


//LesbianDB Optimistic Applications Framework
namespace LesbianDB.Optimism.Core
{
	public interface IOptimisticExecutionScope{
		public Task<string> Read(string key);
		//NOTE: the write method is not asynchromous since writes are cached in-memory until commit
		public void Write(string key, string value);
	}
	public sealed class OptimisticExecutionManager{
		private readonly AsyncReaderWriterLock cachelock = new AsyncReaderWriterLock();
		private readonly IDatabaseEngine databaseEngine;
		private readonly ConcurrentDictionary<string, string> optimisticCache = new ConcurrentDictionary<string, string>();
		private readonly struct OptimisticCachePartition{
			private readonly AsyncReaderWriterLock asyncReaderWriterLock;
			private readonly Dictionary<string, string> dictionary;
			public OptimisticCachePartition(AsyncReaderWriterLock asyncReaderWriterLock, Dictionary<string, string> dictionary)
			{
				this.asyncReaderWriterLock = asyncReaderWriterLock;
				this.dictionary = dictionary;
			}
			public async Task Clear(){
				await asyncReaderWriterLock.AcquireWriterLock();
				try{
					dictionary.Clear();
				} finally{
					asyncReaderWriterLock.ReleaseWriterLock();
				}
			}
			public async Task Set(string key, string value){
				await asyncReaderWriterLock.AcquireWriterLock();
				try
				{
					dictionary[key] = value;
				}
				finally
				{
					asyncReaderWriterLock.ReleaseWriterLock();
				}
			}
			public async Task<bool> TryAdd(string key, string value)
			{
				await asyncReaderWriterLock.AcquireWriterLock();
				try
				{
					return dictionary.TryAdd(key, value);
				}
				finally
				{
					asyncReaderWriterLock.ReleaseWriterLock();
				}
			}
			public async Task<OptimisticCacheReadResult> Get(string key)
			{
				await asyncReaderWriterLock.AcquireReaderLock();
				try
				{
					bool found = dictionary.TryGetValue(key, out string value);
					return new OptimisticCacheReadResult(found, value);
				}
				finally
				{
					asyncReaderWriterLock.ReleaseReaderLock();
				}
			}
		}
		private readonly struct OptimisticCacheReadResult{
			public readonly bool found;
			public readonly string value;

			public OptimisticCacheReadResult(bool found, string value)
			{
				this.found = found;
				this.value = value;
			}
		}

		private static ushort Random2()
		{
			//Prevents unwanted synchronization
			Span<byte> bytes = stackalloc byte[2];
			RandomNumberGenerator.Fill(bytes);
			return BitConverter.ToUInt16(bytes);
		}
		private readonly OptimisticCachePartition[] optimisticCachePartitions = new OptimisticCachePartition[256];
		private static async void Collect(WeakReference<OptimisticCachePartition[]> weakReference, long softMemoryLimit){
		start:
			ushort random = Random2();
			await Task.Delay((random / 256) + 1);
			if(Misc.thisProcess.VirtualMemorySize64 < softMemoryLimit){
				goto start;
			}
			if(weakReference.TryGetTarget(out OptimisticCachePartition[] optimisticCachePartitions)){
				await optimisticCachePartitions[random % 256].Clear();
				goto start;
			}
		}
		public OptimisticExecutionManager(IDatabaseEngine databaseEngine, long softMemoryLimit)
		{
			this.databaseEngine = databaseEngine ?? throw new ArgumentNullException(nameof(databaseEngine));
			for(int i = 0; i < 256; ){
				optimisticCachePartitions[i++] = new OptimisticCachePartition(new AsyncReaderWriterLock(), new Dictionary<string, string>());
			}
			Collect(new WeakReference<OptimisticCachePartition[]>(optimisticCachePartitions, false), softMemoryLimit);
		}
		private OptimisticCachePartition GetOptimisticCachePartition(string key)
		{
			return optimisticCachePartitions[("Lesbians are optimistic " + key).GetHashCode() & 255];
		}
		private sealed class OptimisticExecutionScope : IOptimisticExecutionScope
		{
			private static readonly IReadOnlyDictionary<string, string> emptyStringDictionary = new Dictionary<string, string>();
			private static readonly string[] emptyStringArray = new string[0];
			private readonly OptimisticCachePartition[] optimisticCachePartitions;
			private readonly IDatabaseEngine databaseEngine;
			public readonly ConcurrentDictionary<string, string> L1ReadCache = new ConcurrentDictionary<string, string>();
			public readonly ConcurrentDictionary<string, string> L1WriteCache = new ConcurrentDictionary<string, string>();
			private readonly bool readOnly;

			public OptimisticExecutionScope(OptimisticCachePartition[] optimisticCachePartitions, IDatabaseEngine databaseEngine, bool readOnly)
			{
				this.optimisticCachePartitions = optimisticCachePartitions;
				this.databaseEngine = databaseEngine;
				this.readOnly = readOnly;
			}

			private OptimisticCachePartition GetOptimisticCachePartition(string key)
			{
				return optimisticCachePartitions[("Lesbians are optimistic " + key).GetHashCode() & 255];
			}


			public Task<string> Read(string key)
			{
			start1:
				if(L1WriteCache.TryGetValue(key, out string value)){
					return Task.FromResult(value);
				} else if (L1ReadCache.TryGetValue(key, out value))
				{
					return Task.FromResult(value);
				} else{
					return ChainedRead(key, readOnly ? ReadUnderlying(key) : ReadAsync(key));
				}
			}
			private async Task<string> ChainedRead(string key, Task<string> tsk){
				string value = L1ReadCache.GetOrAdd(key, await tsk);
				if (L1WriteCache.TryGetValue(key, out string value2))
				{
					return value2;
				} else{
					return value;
				}
			}
			private async Task<string> ReadUnderlying(string key){
				return (await databaseEngine.Execute(new string[] { key }, emptyStringDictionary, emptyStringDictionary))[key];
			}
			private async Task<string> ReadAsync(string key){
				OptimisticCachePartition optimisticCachePartition = GetOptimisticCachePartition(key);
			start:
				OptimisticCacheReadResult optimisticCacheReadResult = await optimisticCachePartition.Get(key);
				if(optimisticCacheReadResult.found){
					return optimisticCacheReadResult.value;
				} else{
					string value = await ReadUnderlying(key);
					if(await optimisticCachePartition.TryAdd(key, value)){
						return value;
					} else{
						goto start;
					}
				}
			}

			public void Write(string key, string value)
			{
				if (readOnly)
				{
					throw new InvalidOperationException("Writing is not allowed in read-only optimistic function");
				}
				else{
					L1WriteCache[key] = value;
				}
			}
		}
		public async Task<T> ExecuteOptimisticFunction<T>(Func<IOptimisticExecutionScope, Task<T>> optimisticFunction, bool readOnly){
			OptimisticExecutionScope optimisticExecutionScope = new OptimisticExecutionScope(optimisticCachePartitions, databaseEngine, readOnly);
		start:
			T ret = await optimisticFunction(optimisticExecutionScope);
			if(readOnly){
				return ret;
			}
			//Conditions
			Dictionary<string, string> reads = new Dictionary<string, string>(optimisticExecutionScope.L1ReadCache.ToArray());

			//Effects
			Dictionary<string, string> writes = new Dictionary<string, string>();
			foreach(KeyValuePair<string, string> kvp in optimisticExecutionScope.L1WriteCache.ToArray()){
				string key = kvp.Key;
				string value = kvp.Value;
				if(reads.TryGetValue(key, out string old)){
					if(old == value){
						continue;
					}
				}
				writes.Add(key, value);
			}

			IReadOnlyDictionary<string, string> updated = await databaseEngine.Execute(reads.Keys, reads, writes);
			bool success = true;
			foreach(KeyValuePair<string, string> kvp in updated){
				string key = kvp.Key;
				string value = kvp.Value;
				if(!readOnly){
					await GetOptimisticCachePartition(key).Set(key, value);
				}
				success &= reads[key] == value;
			}
			if (success)
			{
				return ret;
			}
			else
			{
				optimisticExecutionScope.L1WriteCache.Clear();
				foreach (KeyValuePair<string, string> kvp in updated)
				{
					optimisticExecutionScope.L1ReadCache[kvp.Key] = kvp.Value;
				}
				goto start;
			}
		}
	}

}
