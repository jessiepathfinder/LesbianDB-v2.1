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
		private readonly IDatabaseEngine databaseEngine;

		private static ushort Random2()
		{
			//Prevents unwanted synchronization
			Span<byte> bytes = stackalloc byte[2];
			RandomNumberGenerator.Fill(bytes);
			return BitConverter.ToUInt16(bytes);
		}
		private readonly ConcurrentDictionary<string, string>[] optimisticCachePartitions = new ConcurrentDictionary<string, string>[256];
		private static async void Collect(WeakReference<ConcurrentDictionary<string, string>[]> weakReference, long softMemoryLimit){
		start:
			ushort random = Random2();
			await Task.Delay((random / 256) + 1);
			if(Misc.thisProcess.VirtualMemorySize64 < softMemoryLimit){
				goto start;
			}
			if(weakReference.TryGetTarget(out ConcurrentDictionary<string, string>[] optimisticCachePartitions)){
				optimisticCachePartitions[random % 256].Clear();
				goto start;
			}
		}
		public OptimisticExecutionManager(IDatabaseEngine databaseEngine, long softMemoryLimit)
		{
			this.databaseEngine = databaseEngine ?? throw new ArgumentNullException(nameof(databaseEngine));
			for(int i = 0; i < 256; ){
				optimisticCachePartitions[i++] = new ConcurrentDictionary<string, string>();
			}
			Collect(new WeakReference<ConcurrentDictionary<string, string>[]>(optimisticCachePartitions, false), softMemoryLimit);
		}
		private ConcurrentDictionary<string, string> GetOptimisticCachePartition(string key)
		{
			return optimisticCachePartitions[("Lesbians are optimistic " + key).GetHashCode() & 255];
		}
		private sealed class OptimisticExecutionScope : IOptimisticExecutionScope
		{
			private static readonly IReadOnlyDictionary<string, string> emptyStringDictionary = new Dictionary<string, string>();
			private static readonly string[] emptyStringArray = new string[0];
			private readonly ConcurrentDictionary<string, string>[] optimisticCachePartitions;
			private readonly IDatabaseEngine databaseEngine;
			public readonly ConcurrentDictionary<string, string> L1ReadCache = new ConcurrentDictionary<string, string>();
			public readonly ConcurrentDictionary<string, string> L1WriteCache = new ConcurrentDictionary<string, string>();
			private readonly bool readOnly;

			public OptimisticExecutionScope(ConcurrentDictionary<string, string>[] optimisticCachePartitions, IDatabaseEngine databaseEngine, bool readOnly)
			{
				this.optimisticCachePartitions = optimisticCachePartitions;
				this.databaseEngine = databaseEngine;
				this.readOnly = readOnly;
			}

			private ConcurrentDictionary<string, string> GetOptimisticCachePartition(string key)
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
					return readOnly ? ReadUnderlying2(key) : ReadUnderlying(key);
				}
			}
			private async Task<string> ReadUnderlying(string key){
				ConcurrentDictionary<string, string> optimisticCachePartition = GetOptimisticCachePartition(key);
				if (!optimisticCachePartition.TryGetValue(key, out string value))
				{
					value = optimisticCachePartition.GetOrAdd(key, await ReadUnderlying2(key));
				}
				value = L1ReadCache.GetOrAdd(key, value);

				if (L1WriteCache.TryGetValue(key, out string value2))
				{
					return value2;
				}
				else
				{
					return value;
				}
			}
			private async Task<string> ReadUnderlying2(string key){
				return (await databaseEngine.Execute(new string[] { key }, emptyStringDictionary, emptyStringDictionary))[key];
			}

			public void Write(string key, string value)
			{
				if (readOnly)
				{
					throw new InvalidOperationException("Writing is not allowed in read-only optimistic function");
				} else if (key.StartsWith("LesbianDB_reserved_"))
				{
					throw new InvalidOperationException("Writing to keys starting with \"LesbianDB_reserved_\" is discouraged");
				}
				else
				{
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
					GetOptimisticCachePartition(key)[key] = value;
				}
				success &= reads[key] == value;
			}
			if (success)
			{
				foreach (KeyValuePair<string, string> kvp in writes)
				{
					string key = kvp.Key;
					GetOptimisticCachePartition(key)[key] = kvp.Value;
				}
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
