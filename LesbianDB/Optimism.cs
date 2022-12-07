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
		/// <summary>
		/// Serializable and cacheable optimistic locking read
		/// </summary>
		public Task<string> Read(string key);
		/// <summary>
		/// Uncached atomically snapshotting optimistic locking read (may throw spurrious OptimisticFaults)
		/// </summary>
		public Task<IReadOnlyDictionary<string, string>> VolatileRead(IEnumerable<string> keys);
		//NOTE: the write method is not asynchromous since writes are cached in-memory until commit
		public void Write(string key, string value);
	}
	/// <summary>
	/// An optimistic fault that causes the optimistic execution manager to revert and restart the transaction
	/// </summary>
	public sealed class OptimisticFault : Exception{
		
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
		private static readonly IReadOnlyDictionary<string, string> emptyDictionary = new Dictionary<string, string>();
		private sealed class OptimisticExecutionScope : IOptimisticExecutionScope
		{
			private readonly ConcurrentDictionary<string, string>[] optimisticCachePartitions;
			private readonly IDatabaseEngine databaseEngine;
			public readonly ConcurrentDictionary<string, string> L1ReadCache = new ConcurrentDictionary<string, string>();
			private static readonly string[] emptyStringArray = new string[0];
			public readonly ConcurrentDictionary<string, string> L1WriteCache = new ConcurrentDictionary<string, string>();

			public OptimisticExecutionScope(ConcurrentDictionary<string, string>[] optimisticCachePartitions, IDatabaseEngine databaseEngine)
			{
				this.optimisticCachePartitions = optimisticCachePartitions;
				this.databaseEngine = databaseEngine;
			}

			private ConcurrentDictionary<string, string> GetOptimisticCachePartition(string key)
			{
				return optimisticCachePartitions[("Lesbians are optimistic " + key).GetHashCode() & 255];
			}
			public async Task<IReadOnlyDictionary<string, string>> VolatileRead(IEnumerable<string> keys){
				IReadOnlyDictionary<string, string> read = await databaseEngine.Execute(keys, emptyDictionary, emptyDictionary);
				foreach(KeyValuePair<string, string> keyValuePair in read){
					string value = keyValuePair.Value;
					if(L1ReadCache.GetOrAdd(keyValuePair.Key, value) != value){
						throw new OptimisticFault();
					}
				}

				IDictionary<string, string> keyValuePairs = read as IDictionary<string, string> ?? new Dictionary<string, string>();
				if(ReferenceEquals(keyValuePairs, read)){
					foreach(string key in keys){
						if(L1WriteCache.TryGetValue(key, out string value)){
							keyValuePairs[key] = value;
						}
					}
				} else{
					foreach (string key in read.Keys)
					{
						if (L1WriteCache.TryGetValue(key, out string value))
						{
							keyValuePairs.Add(key, value);
						} else{
							keyValuePairs.Add(read[key], value);
						}
					}
				}
				return (IReadOnlyDictionary<string, string>)keyValuePairs;
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
					return ReadUnderlying(key);
				}
			}
			private async Task<string> ReadUnderlying(string key){
				ConcurrentDictionary<string, string> optimisticCachePartition = GetOptimisticCachePartition(key);
				if (!optimisticCachePartition.TryGetValue(key, out string value))
				{
					value = optimisticCachePartition.GetOrAdd(key, (await databaseEngine.Execute(new string[] { key }, emptyDictionary, emptyDictionary))[key]);
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

			public void Write(string key, string value)
			{
				if (key.StartsWith("LesbianDB_reserved_"))
				{
					throw new InvalidOperationException("Writing to keys starting with \"LesbianDB_reserved_\" is discouraged");
				}
				L1WriteCache[key] = value;
			}
		}
		private static IEnumerable<string> GetKeys(KeyValuePair<string, string>[] keyValuePairs){
			foreach(KeyValuePair<string, string> keyValuePair in keyValuePairs){
				yield return keyValuePair.Key;
			}
		}
		public async Task<T> ExecuteOptimisticFunction<T>(Func<IOptimisticExecutionScope, Task<T>> optimisticFunction){
			OptimisticExecutionScope optimisticExecutionScope = new OptimisticExecutionScope(optimisticCachePartitions, databaseEngine);
		start:
			T ret;
			Exception exception = null;
			try{
				ret = await optimisticFunction(optimisticExecutionScope);
			} catch(Exception e){
				if(e is OptimisticFault){
					foreach (KeyValuePair<string, string> keyValuePair in await databaseEngine.Execute(GetKeys(optimisticExecutionScope.L1ReadCache.ToArray()), emptyDictionary, emptyDictionary))
					{
						optimisticExecutionScope.L1ReadCache[keyValuePair.Key] = keyValuePair.Value;
					}
					optimisticExecutionScope.L1WriteCache.Clear();
					goto start;
				} else{
					ret = default;
					exception = e;
				}
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

			IReadOnlyDictionary<string, string> updated = await databaseEngine.Execute(reads.Keys, writes.Count == 0 ? emptyDictionary : reads, writes);
			bool success = true;
			foreach(KeyValuePair<string, string> kvp in updated){
				string key = kvp.Key;
				string value = kvp.Value;
				success &= reads[key] == value;
			}
			if (success)
			{
				foreach (KeyValuePair<string, string> kvp in writes)
				{
					string key = kvp.Key;
					GetOptimisticCachePartition(key)[key] = kvp.Value;
				}

				if(exception is null){
					return ret;
				} else{
					throw exception;
				}
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
