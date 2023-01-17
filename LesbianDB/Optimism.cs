using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
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
		/// Semi-cacheable atomically snapshotting optimistic locking read (may throw spurrious OptimisticFaults)
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
	public interface IOptimisticExecutionManager{
		public Task<T> ExecuteOptimisticFunction<T>(Func<IOptimisticExecutionScope, Task<T>> optimisticFunction);
	}
	public sealed class OptimisticExecutionManager : IOptimisticExecutionManager
	{
		private readonly IDatabaseEngine[] databaseEngines;

		private readonly ConcurrentDictionary<string, string>[] optimisticCachePartitions = new ConcurrentDictionary<string, string>[256];
		private static async void Collect(WeakReference<ConcurrentDictionary<string, string>[]> weakReference, long softMemoryLimit){
		start:
			await Task.Delay(Misc.FastRandom(1, 300));
			
			if(weakReference.TryGetTarget(out ConcurrentDictionary<string, string>[] optimisticCachePartitions)){
				if (Misc.thisProcess.VirtualMemorySize64 > softMemoryLimit)
				{
					optimisticCachePartitions[Misc.FastRandom(0, 256)].Clear();
				}
				goto start;
			}
		}
		public OptimisticExecutionManager(IDatabaseEngine[] databaseEngines, long softMemoryLimit)
		{
			this.databaseEngines = databaseEngines ?? throw new ArgumentNullException(nameof(databaseEngines));
			for (int i = 0; i < 256; ){
				optimisticCachePartitions[i++] = new ConcurrentDictionary<string, string>();
			}
			Collect(new WeakReference<ConcurrentDictionary<string, string>[]>(optimisticCachePartitions, false), softMemoryLimit);
		}
		public OptimisticExecutionManager(IDatabaseEngine databaseEngine, long softMemoryLimit) {
			databaseEngines = new IDatabaseEngine[]{databaseEngine ?? throw new ArgumentNullException(nameof(databaseEngine))};
			for (int i = 0; i < 256;)
			{
				optimisticCachePartitions[i++] = new ConcurrentDictionary<string, string>();
			}
			Collect(new WeakReference<ConcurrentDictionary<string, string>[]>(optimisticCachePartitions, false), softMemoryLimit);
		}
		private ConcurrentDictionary<string, string> GetOptimisticCachePartition(string key)
		{
			return optimisticCachePartitions[("Lesbians are optimistic " + key).GetHashCode() & 255];
		}
		private static readonly IReadOnlyDictionary<string, string> emptyDictionary = SafeEmptyReadOnlyDictionary<string, string>.instance;

		/// <summary>
		/// Help maintain the integrity of the optimistic execution scope while still performing optimistic optimizations
		/// based on what we observed from previous executions
		/// </summary>
		private sealed class ProxyOptimisticExecutionScope : IOptimisticExecutionScope{
			private IOptimisticExecutionScope underlying;
			public bool disposed;
			public volatile int busylock;
			public readonly AsyncReaderWriterLock asyncReaderWriterLock = new AsyncReaderWriterLock();

			public ProxyOptimisticExecutionScope(IOptimisticExecutionScope underlying)
			{
				this.underlying = underlying;
			}

			public async Task<string> Read(string key)
			{
				await asyncReaderWriterLock.AcquireReaderLock();
				try
				{
					if (disposed)
					{
						throw new ObjectDisposedException("IOptimisticExecutionScope");
					}
					return await underlying.Read(key);
				}
				finally
				{
					asyncReaderWriterLock.ReleaseReaderLock();
				}
			}

			public async Task<IReadOnlyDictionary<string, string>> VolatileRead(IEnumerable<string> keys)
			{
				await asyncReaderWriterLock.AcquireReaderLock();
				try
				{
					if (disposed)
					{
						throw new ObjectDisposedException("IOptimisticExecutionScope");
					}
					return await underlying.VolatileRead(keys);
				}
				finally
				{
					asyncReaderWriterLock.ReleaseReaderLock();
				}
			}

			public void Write(string key, string value)
			{
				while (Interlocked.Exchange(ref busylock, 1) == 1)
				{
					
				}
				try
				{
					if(disposed){
						throw new ObjectDisposedException("IOptimisticExecutionScope");
					}
					underlying.Write(key, value);
				} finally{
					busylock = 0;
				}
			}
		}
		private sealed class OptimisticExecutionScope : IOptimisticExecutionScope
		{
			private readonly ConcurrentDictionary<string, string>[] optimisticCachePartitions;
			private readonly IDatabaseEngine[] databaseEngines;
			public readonly ConcurrentDictionary<string, string> L1ReadCache = new ConcurrentDictionary<string, string>();
			public readonly ConcurrentDictionary<string, string> L1WriteCache = new ConcurrentDictionary<string, string>();
			public readonly ConcurrentDictionary<string, bool> cacheableVolatileReads = new ConcurrentDictionary<string, bool>();

			public OptimisticExecutionScope(ConcurrentDictionary<string, string>[] optimisticCachePartitions, IDatabaseEngine[] databaseEngines)
			{
				this.optimisticCachePartitions = optimisticCachePartitions;
				this.databaseEngines = databaseEngines;
			}

			private ConcurrentDictionary<string, string> GetOptimisticCachePartition(string key)
			{
				return optimisticCachePartitions[("Lesbians are optimistic " + key).GetHashCode() & 255];
			}
			public async Task<IReadOnlyDictionary<string, string>> VolatileRead(IEnumerable<string> keys){
				Dictionary<string, string> keyValuePairs1 = new Dictionary<string, string>();
				foreach(string key in keys){
					if(L1WriteCache.TryGetValue(key, out string val2)){
						keyValuePairs1.TryAdd(key, val2);
					} else if(cacheableVolatileReads.ContainsKey(key)){
						keyValuePairs1.TryAdd(key, L1ReadCache[key]);
					} else{
						goto pessimistic;
					}
				}
				return keyValuePairs1;
			pessimistic:
				IReadOnlyDictionary<string, string> read = await databaseEngines[Misc.FastRandom(0, databaseEngines.Length)].Execute(keys, emptyDictionary, emptyDictionary);
				foreach(KeyValuePair<string, string> keyValuePair in read){
					string value = keyValuePair.Value;
					if(L1ReadCache.GetOrAdd(keyValuePair.Key, value) != value){
						throw new OptimisticFault();
					}
				}

				IDictionary<string, string> keyValuePairs = read as IDictionary<string, string> ?? new Dictionary<string, string>();
				if(ReferenceEquals(keyValuePairs, read)){
					foreach(string key in keys){
						cacheableVolatileReads.TryAdd(key, false);
						if(L1WriteCache.TryGetValue(key, out string value)){
							keyValuePairs[key] = value;
						}
					}
				} else{
					foreach (string key in read.Keys)
					{
						cacheableVolatileReads.TryAdd(key, false);
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
				if(L1WriteCache.TryGetValue(key, out string value)){
					return Task.FromResult(value);
				} else if (L1ReadCache.TryGetValue(key, out value))
				{
					if (L1WriteCache.TryGetValue(key, out string value2))
					{
						return Task.FromResult(value2);
					}
					else
					{
						return Task.FromResult(value);
					}
				} else{
					return ReadUnderlying(key);
				}
			}
			private async Task<string> ReadUnderlying(string key){
				ConcurrentDictionary<string, string> optimisticCachePartition = GetOptimisticCachePartition(key);
				if (!optimisticCachePartition.TryGetValue(key, out string value))
				{
					value = optimisticCachePartition.GetOrAdd(key, (await databaseEngines[Misc.FastRandom(0, databaseEngines.Length)].Execute(new string[] { key }, emptyDictionary, emptyDictionary))[key]);
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
			OptimisticExecutionScope optimisticExecutionScope = new OptimisticExecutionScope(optimisticCachePartitions, databaseEngines);
		start:
			T ret;
			Exception exception = null;
			ProxyOptimisticExecutionScope proxyOptimisticExecutionScope = new ProxyOptimisticExecutionScope(optimisticExecutionScope);
			try
			{
				ret = await optimisticFunction(proxyOptimisticExecutionScope);
			} catch(Exception e){
				if(e is OptimisticFault){
					if(optimisticExecutionScope.L1ReadCache.Count > 0){
						foreach (KeyValuePair<string, string> keyValuePair in await databaseEngines[Misc.FastRandom(0, databaseEngines.Length)].Execute(GetKeys(optimisticExecutionScope.L1ReadCache.ToArray()), emptyDictionary, emptyDictionary))
						{
							string key = keyValuePair.Key;
							optimisticExecutionScope.cacheableVolatileReads.TryAdd(key, false);
							optimisticExecutionScope.L1ReadCache[key] = keyValuePair.Value;
						}
					}
					optimisticExecutionScope.L1WriteCache.Clear();
					goto start;
				} else{
					ret = default;
					exception = e;
					
				}
			} finally{
				AsyncReaderWriterLock asyncReaderWriterLock = proxyOptimisticExecutionScope.asyncReaderWriterLock;
				Task tsk = asyncReaderWriterLock.AcquireWriterLock();
				try{
					while(Interlocked.Exchange(ref proxyOptimisticExecutionScope.busylock, 1) == 1){
							
					}
					try{
						await tsk;
						proxyOptimisticExecutionScope.disposed = true;
					} finally{
						proxyOptimisticExecutionScope.busylock = 0;
					}
				} finally{
					await tsk;
					asyncReaderWriterLock.ReleaseWriterLock();
				}
			}
			//Conditions
			Dictionary<string, string> reads = new Dictionary<string, string>(optimisticExecutionScope.L1ReadCache.ToArray());

			//Effects
			Dictionary<string, string> writes;
			if(exception is null){
				writes = new Dictionary<string, string>();
				foreach (KeyValuePair<string, string> kvp in optimisticExecutionScope.L1WriteCache.ToArray())
				{
					string key = kvp.Key;
					string value = kvp.Value;
					if (reads.TryGetValue(key, out string old))
					{
						if (old == value)
						{
							continue;
						}
					}
					writes.Add(key, value);
				}
			} else{
				writes = new Dictionary<string, string>();
			}

			IReadOnlyDictionary<string, string> updated = await databaseEngines[Misc.FastRandom(0, databaseEngines.Length)].Execute(reads.Keys, writes.Count == 0 ? emptyDictionary : reads, writes);
			bool success = true;
			foreach(KeyValuePair<string, string> kvp in updated){
				string key = kvp.Key;
				string value = kvp.Value;
				success &= reads[key] == value;
			}
			if (success)
			{
				if(exception is null){
					foreach (KeyValuePair<string, string> kvp in writes)
					{
						string key = kvp.Key;
						GetOptimisticCachePartition(key)[key] = kvp.Value;
					}
					return ret;
				} else{
					ExceptionDispatchInfo.Throw(exception);
					throw exception;
				}
			}
			else
			{
				optimisticExecutionScope.L1WriteCache.Clear();
				foreach (KeyValuePair<string, string> kvp in updated)
				{
					string key = kvp.Key;
					optimisticExecutionScope.cacheableVolatileReads.TryAdd(key, false);
					optimisticExecutionScope.L1ReadCache[key] = kvp.Value;
				}
				goto start;
			}
		}
	}
	public interface ISnapshotReadScope : IOptimisticExecutionScope{
		
	}
	/// <summary>
	/// A wrapper designed to convert serializable optimistic locking reads into snapshotting optimitic locking reads
	/// </summary>
	public sealed class VolatileReadManager : ISnapshotReadScope
	{
		public static ISnapshotReadScope Create(IOptimisticExecutionScope underlying)
		{
			if (underlying is ISnapshotReadScope snapshotReadScope)
			{
				return snapshotReadScope;
			}
			return new VolatileReadManager(underlying);
		}
		private readonly IOptimisticExecutionScope underlying;
		private readonly ConcurrentDictionary<string, bool> log = new ConcurrentDictionary<string, bool>();

		private VolatileReadManager(IOptimisticExecutionScope underlying)
		{
			this.underlying = underlying ?? throw new ArgumentNullException(nameof(underlying));
		}
		private static IEnumerable<string> GetKeys(KeyValuePair<string, bool>[] keyValuePairs)
		{
			foreach (KeyValuePair<string, bool> keyValuePair in keyValuePairs)
			{
				yield return keyValuePair.Key;
			}
		}
		public async Task<string> Read(string key)
		{
			log.TryAdd(key, false);
			return (await underlying.VolatileRead(GetKeys(log.ToArray())))[key];
		}

		public async Task<IReadOnlyDictionary<string, string>> VolatileRead(IEnumerable<string> keys)
		{
			foreach (string key in keys)
			{
				log.TryAdd(key, false);
			}
			Task<IReadOnlyDictionary<string, string>> task = underlying.VolatileRead(GetKeys(log.ToArray()));
			Dictionary<string, string> keyValuePairs = new Dictionary<string, string>();
			IReadOnlyDictionary<string, string> keyValuePairs1 = await task;
			foreach (string key in keys)
			{
				keyValuePairs.TryAdd(key, keyValuePairs1[key]);
			}
			return keyValuePairs;
		}

		public void Write(string key, string value)
		{
			underlying.Write(key, value);
		}
	}
}
