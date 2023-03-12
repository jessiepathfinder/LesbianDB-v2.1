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
		/// <summary>
		/// Throws OptimisticFault if we made incorrect assumptions. Should be called once in a while in loops.
		/// </summary>
		/// <returns></returns>
		public Task Safepoint();
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
		private readonly int databaseEnginesCount;

		private readonly ConcurrentXHashMap<string>[] optimisticCachePartitions = new ConcurrentXHashMap<string>[256];
		private static async void Collect(WeakReference<ConcurrentXHashMap<string>[]> weakReference, long softMemoryLimit){
			AsyncManagedSemaphore asyncManagedSemaphore = new AsyncManagedSemaphore(0);
			Misc.RegisterGCListenerSemaphore(asyncManagedSemaphore);
		start:
			await asyncManagedSemaphore.Enter();
			if(weakReference.TryGetTarget(out ConcurrentXHashMap<string>[] optimisticCachePartitions)){
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
			int count = databaseEngines.Length;
			if (count == 0)
			{
				throw new ArgumentOutOfRangeException("At least 1 database engine required");
			}
			for (int i = 0; i < 256; ){
				optimisticCachePartitions[i++] = new ConcurrentXHashMap<string>();
			}
			Collect(new WeakReference<ConcurrentXHashMap<string>[]>(optimisticCachePartitions, false), softMemoryLimit);
			databaseEnginesCount = count;
		}
		public OptimisticExecutionManager(IDatabaseEngine databaseEngine, long softMemoryLimit) {
			databaseEngines = new IDatabaseEngine[]{databaseEngine ?? throw new ArgumentNullException(nameof(databaseEngine))};
			for (int i = 0; i < 256;)
			{
				optimisticCachePartitions[i++] = new ConcurrentXHashMap<string>();
			}
			Collect(new WeakReference<ConcurrentXHashMap<string>[]>(optimisticCachePartitions, false), softMemoryLimit);
		}

		public async Task<T> ExecuteOptimisticFunction<T>(Func<IOptimisticExecutionScope, Task<T>> optimisticFunction)
		{
			ConcurrentDictionary<string, string> readCache = new ConcurrentDictionary<string, string>();
			Dictionary<string, bool> consistentReads = new Dictionary<string, bool>();
			while(true){
				OptimisticExecutionScope optimisticExecutionScope = new OptimisticExecutionScope(databaseEngines, readCache, consistentReads, optimisticCachePartitions);
				T ret;
				Exception failure;
				try{
					ret = await optimisticFunction(optimisticExecutionScope);
					failure = null;
				} catch(OptimisticFault)
				{
					IReadOnlyDictionary<string, string> state = optimisticExecutionScope.barrierReads ?? await databaseEngines[Misc.FastRandom(0, databaseEnginesCount)].Execute(GetKeys(readCache.ToArray()), SafeEmptyReadOnlyDictionary<string, string>.instance, SafeEmptyReadOnlyDictionary<string, string>.instance);
					foreach(KeyValuePair<string, string> keyValuePair in state){
						string key = keyValuePair.Key;
						optimisticCachePartitions[key.GetHashCode() & 255][key] = keyValuePair.Value;
					}
					readCache = new ConcurrentDictionary<string, string>(state);
					continue;
				} catch (Exception e)
				{
					ret = default;
					failure = e;
				}
				
				KeyValuePair<string, string>[] keyValuePairs1 = readCache.ToArray();
				Dictionary<string, string> keyValuePairs3 = new Dictionary<string, string>();
				foreach (KeyValuePair<string, string> keyValuePair1 in keyValuePairs1)
				{
					string key = keyValuePair1.Key;
					if (optimisticExecutionScope.readFromCache.ContainsKey(key))
					{
						keyValuePairs3.Add(key, keyValuePair1.Value);
					}
				}
				IReadOnlyDictionary<string, string> writeCache;
				IReadOnlyDictionary<string, string> conditions;
				if (failure is null){
					KeyValuePair<string, string>[] keyValuePairs = optimisticExecutionScope.writecache.ToArray();
					if (keyValuePairs.Length == 0)
					{
						writeCache = SafeEmptyReadOnlyDictionary<string, string>.instance;
						conditions = SafeEmptyReadOnlyDictionary<string, string>.instance;
					}
					else
					{
						Dictionary<string, string> keyValuePairs2 = new Dictionary<string, string>();
						foreach(KeyValuePair<string, string> keyValuePair in keyValuePairs){
							string key = keyValuePair.Key;
							string value = keyValuePair.Value;
							if(keyValuePairs3.TryGetValue(key, out string read)){
								if(value == read){
									continue;
								}
							}
							keyValuePairs2.Add(key, value);
						}
						if(keyValuePairs2.Count == 0){
							writeCache = SafeEmptyReadOnlyDictionary<string, string>.instance;
							conditions = SafeEmptyReadOnlyDictionary<string, string>.instance;
						} else{
							conditions = keyValuePairs3;
							writeCache = keyValuePairs2;
						}
						
					}
				} else{
					writeCache = SafeEmptyReadOnlyDictionary<string, string>.instance;
					conditions = SafeEmptyReadOnlyDictionary<string, string>.instance;
				}
				bool restart = false;
				IReadOnlyDictionary<string, string> reads = await databaseEngines[Misc.FastRandom(0, databaseEnginesCount)].Execute(GetKeys2(keyValuePairs1, keyValuePairs3), conditions, writeCache);
				foreach(KeyValuePair<string, string> keyValuePair1 in keyValuePairs3)
				{
					if (reads[keyValuePair1.Key] == keyValuePair1.Value)
					{
						continue;
					}
					restart = true;
				}
				if(restart)
				{
					foreach (KeyValuePair<string, string> keyValuePair1 in keyValuePairs1)
					{
						string key = keyValuePair1.Key;
						optimisticCachePartitions[key.GetHashCode() & 255][key] = keyValuePair1.Value;
					}
					readCache = new ConcurrentDictionary<string, string>(reads);
				} else{
					if (failure is null)
					{
						ThreadPool.QueueUserWorkItem((object obj) => {
							foreach (KeyValuePair<string, string> keyValuePair1 in writeCache)
							{
								string key = keyValuePair1.Key;
								ConcurrentXHashMap<string> cachefragment = optimisticCachePartitions[key.GetHashCode() & 255];
								if (conditions.TryGetValue(key, out string val1))
								{
									string value = keyValuePair1.Value;
									Hash256 hash256 = new Hash256(key);
									try{
										cachefragment.AddOrUpdate(hash256, value, (Hash256 x, string old) => {
											if(old == val1){
												return value;
											}
											throw new DontWantToAddException();
										});
									} catch(DontWantToAddException){
										
									}
								}
								else
								{
									cachefragment[key] = keyValuePair1.Value;
								}

							}
						});
						return ret;
					}
					ExceptionDispatchInfo.Throw(failure);
				}
			}
		}
		private sealed class DontWantToAddException : Exception{
			
		}
		private static IEnumerable<string> GetKeys2(KeyValuePair<string, string>[] keyValuePairs, Dictionary<string, string> conditions)
		{
			foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
			{
				string key = keyValuePair.Key;
				if(conditions.ContainsKey(key)){
					yield return key;
					continue;
				}
				if(Misc.FastRandom(0, 20) > 0){
					yield return key;
				}
			}
		}
		private static IEnumerable<string> GetKeys(KeyValuePair<string, string>[] keyValuePairs)
		{
			foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
			{
				yield return keyValuePair.Key;
			}
		}
		private sealed class OptimisticExecutionScope : IOptimisticExecutionScope{
			private readonly IDatabaseEngine[] databaseEngines;
			private readonly int databaseCount;
			private readonly ConcurrentXHashMap<string>[] optimisticCachePartitions;
			public readonly ConcurrentDictionary<string, bool> readFromCache = new ConcurrentDictionary<string, bool>();
			public readonly ConcurrentDictionary<string, string> writecache = new ConcurrentDictionary<string, string>();
			private readonly ConcurrentDictionary<string, string> readcache;
			private readonly IReadOnlyDictionary<string, bool> consistentReads;
			public volatile IReadOnlyDictionary<string, string> barrierReads;

			public OptimisticExecutionScope(IDatabaseEngine[] databaseEngines, ConcurrentDictionary<string, string> readcache, IReadOnlyDictionary<string, bool> consistentReads, ConcurrentXHashMap<string>[] optimisticCachePartitions)
			{
				this.databaseEngines = databaseEngines;
				this.readcache = readcache;
				databaseCount = databaseEngines.Length;
				this.consistentReads = consistentReads;
				this.optimisticCachePartitions = optimisticCachePartitions;
			}

			public async Task Safepoint()
			{
				KeyValuePair<string, string>[] keyValuePairs = readcache.ToArray();
				IReadOnlyDictionary<string, string> reads = await databaseEngines[Misc.FastRandom(0, databaseCount)].Execute(GetKeys(keyValuePairs), SafeEmptyReadOnlyDictionary<string, string>.instance, SafeEmptyReadOnlyDictionary<string, string>.instance);
				foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs){
					if(reads[keyValuePair.Key] == keyValuePair.Value){
						break;
					}
					Interlocked.CompareExchange(ref barrierReads, reads, null);
					throw new OptimisticFault();
				}
			}

			public async Task<string> Read(string key)
			{
				if (key is null)
				{
					throw new ArgumentNullException(key);
				}
				if (writecache.TryGetValue(key, out string value1)){
					return value1;
				}
				if (readcache.TryGetValue(key, out value1))
				{
					if(writecache.TryGetValue(key, out string value2)){
						return value2;
					}
					readFromCache.TryAdd(key, false);
					return value1;
				}
				ConcurrentXHashMap<string> cacheBucket = optimisticCachePartitions[key.GetHashCode() & 255];
				Hash256 hash = new Hash256(key);
				if(cacheBucket.TryGetValue(hash, out value1)){
					value1 = readcache.GetOrAdd(key, value1);
					if (writecache.TryGetValue(key, out string value2))
					{
						return value2;
					}
					readFromCache.TryAdd(key, false);
					return value1;
				}
				string dbvalue = (await databaseEngines[Misc.FastRandom(0, databaseCount)].Execute(new string[] { key }, SafeEmptyReadOnlyDictionary<string, string>.instance, SafeEmptyReadOnlyDictionary<string, string>.instance))[key];
				cacheBucket[hash] = dbvalue;
				string value3 = readcache.GetOrAdd(key, dbvalue);
				if (writecache.TryGetValue(key, out string value4))
				{
					return value4;
				}
				readFromCache.TryAdd(key, false);
				return value3;
			}

			public async Task<IReadOnlyDictionary<string, string>> VolatileRead(IEnumerable<string> keys)
			{
				Dictionary<string, bool> dedup = new Dictionary<string, bool>();

				Dictionary<string, string> reads = new Dictionary<string, string>();
				bool checkingoptimizability = true;
				foreach(string key in keys){
					if(key is null){
						throw new NullReferenceException("Reading null keys is not supported");
					}
					readFromCache.TryAdd(key, false);
					if (dedup.TryAdd(key, false) & checkingoptimizability){
						if (consistentReads.ContainsKey(key))
						{
							if (writecache.TryGetValue(key, out string value))
							{
								reads.Add(key, value);
							}
							string value1 = readcache[key];
							if (writecache.TryGetValue(key, out value))
							{
								reads.Add(key, value);
							}
							else
							{
								reads.Add(key, value1);
							}
						}
						else
						{
							checkingoptimizability = false;
						}
					}
				}
				if(checkingoptimizability){
					return reads;
				}
				reads.Clear();
				bool success = true;
				foreach(KeyValuePair<string, string> keyValuePair in await databaseEngines[Misc.FastRandom(0, databaseCount)].Execute(dedup.Keys, SafeEmptyReadOnlyDictionary<string, string>.instance, SafeEmptyReadOnlyDictionary<string, string>.instance)){
					string key = keyValuePair.Key;
					string value = keyValuePair.Value;
					string existing = readcache.GetOrAdd(key, value);
					if(success){
						success = existing == value;
						if (writecache.TryGetValue(key, out string value1))
						{
							reads.Add(key, value1);
						}
						else
						{
							reads.Add(key, value);
						}
					}
					optimisticCachePartitions[key.GetHashCode() & 255][key] = value;
					
				}
				if(success){
					return reads;
				}
				throw new OptimisticFault();
			}

			public void Write(string key, string value)
			{
				if(key is null){
					throw new ArgumentNullException(key);
				}
				writecache[key] = value;
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

		public Task Safepoint()
		{
			return Misc.completed;
		}
	}
	/// <summary>
	/// A safepoint controller that aims to target 1% of our transaction execution time in safepoint
	/// </summary>
	public sealed class SafepointController{
		private long timeSpentInSafepoint;
		private long totalSafepoints;
		private long lastSafepointTime;
		private readonly long starttime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
		private readonly IOptimisticExecutionScope scope;
		private readonly AsyncReaderWriterLock asyncReaderWriterLock = new AsyncReaderWriterLock();

		public SafepointController(IOptimisticExecutionScope scope)
		{
			this.scope = scope ?? throw new ArgumentNullException(nameof(scope));
		}
		public async Task SafepointIfNeeded(){
			long time;
			long sincelastsafepoint;
			long totalSafepointsCache;
			await asyncReaderWriterLock.AcquireReaderLock();
			try{
				time = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
				sincelastsafepoint = time - lastSafepointTime;
				if (time - starttime < 500)
				{
					return;
				}
				totalSafepointsCache = totalSafepoints;
				if(totalSafepointsCache > 0){
					if((timeSpentInSafepoint * 100) / totalSafepointsCache > sincelastsafepoint)
					{
						return;
					}
				}
			} finally{
				asyncReaderWriterLock.ReleaseReaderLock();
			}
			await asyncReaderWriterLock.AcquireWriterLock();
			try{
				if (totalSafepoints == totalSafepointsCache)
				{
					totalSafepoints = totalSafepointsCache + 1;
					await scope.Safepoint();
					timeSpentInSafepoint += DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - time;
				}
			} finally{
				asyncReaderWriterLock.ReleaseWriterLock();
			}
		}
		public void EnsureScopeIs(IOptimisticExecutionScope optimisticExecutionScope){
			if(ReferenceEquals(scope, optimisticExecutionScope)){
				return;
			}
			while(optimisticExecutionScope is IChildTransaction childTransaction){
				optimisticExecutionScope = childTransaction.GetParent();
				if (ReferenceEquals(scope, optimisticExecutionScope))
				{
					return;
				}
			}
			throw new InvalidOperationException("This safepoint controller is created with a diffrent optimistic execution scope");
		}
	}
	public interface IChildTransaction : IOptimisticExecutionScope
	{
		public IOptimisticExecutionScope GetParent();
	}
	public sealed class NestedTransactionsManager : IOptimisticExecutionManager{
		private readonly AsyncMutex asyncMutex = new AsyncMutex();
		private readonly IOptimisticExecutionScope optimisticExecutionScope;

		public NestedTransactionsManager(IOptimisticExecutionScope optimisticExecutionScope)
		{
			this.optimisticExecutionScope = optimisticExecutionScope;
		}

		public async Task<T> ExecuteOptimisticFunction<T>(Func<IOptimisticExecutionScope, Task<T>> optimisticFunction)
		{
		start:
			NestedOptimisticExecutioner nestedOptimisticExecutioner = new NestedOptimisticExecutioner(optimisticExecutionScope);
			T result;
			try
			{
				result = await optimisticFunction(nestedOptimisticExecutioner);
			}
			catch (OptimisticFault)
			{
				goto start;
			}
			catch
			{
				KeyValuePair<string, string>[] keyValuePairs = nestedOptimisticExecutioner.reads.ToArray();
				await asyncMutex.Enter();
				try
				{
					foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
					{
						if ((await optimisticExecutionScope.Read(keyValuePair.Key)) == keyValuePair.Value){
							continue;
						}
						goto start;
					}
					
				}
				finally
				{
					asyncMutex.Exit();
				}
				throw;
			}
			KeyValuePair<string, string>[] keyValuePairs1 = nestedOptimisticExecutioner.reads.ToArray();
			await asyncMutex.Enter();
			try
			{
				foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs1)
				{
					if ((await optimisticExecutionScope.Read(keyValuePair.Key)) == keyValuePair.Value)
					{
						continue;
					}
					goto start;
				}
				foreach(KeyValuePair<string, string> keyValuePair1 in nestedOptimisticExecutioner.writes){
					optimisticExecutionScope.Write(keyValuePair1.Key, keyValuePair1.Value);
				}
			}
			finally
			{
				asyncMutex.Exit();
			}
			return result;
		}

		private sealed class NestedOptimisticExecutioner : IChildTransaction{
			private readonly IOptimisticExecutionScope optimisticExecutionScope;
			public readonly ConcurrentDictionary<string, string> writes = new ConcurrentDictionary<string, string>();
			public readonly ConcurrentDictionary<string, string> reads = new ConcurrentDictionary<string, string>();

			public NestedOptimisticExecutioner(IOptimisticExecutionScope optimisticExecutionScope)
			{
				this.optimisticExecutionScope = optimisticExecutionScope;
			}

			public async Task<string> Read(string key)
			{
				if(writes.TryGetValue(key, out string temp1)){
					return temp1;
				}
				if (reads.TryGetValue(key, out temp1))
				{
					if(writes.TryGetValue(key, out string temp2)){
						return temp2;
					}
					return temp1;
				}
				string temp3 = reads.GetOrAdd(key, await optimisticExecutionScope.Read(key));
				if (writes.TryGetValue(key, out string temp4))
				{
					return temp4;
				}
				return temp3;
			}

			public async Task<IReadOnlyDictionary<string, string>> VolatileRead(IEnumerable<string> keys)
			{
				IReadOnlyDictionary<string, string> result = await optimisticExecutionScope.VolatileRead(keys);
				Dictionary<string, string> output = new Dictionary<string, string>();
				foreach(KeyValuePair<string, string> keyValuePair in result)
				{
					string key = keyValuePair.Key;
					string value = keyValuePair.Value;
					string cached = reads.GetOrAdd(key, value);
					if (cached == value){
						if(writes.TryGetValue(key, out string temp1)){
							output.Add(key, temp1);
						} else{
							output.Add(key, cached);
						}
						continue;
					}
					throw new OptimisticFault();
				}

				return output;
			}

			public void Write(string key, string value)
			{
				writes[key] = value;
			}

			public Task Safepoint()
			{
				return Misc.completed;
			}

			public IOptimisticExecutionScope GetParent()
			{
				return optimisticExecutionScope;
			}
		}
	}
}
