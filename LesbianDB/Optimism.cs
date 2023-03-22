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
		private static volatile int counter;
		private readonly int hash = Interlocked.Increment(ref counter);
		public override int GetHashCode()
		{
			return hash;
		}
		public override bool Equals(object obj)
		{
			return ReferenceEquals(this, obj);
		}
	}

	//TODO: use pregnancy-friendly terms
	public sealed class TransactionAbortedException : Exception{
		public TransactionAbortedException() : base("The transaction has been aborted"){
			
		}
	}
	public interface IOptimisticExecutionManager{
		public Task<T> ExecuteOptimisticFunction<T>(Func<IOptimisticExecutionScope, Task<T>> optimisticFunction);
	}
	public sealed class OptimisticExecutionManager : IOptimisticExecutionManager
	{
		private readonly ConcurrentBag<WeakReference<OptimisticExecutionScope>> concurrentBag = new ConcurrentBag<WeakReference<OptimisticExecutionScope>>();
		private static async void DeferredConflictMonitoringRegistration(WeakReference<ConcurrentBag<WeakReference<OptimisticExecutionScope>>> weakReference1, WeakReference<OptimisticExecutionScope> weakReference2, CancellationToken cancellationToken)
		{
			try{
				await Task.Delay(500, cancellationToken);
			} catch(TaskCanceledException){
				return;
			}
			if(weakReference1.TryGetTarget(out ConcurrentBag<WeakReference<OptimisticExecutionScope>> concurrentBag)){
				if(weakReference2.TryGetTarget(out OptimisticExecutionScope optimisticExecutionScope)){
					if(optimisticExecutionScope.abort == 0){
						concurrentBag.Add(weakReference2);
					}
				}
			}
		}
		private static async void ConflictMonitor(WeakReference<ConcurrentBag<WeakReference<OptimisticExecutionScope>>> weakReference){
			Queue<WeakReference<OptimisticExecutionScope>> queue = new Queue<WeakReference<OptimisticExecutionScope>>();
			while (true){
				await Task.Delay(Misc.FastRandom(1, 500));
				if (weakReference.TryGetTarget(out ConcurrentBag<WeakReference<OptimisticExecutionScope>> concurrentBag))
				{
					try
					{
						while (concurrentBag.TryTake(out WeakReference<OptimisticExecutionScope> wr)) {
							if (wr.TryGetTarget(out _))
							{
								queue.Enqueue(wr);
							}
						}
					} catch (ObjectDisposedException) {
						return;
					}
					
				} else{
					return;
				}
				if(queue.Count == 0){
					continue;
				}
				Queue<WeakReference<OptimisticExecutionScope>> queue1 = new Queue<WeakReference<OptimisticExecutionScope>>();
				Dictionary<string, Dictionary<OptimisticExecutionScope, bool>> writekeys = new Dictionary<string, Dictionary<OptimisticExecutionScope, bool>>();
				Dictionary<string, Dictionary<OptimisticExecutionScope, bool>> readkeys = new Dictionary<string, Dictionary<OptimisticExecutionScope, bool>>();
				while (queue.TryDequeue(out WeakReference<OptimisticExecutionScope> wr)){
					if(wr.TryGetTarget(out OptimisticExecutionScope optimisticExecutionScope)){
						if(optimisticExecutionScope.abort == 1 || optimisticExecutionScope.opportunisticRevertNotified == 1){
							continue;
						}
						queue1.Enqueue(wr);
						foreach (KeyValuePair<string, bool> keyValuePair in optimisticExecutionScope.readFromCache.ToArray())
						{
							string key = keyValuePair.Key;
							if (!readkeys.TryGetValue(key, out Dictionary<OptimisticExecutionScope, bool> dictionary))
							{
								dictionary = new Dictionary<OptimisticExecutionScope, bool>();
								readkeys.Add(key, dictionary);
							}
							dictionary.Add(optimisticExecutionScope, false);
						}
						foreach (KeyValuePair<string, string> keyValuePair in optimisticExecutionScope.writecache.ToArray())
						{
							string key = keyValuePair.Key;
							if (optimisticExecutionScope.readcache.TryGetValue(key, out string value))
							{
								if (value == keyValuePair.Value)
								{
									continue;
								}
							}
							if (!writekeys.TryGetValue(key, out Dictionary<OptimisticExecutionScope, bool> dictionary))
							{
								dictionary = new Dictionary<OptimisticExecutionScope, bool>();
								writekeys.Add(key, dictionary);
							}
							dictionary.Add(optimisticExecutionScope, false);
						}
					}
				}
				queue = queue1;
				
				foreach(KeyValuePair<string, Dictionary<OptimisticExecutionScope, bool>> keyValuePair1 in writekeys){
					string key1 = keyValuePair1.Key;
					if (readkeys.TryGetValue(key1, out Dictionary<OptimisticExecutionScope, bool> value2))
					{
						foreach(OptimisticExecutionScope writer in keyValuePair1.Value.Keys){
							if(writer.abort == 1 || writer.opportunisticRevertNotified == 1){
								break;
							}
							foreach(OptimisticExecutionScope reader in value2.Keys){
								if(ReferenceEquals(reader, writer)){
									continue;
								}
								if (reader.abort == 1 || reader.opportunisticRevertNotified == 1)
								{
									continue;
								}
								writer.opportunisticRevertNotificationReceivers.TryAdd(reader, false);
							}
						}
					}
				}

			}
		}
		
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
					Misc.AttemptSecondGC();
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
			ConflictMonitor(new WeakReference<ConcurrentBag<WeakReference<OptimisticExecutionScope>>>(concurrentBag, false));
			databaseEnginesCount = count;
		}
		public OptimisticExecutionManager(IDatabaseEngine databaseEngine, long softMemoryLimit) : this(new IDatabaseEngine[] { databaseEngine}, softMemoryLimit){
			
		}
		private readonly VeryASMRLockingManager veryASMRLockingManager = new VeryASMRLockingManager();

		public async Task<T> ExecuteOptimisticFunction<T>(Func<IOptimisticExecutionScope, Task<T>> optimisticFunction)
		{
			ConcurrentDictionary<string, string> readCache = new ConcurrentDictionary<string, string>();
			IReadOnlyDictionary<string, string> reads;
			Dictionary<string, LockMode> locks = new Dictionary<string, LockMode>();
			List<string> sortedLockList = new List<string>();
			IEnumerable<string> contendedKeys1 = null;
			while (true){
				OptimisticExecutionScope optimisticExecutionScope = new OptimisticExecutionScope(databaseEngines, readCache, optimisticCachePartitions);
				T ret;
				Exception failure;
				IReadOnlyDictionary<string, string> writeCache;
				IReadOnlyDictionary<string, string> conditions;
				KeyValuePair<string, string>[] keyValuePairs1;
				Dictionary<string, string> keyValuePairs3;
				bool upgrading = false;
				bool upgraded = false;
				Queue<LockHandle> unlockQueue = new Queue<LockHandle>();
				Queue<LockHandle> upgradeQueue = new Queue<LockHandle>();
				int locksCount = locks.Count;
				if (locksCount > sortedLockList.Count){
					sortedLockList.Clear();
					if(locksCount > sortedLockList.Capacity){
						sortedLockList.Capacity = locksCount * 2;
					}
					sortedLockList.AddRange(locks.Keys);
					sortedLockList.Sort(LockOrderingComparer.instance);
				}
				foreach (string lockstr in sortedLockList)
				{
					LockMode lockMode = locks[lockstr];
					LockHandle lockHandle = await veryASMRLockingManager.Lock(lockstr, lockMode);
					if (lockMode == LockMode.Upgradeable)
					{
						upgradeQueue.Enqueue(lockHandle);
					}
					unlockQueue.Enqueue(lockHandle);
				}
				try
				{
					if(contendedKeys1 is { })
					{
						foreach (KeyValuePair<string, string> keyValuePair in await databaseEngines[Misc.FastRandom(0, databaseEnginesCount)].Execute(contendedKeys1, SafeEmptyReadOnlyDictionary<string, string>.instance, SafeEmptyReadOnlyDictionary<string, string>.instance)){
							string key = keyValuePair.Key;
							string value = keyValuePair.Value;
							optimisticCachePartitions[key.GetHashCode() & 255][key] = value;
							readCache.TryAdd(key, value);
						}
					}
					CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
					DeferredConflictMonitoringRegistration(new WeakReference<ConcurrentBag<WeakReference<OptimisticExecutionScope>>>(concurrentBag, false), new WeakReference<OptimisticExecutionScope>(optimisticExecutionScope, false), cancellationTokenSource.Token);
					try
					{
						
						try
						{
							ret = await optimisticFunction(optimisticExecutionScope);
							if (optimisticExecutionScope.opportunisticRevertNotified == 1)
							{
								//REVERT because we received an opportunistic revert notification
								throw new OptimisticFault();
							}
						}
						catch(TransactionAbortedException){
							throw new OptimisticFault();
						}
						finally
						{
							cancellationTokenSource.Cancel();
							optimisticExecutionScope.abort = 1;
							cancellationTokenSource.Dispose();
						}
						failure = null;
					}
					catch (OptimisticFault)
					{
						KeyValuePair<string, string>[] keyValuePairs = readCache.ToArray();
						IReadOnlyDictionary<string, string> state = optimisticExecutionScope.barrierReads ?? await databaseEngines[Misc.FastRandom(0, databaseEnginesCount)].Execute(GetInvolvedKeys(keyValuePairs, optimisticExecutionScope.readFromCache), SafeEmptyReadOnlyDictionary<string, string>.instance, SafeEmptyReadOnlyDictionary<string, string>.instance);
						Queue<KeyValuePair<string, string>> newstate = new Queue<KeyValuePair<string, string>>();
						Queue<string> contendedKeys2 = new Queue<string>();
						foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
						{
							string key = keyValuePair.Key;
							string value = keyValuePair.Value;
							if(optimisticExecutionScope.readFromCache.ContainsKey(key)){
								if (state.TryGetValue(key, out string value1))
								{
									if (value1 != value)
									{
										if (optimisticExecutionScope.writecache.ContainsKey(key))
										{
											locks[key] = LockMode.Upgradeable;
										}
										else
										{
											locks.TryAdd(key, LockMode.Read);
										}
										contendedKeys2.Enqueue(key);
										continue;
									}
								}
							}
							newstate.Enqueue(keyValuePair);
						}
						readCache = new ConcurrentDictionary<string, string>(newstate.ToArray());
						contendedKeys1 = contendedKeys2.ToArray();
						continue;
					}
					catch (Exception e)
					{
						ret = default;
						failure = e;
					}

					keyValuePairs1 = readCache.ToArray();
					keyValuePairs3 = new Dictionary<string, string>();
					foreach (KeyValuePair<string, string> keyValuePair1 in keyValuePairs1)
					{
						string key = keyValuePair1.Key;
						if (optimisticExecutionScope.readFromCache.ContainsKey(key))
						{
							keyValuePairs3.Add(key, keyValuePair1.Value);
						}
					}
					
					if (failure is null)
					{
						KeyValuePair<string, string>[] keyValuePairs = optimisticExecutionScope.writecache.ToArray();
						if (keyValuePairs.Length == 0)
						{
							writeCache = SafeEmptyReadOnlyDictionary<string, string>.instance;
							conditions = SafeEmptyReadOnlyDictionary<string, string>.instance;
						}
						else
						{
							Dictionary<string, string> keyValuePairs2 = new Dictionary<string, string>();
							foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
							{
								string key = keyValuePair.Key;
								string value = keyValuePair.Value;
								if (keyValuePairs3.TryGetValue(key, out string read))
								{
									if (value == read)
									{
										continue;
									}
								}
								keyValuePairs2.Add(key, value);
							}
							if (keyValuePairs2.Count == 0)
							{
								writeCache = SafeEmptyReadOnlyDictionary<string, string>.instance;
								conditions = SafeEmptyReadOnlyDictionary<string, string>.instance;
							}
							else
							{
								conditions = keyValuePairs3;
								writeCache = keyValuePairs2;
							}

						}
					}
					else
					{
						writeCache = SafeEmptyReadOnlyDictionary<string, string>.instance;
						conditions = SafeEmptyReadOnlyDictionary<string, string>.instance;
					}
					
					
					upgrading = true;
					while(upgradeQueue.TryDequeue(out LockHandle lockHandle)){
						await lockHandle.UpgradeToWriteLock();
					}
					upgraded = true;
					reads = await databaseEngines[Misc.FastRandom(0, databaseEnginesCount)].Execute(GetKeys2(keyValuePairs1, keyValuePairs3), conditions, writeCache);
				} finally{
					if (upgrading ^ upgraded)
					{
						throw new Exception("Inconsistent lock upgrading (should not reach here)");
					}
					while(unlockQueue.TryDequeue(out LockHandle lockHandle)){
						lockHandle.Dispose();
					}
				}
				bool restart = false;
				Dictionary<string, bool> contendedKeys = new Dictionary<string, bool>();
				foreach (KeyValuePair<string, string> keyValuePair1 in keyValuePairs3)
				{
					string key = keyValuePair1.Key;
					if (reads[key] == keyValuePair1.Value)
					{
						continue;
					}
					contendedKeys.Add(key, false);
					if (writeCache.ContainsKey(key)) {
						locks[key] = LockMode.Upgradeable;
					} else{
						locks.TryAdd(key, LockMode.Read);
					}
					restart = true;
				}
				if(restart)
				{
					ThreadPool.QueueUserWorkItem((object obj) =>
					{
						foreach (KeyValuePair<string, string> keyValuePair1 in reads)
						{
							string key = keyValuePair1.Key;
							if (contendedKeys.ContainsKey(key))
							{
								continue;
							}
							optimisticCachePartitions[key.GetHashCode() & 255][key] = keyValuePair1.Value;
						}
					});
					readCache = new ConcurrentDictionary<string, string>(GetUncontendedKeys(reads, contendedKeys));
					contendedKeys1 = contendedKeys.Keys;
				} else{
					if (failure is null)
					{
						ThreadPool.QueueUserWorkItem((ConcurrentDictionary<OptimisticExecutionScope, bool> notificationReceivers) => {
							foreach(KeyValuePair<OptimisticExecutionScope, bool> keyValuePair in notificationReceivers.ToArray()){
								keyValuePair.Key.opportunisticRevertNotified = 1;
							}
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
						}, optimisticExecutionScope.opportunisticRevertNotificationReceivers, true);
						return ret;
					}
					ExceptionDispatchInfo.Throw(failure);
				}
			}
		}
		private static IEnumerable<KeyValuePair<string, string>> GetUncontendedKeys(IReadOnlyDictionary<string, string> allKeys, Dictionary<string, bool> contendedKeys){
			foreach(KeyValuePair<string, string> keyValuePair in allKeys){
				if(contendedKeys.ContainsKey(keyValuePair.Key)){
					continue;
				}
				yield return keyValuePair;
			}
		}
		private static IEnumerable<string> GetInvolvedKeys(KeyValuePair<string, string>[] keyValuePairs, ConcurrentDictionary<string, bool> touchedKeys)
		{
			foreach(KeyValuePair<string, string> keyValuePair in keyValuePairs){
				string key = keyValuePair.Key;
				if (touchedKeys.ContainsKey(key)){
					yield return key;
					continue;
				}
				if (Misc.FastRandom(0, 20) > 0)
				{
					yield return key;
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
			private static volatile int counter;
			private readonly int hash = Interlocked.Increment(ref counter);
			public override int GetHashCode()
			{
				return hash;
			}
			public override bool Equals(object obj)
			{
				return ReferenceEquals(this, obj);
			}
			public volatile int opportunisticRevertNotified;
			public readonly ConcurrentDictionary<OptimisticExecutionScope, bool> opportunisticRevertNotificationReceivers = new ConcurrentDictionary<OptimisticExecutionScope, bool>();
			public volatile int abort;
			private readonly IDatabaseEngine[] databaseEngines;
			private readonly int databaseCount;
			private readonly ConcurrentXHashMap<string>[] optimisticCachePartitions;
			public readonly ConcurrentDictionary<string, bool> readFromCache = new ConcurrentDictionary<string, bool>();
			public readonly ConcurrentDictionary<string, string> writecache = new ConcurrentDictionary<string, string>();
			public readonly ConcurrentDictionary<string, string> readcache;
			public volatile IReadOnlyDictionary<string, string> barrierReads;

			public OptimisticExecutionScope(IDatabaseEngine[] databaseEngines, ConcurrentDictionary<string, string> readcache, ConcurrentXHashMap<string>[] optimisticCachePartitions)
			{
				this.databaseEngines = databaseEngines;
				this.readcache = readcache;
				databaseCount = databaseEngines.Length;
				this.optimisticCachePartitions = optimisticCachePartitions;
			}

			public async Task Safepoint()
			{
				if(abort == 1){
					throw new TransactionAbortedException();
				}
				if (opportunisticRevertNotified == 1)
				{
					abort = 1;
					throw new OptimisticFault();
				}
				KeyValuePair<string, string>[] keyValuePairs = readcache.ToArray();
				IReadOnlyDictionary<string, string> reads = await databaseEngines[Misc.FastRandom(0, databaseCount)].Execute(GetKeys(keyValuePairs), SafeEmptyReadOnlyDictionary<string, string>.instance, SafeEmptyReadOnlyDictionary<string, string>.instance);
				foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs){
					if(reads[keyValuePair.Key] == keyValuePair.Value){
						break;
					}
					Interlocked.CompareExchange(ref barrierReads, reads, null);
					abort = 1;
					throw new OptimisticFault();
				}
			}

			public async Task<string> Read(string key)
			{
				if (abort == 1)
				{
					throw new TransactionAbortedException();
				}
				if (opportunisticRevertNotified == 1)
				{
					abort = 1;
					throw new OptimisticFault();
				}
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
				if (abort == 1)
				{
					throw new TransactionAbortedException();
				}
				if (opportunisticRevertNotified == 1)
				{
					abort = 1;
					throw new OptimisticFault();
				}
				Dictionary<string, bool> dedup = new Dictionary<string, bool>();

				Dictionary<string, string> reads = new Dictionary<string, string>();
				foreach(string key in keys){
					if(key is null){
						throw new NullReferenceException("Reading null keys is not supported");
					}
					readFromCache.TryAdd(key, false);
					dedup.TryAdd(key, false);
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
				abort = 1;
				throw new OptimisticFault();
			}

			public void Write(string key, string value)
			{
				if (abort == 1)
				{
					throw new TransactionAbortedException();
				}
				if (opportunisticRevertNotified == 1)
				{
					abort = 1;
					throw new OptimisticFault();
				}
				if (key is null){
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
		private volatile int locker;

		public SafepointController(IOptimisticExecutionScope scope)
		{
			this.scope = scope ?? throw new ArgumentNullException(nameof(scope));
		}
		public async Task SafepointIfNeeded(){
			long time = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

			//no safepoints in the first 500 milliseconds
			long runtime = time - starttime;
			if (runtime < 500){
				return;
			}

			if(Interlocked.Exchange(ref locker, 1) == 1){
				return;
			}
			try{
				long timeSinceLastSafepoint = time - lastSafepointTime;
				if(totalSafepoints == 0 || runtime > (100 * timeSpentInSafepoint) / totalSafepoints)
				{
					await scope.Safepoint();
					lastSafepointTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
					timeSpentInSafepoint += lastSafepointTime - time;
					++totalSafepoints;
					return;
				}

			} finally{
				locker = 0;
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
		private readonly AsyncReaderWriterLock asyncReaderWriterLock = new AsyncReaderWriterLock(true);
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
			await asyncReaderWriterLock.AcquireReaderLock();
			try
			{
				result = await optimisticFunction(nestedOptimisticExecutioner);
			}
			catch (OptimisticFault optimisticFault)
			{
				if(nestedOptimisticExecutioner.knownOptimisticFaults.ContainsKey(optimisticFault)){
					goto start;
				}
				throw;
			} finally{
				asyncReaderWriterLock.ReleaseReaderLock();
			}
			KeyValuePair<string, string>[] keyValuePairs1 = nestedOptimisticExecutioner.reads.ToArray();
			KeyValuePair<string, string>[] keyValuePairs2 = nestedOptimisticExecutioner.writes.ToArray();
			bool upgradeable = keyValuePairs2.Length > 0;
			bool upgraded = false;
			bool upgrading = false;
			if(upgradeable){
				await asyncReaderWriterLock.AcquireUpgradeableReadLock();
			} else{
				await asyncReaderWriterLock.AcquireReaderLock();
			}
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
				if(upgradeable){
					upgrading = true;
					await asyncReaderWriterLock.UpgradeToWriteLock();
					upgraded = true;
					foreach (KeyValuePair<string, string> keyValuePair1 in keyValuePairs2)
					{
						optimisticExecutionScope.Write(keyValuePair1.Key, keyValuePair1.Value);
					}
				}
			}
			finally
			{
				if(upgradeable){
					if (upgraded)
					{
						asyncReaderWriterLock.FullReleaseUpgradedLock();
					}
					else
					{
						if(upgrading){
							throw new Exception("Unable to determine lock status (should not reach here)");
						}
						asyncReaderWriterLock.ReleaseUpgradeableReadLock();
					}
				} else{
					asyncReaderWriterLock.ReleaseReaderLock();
				}
			}
			return result;
		}

		private sealed class NestedOptimisticExecutioner : IChildTransaction{
			private readonly IOptimisticExecutionScope optimisticExecutionScope;
			public readonly ConcurrentDictionary<string, string> writes = new ConcurrentDictionary<string, string>();
			public readonly ConcurrentDictionary<string, string> reads = new ConcurrentDictionary<string, string>();
			public readonly ConcurrentDictionary<OptimisticFault, bool> knownOptimisticFaults = new ConcurrentDictionary<OptimisticFault, bool>();

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
					OptimisticFault exception = new OptimisticFault();
					knownOptimisticFaults.TryAdd(exception, false);
					throw exception;
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
