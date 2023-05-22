using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace LesbianDB
{
	public sealed class DurableDictionaryCoordinator : IDatabaseEngine
	{
		private readonly IDurableDictionary durableDictionary;
		private readonly VeryASMRLockingManager veryASMRLockingManager = new VeryASMRLockingManager();
		private readonly ConcurrentBag<string> redoLogKeysPool = new ConcurrentBag<string>();
		private readonly AsyncManagedSemaphore asyncManagedSemaphore = new AsyncManagedSemaphore(4096);
		private static readonly string[] redoLogKeys = new string[4096];
		static DurableDictionaryCoordinator(){
			for (int i = 0; i < 4096; ++i)
			{
				redoLogKeys[i] = "LesbianDB_reserved_durable_redo_log_" + i.ToString();
			}
		}
		private DurableDictionaryCoordinator(IDurableDictionary durableDictionary)
		{
			this.durableDictionary = durableDictionary ?? throw new ArgumentNullException(nameof(durableDictionary));
		}
		public static async Task<DurableDictionaryCoordinator> Create(IDurableDictionary durableDictionary){
			DurableDictionaryCoordinator durableDictionaryCoordinator = new DurableDictionaryCoordinator(durableDictionary);
			await durableDictionaryCoordinator.Setup();
			return durableDictionaryCoordinator;
		}
		public static DurableDictionaryCoordinator Create(IDurableDictionary durableDictionary, out Task tsk)
		{
			DurableDictionaryCoordinator durableDictionaryCoordinator = new DurableDictionaryCoordinator(durableDictionary);
			tsk = durableDictionaryCoordinator.Setup();
			return durableDictionaryCoordinator;
		}
		private async Task RestoreAndErase(IReadOnlyDictionary<string, string> changes, string key){
			Task[] tasks = new Task[changes.Count];
			int i = -1;
			foreach(KeyValuePair<string, string> keyValuePair in changes){
				tasks[++i] = durableDictionary.Write(keyValuePair.Key, keyValuePair.Value);
			}
			await tasks;
			await durableDictionary.Write(key, null);
		}
		private readonly struct RestoreAndErase2{
			public readonly string key;
			public readonly Task<string> task;

			public RestoreAndErase2(Task<string> task, string key)
			{
				this.key = key;
				this.task = task;
			}
		}
		private async Task Setup(){
			Queue<RestoreAndErase2> readqueue = new Queue<RestoreAndErase2>();
			for(int i = 0; i < 4096; ++i){
				string key = redoLogKeys[i];
				readqueue.Enqueue(new RestoreAndErase2(durableDictionary.Read(key), key));
				redoLogKeysPool.Add(key);
			}

			Queue<Task> restoreAndEraseQueue = new Queue<Task>();
			while(readqueue.TryDequeue(out RestoreAndErase2 rae)){
				string res = await rae.task;
				if(res is null){
					continue;
				}
				restoreAndEraseQueue.Enqueue(RestoreAndErase(Misc.DeserializeObjectWithFastCreate<Dictionary<string, string>>(res), rae.key));
			}
			await restoreAndEraseQueue.ToArray();
		}
		private volatile Exception damage;
		private readonly LockOrderingComparer lockOrderingComparer = new LockOrderingComparer();
		private readonly struct ReadTask2{
			public readonly Task<string> task;
			public readonly string key;

			public ReadTask2(Task<string> task, string key)
			{
				this.task = task;
				this.key = key;
			}
		}
		private async Task<string> ReadIsolated(string key){
			await Task.Yield();
			return await durableDictionary.Read(key);
		}

		public async Task<IReadOnlyDictionary<string, string>> Execute(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes)
		{
			Exception tempexc2 = damage;
			if(tempexc2 is { }){
				throw new ObjectDamagedException(tempexc2);
			}
			Dictionary<string, string> keyValuePairs = new Dictionary<string, string>();

			Dictionary<string, bool> allReads = new Dictionary<string, bool>();
			Dictionary<string, bool> flatReads = new Dictionary<string, bool>();
			bool write = writes.Count > 0;
			if(write){
				foreach (string key in conditions.Keys)
				{
					if (key is null)
					{
						continue;
					}
					allReads.Add(key, false);
				}
			}
			foreach (string key in reads){
				if (key is null)
				{
					continue;
				}
				allReads.TryAdd(key, false);
				flatReads.TryAdd(key, false);
			}

			Dictionary<string, LockMode> lockLevels = new Dictionary<string, LockMode>();

			
			string jsonwrites;
			if (write)
			{
				foreach (string key in writes.Keys)
				{
					if(key is null | key.StartsWith("LesbianDB_reserved_durable_redo_log_")){
						return SafeEmptyReadOnlyDictionary<string, string>.instance;
					}
					lockLevels.Add(key, LockMode.Upgradeable);
				}
				jsonwrites = JsonConvert.SerializeObject(writes);
			}
			else{
				jsonwrites = null;
				conditions = SafeEmptyReadOnlyDictionary<string, string>.instance;
			}
			foreach (string key in allReads.Keys)
			{
				lockLevels.TryAdd(key, LockMode.Read);
			}
			List<string> lockList = new List<string>(lockLevels.Keys);
			lockList.Sort(lockOrderingComparer);
			bool unsafeLock = false;
			Queue<LockHandle> upgradeQueue = new Queue<LockHandle>();
			Queue<LockHandle> lockHandles = new Queue<LockHandle>();
			Queue<ReadTask2> readQueue = new Queue<ReadTask2>();
			bool chkdamagefinally = false;
			tempexc2 = damage;
			if (tempexc2 is { })
			{
				throw new ObjectDamagedException(tempexc2);
			}
			foreach (string locked in lockList){
				LockMode lockMode = lockLevels[locked];
				LockHandle lockHandle = await veryASMRLockingManager.Lock(locked, lockMode);
				lockHandles.Enqueue(lockHandle);
				if (lockMode == LockMode.Upgradeable){
					upgradeQueue.Enqueue(lockHandle);
				}
				if(allReads.ContainsKey(locked)){
					readQueue.Enqueue(new ReadTask2(ReadIsolated(locked), locked));
				}
			}
			try{
				tempexc2 = damage;
				if (tempexc2 is { })
				{
					chkdamagefinally = false;
					throw new ObjectDamagedException(tempexc2);
				}
				while (readQueue.TryDequeue(out ReadTask2 readTask2)){
					string key = readTask2.key;
					string value = await readTask2.task;

					if (flatReads.ContainsKey(key))
					{
						keyValuePairs.Add(key, value);
					}
					if (write){	
						if(conditions.TryGetValue(key, out string expect)){
							write = expect == value;
						}
					}
				}
				if(write){
					tempexc2 = damage;
					if (tempexc2 is { })
					{
						chkdamagefinally = false;
						throw new ObjectDamagedException(tempexc2);
					}
					unsafeLock = true;
					while(upgradeQueue.TryDequeue(out LockHandle lockHandle)){
						await lockHandle.UpgradeToWriteLock();
					}
					unsafeLock = false;
					tempexc2 = damage;
					if (tempexc2 is { })
					{
						chkdamagefinally = false;
						throw new ObjectDamagedException(tempexc2);
					}
					await asyncManagedSemaphore.Enter();
					string tmpkey;
					try{
						tempexc2 = damage;
						if (tempexc2 is { })
						{
							chkdamagefinally = false;
							throw new ObjectDamagedException(tempexc2);
						}
						if (!redoLogKeysPool.TryTake(out tmpkey))
						{
							throw new Exception("redo log keys pool exhausted (should not reach here!)");
						}
					} catch (Exception e){
						if(chkdamagefinally){
							chkdamagefinally = Interlocked.CompareExchange(ref damage, e, null) is { };
						}
						asyncManagedSemaphore.Exit();
						throw;
					}
					try
					{
						Task logtsk1 = durableDictionary.Write(tmpkey, jsonwrites);
						Task[] writeTasks = new Task[writes.Count];
						await logtsk1;
						int i = -1;
						foreach(KeyValuePair<string, string> keyValuePair in writes){
							writeTasks[++i] = durableDictionary.Write(keyValuePair.Key, keyValuePair.Value);
						}
						await writeTasks;
						await durableDictionary.Write(tmpkey, null);
					}
					catch (Exception e)
					{
						chkdamagefinally = Interlocked.CompareExchange(ref damage, e, null) is { };
						throw;
					}
					finally
					{
						tempexc2 = damage;
						if (tempexc2 is { })
						{
							chkdamagefinally = false;
							throw new ObjectDamagedException(tempexc2);
						}
						redoLogKeysPool.Add(tmpkey);
						asyncManagedSemaphore.Exit();
					}
				}
			} catch(Exception e){
				if(chkdamagefinally){
					chkdamagefinally = Interlocked.CompareExchange(ref damage, e, null) is { };
				}
				throw;
			}
			finally
			{
				if (unsafeLock)
				{
					throw new Exception("Exception while upgrading locks (should not reach here)");
				}
				while (lockHandles.TryDequeue(out LockHandle lockHandle))
				{
					lockHandle.Dispose();
				}
				if (chkdamagefinally)
				{
					tempexc2 = damage;
					if(tempexc2 is { }){
						throw new ObjectDamagedException(tempexc2);
					}
				}
			}

			return keyValuePairs;
		}
	}
}
