/*
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LesbianDB.Optimism.Core;
using Newtonsoft.Json;

namespace LesbianDB
{
	public readonly struct TransientStorageShard{
		public readonly IDatabaseEngine databaseEngine;
		public readonly ushort from;
		public readonly ushort to;

		public TransientStorageShard(IDatabaseEngine databaseEngine, ushort from, ushort to)
		{
			this.databaseEngine = databaseEngine ?? throw new ArgumentNullException(nameof(databaseEngine));
			if(from > to){
				this.to = from;
				this.from = to;
				return;
			}
			this.from = from;
			this.to = to;
		}
	}
	/// <summary>
	/// Advanced-technology sharded database engine
	/// </summary>
	public sealed class KellyanneDatabaseEngine : IDatabaseEngine
	{
		private readonly struct SyncShardTask{
			public readonly Task<string> task;
			public readonly ushort shard;

			public SyncShardTask(Task<string> task, ushort shard)
			{
				this.task = task;
				this.shard = shard;
			}
		}
		private readonly struct ReadShardTask
		{
			public readonly Task<IReadOnlyDictionary<string, string>> task;
			public readonly ushort shard;

			public ReadShardTask(Task<IReadOnlyDictionary<string, string>> task, ushort shard)
			{
				this.task = task;
				this.shard = shard;
			}
		}
		private readonly struct RedoLogShardCandidate{
			public readonly string name;
			public readonly IDatabaseEngine databaseEngine;

			public RedoLogShardCandidate(string name, IDatabaseEngine databaseEngine)
			{
				this.name = name ?? throw new ArgumentNullException(nameof(name));
				this.databaseEngine = databaseEngine ?? throw new ArgumentNullException(nameof(databaseEngine));
			}
		}
		private readonly IDatabaseEngine coordinatorState;
		private readonly IDatabaseEngine[] shards = new IDatabaseEngine[65536];
		private readonly IReadOnlyDictionary<string, IDatabaseEngine> redoLogShards;
		private readonly RedoLogShardCandidate[] redoLogShardCandidates;
		private static readonly IReadOnlyDictionary<string, string> emptyDictionary = new Dictionary<string, string>();
		private static readonly string[] emptyStringArray = new string[0];
		private readonly int redoLogShardsCount;

		public KellyanneDatabaseEngine(IDatabaseEngine coordinatorState, IEnumerable<TransientStorageShard> transientStorageShards, IReadOnlyDictionary<string, IDatabaseEngine> redoLogShards) {
			this.redoLogShards = redoLogShards ?? throw new ArgumentNullException(nameof(redoLogShards));
			this.coordinatorState = coordinatorState ?? throw new ArgumentNullException(nameof(coordinatorState));
			Queue<RedoLogShardCandidate> queue = new Queue<RedoLogShardCandidate>();
			foreach(KeyValuePair<string, IDatabaseEngine> keyValuePair in redoLogShards){
				queue.Enqueue(new RedoLogShardCandidate(keyValuePair.Key, keyValuePair.Value));
			}
			redoLogShardCandidates = queue.ToArray();
			redoLogShardsCount = redoLogShardCandidates.Length;
			if (redoLogShardsCount == 0)
			{
				throw new InvalidOperationException("No redo log shards attached");
			}
			for (int i = 0; i < 65536; ++i) {
				bool found = false;
				foreach (TransientStorageShard transientStorageShard in transientStorageShards) {
					if (i > transientStorageShard.to || i < transientStorageShard.from) {
						continue;
					}
					if (found) {
						throw new InvalidOperationException(new StringBuilder("Virtual shard ").Append(i).Append(" is overlapped").ToString());
					}
					found = true;
					shards[i] = transientStorageShard.databaseEngine;
				}
				if (found) {
					continue;
				}
				throw new InvalidOperationException(new StringBuilder("Virtual shard ").Append(i).Append(" is not covered").ToString());
			}
		}
		
		public async Task<IReadOnlyDictionary<string, string>> Execute(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes)
		{
			await File.AppendAllTextAsync("c:\\users\\jessi\\desktop\\ldlog", "enter\n");
			bool writing = writes.Count > 0;
			Dictionary<string, bool> returningReads = new Dictionary<string, bool>();
			foreach(string read in reads){
				returningReads.TryAdd(read, false);
			}
			IEnumerable<string> readsEnumerable = Misc.JoinEnumerables(reads, conditions.Keys);
			Dictionary<ushort, Queue<string>> readVirtualShards = new Dictionary<ushort, Queue<string>>();
			foreach (string read in readsEnumerable) {
				ushort hash = (ushort)(Misc.HashString3(read) & 65535);
				if (!readVirtualShards.TryGetValue(hash, out Queue<string> queue)) {
					queue = new Queue<string>();
					queue.Enqueue("Kellyanne_storage_checkpoint_" + hash.ToString());
					readVirtualShards.Add(hash, queue);
				}
				queue.Enqueue(read);
			}
			Dictionary<ushort, Dictionary<string, string>> shardTransactions;
			RedoLogTransaction redoLogTransaction;
			RedoLogCheckpoint redoLogCheckpoint;
			IEnumerable<string> consistentVirtualShards;
			string selectedRedoLogShardName;
			IDatabaseEngine selectedRedoLogShardEngine;
			if (writing){
				shardTransactions = new Dictionary<ushort, Dictionary<string, string>>();
				foreach(KeyValuePair<string, string> write in writes){
					string key = write.Key;
					ushort hash = (ushort)(Misc.HashString3(key) & 65535);
					if(!shardTransactions.TryGetValue(hash, out Dictionary<string, string> transaction)){
						transaction = new Dictionary<string, string>();
						shardTransactions.Add(hash, transaction);
					}
					transaction.Add(key, write.Value);
				}
				Dictionary<string, bool> keyValuePairs = new Dictionary<string, bool>();
				foreach (string involved in Misc.JoinEnumerables(readsEnumerable, writes.Keys))
				{
					keyValuePairs.TryAdd("Kellyanne_coordinator_checkpoint_" + (Misc.HashString3(involved) & 65535).ToString(), false);
				}
				consistentVirtualShards = keyValuePairs.Keys;
				redoLogTransaction = new RedoLogTransaction();
				redoLogCheckpoint = new RedoLogCheckpoint();
				RedoLogShardCandidate redoLogShardCandidate;
				if (redoLogShardsCount == 1)
				{
					redoLogShardCandidate = redoLogShardCandidates[0];
				}
				else
				{
					redoLogShardCandidate = redoLogShardCandidates[RandomNumberGenerator.GetInt32(0, redoLogShardsCount)];
				}
				selectedRedoLogShardEngine = redoLogShardCandidate.databaseEngine;
				selectedRedoLogShardName = redoLogShardCandidate.name;
			} else{
				shardTransactions = null;
				redoLogTransaction = null;
				redoLogCheckpoint = null;
				consistentVirtualShards = null;
				selectedRedoLogShardName = null;
				selectedRedoLogShardEngine = null;
			}

			Dictionary<ushort, string> shardHead = new Dictionary<ushort, string>();
			foreach(ushort shard in readVirtualShards.Keys){
				shardHead.Add(shard, await SyncShard(shard));
			}

			Dictionary<ushort, IEnumerable<string>> bakedReadVirtualShards = new Dictionary<ushort, IEnumerable<string>>();

			foreach (KeyValuePair<ushort, Queue<string>> keyValuePair in readVirtualShards)
			{
				bakedReadVirtualShards.Add(keyValuePair.Key, keyValuePair.Value.ToArray());
			}

			IReadOnlyDictionary<string, string> coordinatorConditions = null;
			Queue<SyncShardTask> syncShardTasks = new Queue<SyncShardTask>();
		restart:
			foreach (ushort shard in bakedReadVirtualShards.Keys){
				Task<string> task = SyncShard(shard);
				syncShardTasks.Enqueue(new SyncShardTask(task, shard));
				await task;
			}
			
			Queue<ReadShardTask> asyncReadsQueue = new Queue<ReadShardTask>();
			
			Dictionary<string, string> allReads = new Dictionary<string, string>();
			foreach (KeyValuePair<ushort, IEnumerable<string>> keyValuePair in bakedReadVirtualShards)
			{
				ushort shard = keyValuePair.Key;
				asyncReadsQueue.Enqueue(new ReadShardTask(shards[shard].Execute(keyValuePair.Value, emptyDictionary, emptyDictionary), shard));
			}

			Dictionary<ushort, string> shardSynchronziationPositions = new Dictionary<ushort, string>();
			while (syncShardTasks.TryDequeue(out SyncShardTask task))
			{
				shardSynchronziationPositions.Add(task.shard, await task.task);
			}

			Dictionary<string, string> returns = new Dictionary<string, string>();
			
			Dictionary<string, string> redoLogWrites;
			Dictionary<string, string> coordinatorWrites;
			
			if(writing){
				if(coordinatorConditions is null){
					coordinatorConditions = await coordinatorState.Execute(consistentVirtualShards, emptyDictionary, emptyDictionary);
				}
				foreach(KeyValuePair<ushort, string> keyValuePair in shardSynchronziationPositions){
					ushort key = keyValuePair.Key;
					string condition = coordinatorConditions["Kellyanne_coordinator_checkpoint_" + key.ToString()];
					string value = keyValuePair.Value;
					if(condition is null && value is null){
						continue;
					}
					JsonConvert.PopulateObject(condition, redoLogCheckpoint);
					if (redoLogCheckpoint.id == value)
					{
						continue;
					}
					coordinatorConditions = null;
					await File.AppendAllTextAsync("c:\\users\\jessi\\desktop\\ldlog", redoLogCheckpoint.id + " - " + value + "\n");
					goto restart;
				}
				redoLogWrites = new Dictionary<string, string>();
				coordinatorWrites = new Dictionary<string, string>();
				redoLogCheckpoint.shard = selectedRedoLogShardName;
				foreach (KeyValuePair<ushort, Dictionary<string, string>> keyValuePair1 in shardTransactions){
					ushort key = keyValuePair1.Key;
					redoLogTransaction.changes = keyValuePair1.Value;
					string checkpointJson = coordinatorConditions["Kellyanne_coordinator_checkpoint_" + key.ToString()];
					if(checkpointJson is null){
						redoLogTransaction.previd = null;
						redoLogTransaction.prevshard = null;
					} else{
						JsonConvert.PopulateObject(checkpointJson, redoLogCheckpoint);
						redoLogTransaction.previd = redoLogCheckpoint.id;
						redoLogTransaction.prevshard = redoLogCheckpoint.shard;
					}
					string id = RandomString();
					redoLogCheckpoint.id = id;
					coordinatorWrites.Add("Kellyanne_coordinator_checkpoint_" + key.ToString(), JsonConvert.SerializeObject(redoLogCheckpoint));
					redoLogWrites.Add(id, JsonConvert.SerializeObject(redoLogTransaction));
				}
			} else{
				redoLogWrites = null;
				coordinatorWrites = null;
			}

			while (asyncReadsQueue.TryDequeue(out ReadShardTask readtask)){
				IReadOnlyDictionary<string, string> keyValuePairs = await readtask.task;
				string first = keyValuePairs["Kellyanne_storage_checkpoint_" + readtask.shard.ToString()];
				string second = shardSynchronziationPositions[readtask.shard];
				if(first is null && second is null){
					foreach (string key in keyValuePairs.Keys)
					{
						allReads.Add(key, null);
					}
					continue;
				}
				JsonConvert.PopulateObject(first, redoLogCheckpoint);
				if (redoLogCheckpoint.id == second)
				{
					foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
					{
						allReads.Add(keyValuePair.Key, keyValuePair.Value);
					}
					continue;
				}
				coordinatorConditions = null;
				await File.AppendAllTextAsync("c:\\users\\jessi\\desktop\\ldlog", "option 2\n");
				goto restart;
			}
			
			if(writing){
				foreach(KeyValuePair<string, string> condition in conditions){
					if(allReads[condition.Key] == condition.Value){
						continue;
					}
					break;
				}
				await selectedRedoLogShardEngine.Execute(emptyStringArray, emptyDictionary, redoLogWrites);
				IReadOnlyDictionary<string, string> keyValuePairs = await coordinatorState.Execute(coordinatorConditions.Keys, coordinatorConditions, coordinatorWrites);
				foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs){
					if(coordinatorConditions[keyValuePair.Key] == keyValuePair.Value){
						continue;
					}
					throw new Exception(JsonConvert.SerializeObject(coordinatorConditions) + " - " + JsonConvert.SerializeObject(keyValuePairs));
					coordinatorConditions = keyValuePairs;
					await File.AppendAllTextAsync("c:\\users\\jessi\\desktop\\ldlog", "option 3\n");
					goto restart;
				}
			}
			foreach (string read in reads)
			{
				returns.Add(read, allReads[read]);
			}
			return returns;
		}
		private static volatile int limit;
		private static async Task<string> Read(IDatabaseEngine databaseEngine, string key){
			return (await databaseEngine.Execute(new string[] { key }, emptyDictionary, emptyDictionary))[key];
		}
		private static string RandomString(){
			Span<byte> bytes = stackalloc byte[32];
			RandomNumberGenerator.Fill(bytes);
			return Convert.ToBase64String(bytes, Base64FormattingOptions.None);
		}
		private async Task<string> SyncShard(ushort shardid){
			//Sadly, we CANNOT use the optimistic functions framework
			string strshardid = shardid.ToString();
			string coordinatorCheckpointKey = "Kellyanne_coordinator_checkpoint_" + strshardid;
			Task<string> readtask1 = Read(coordinatorState, coordinatorCheckpointKey);
			string storageCheckpointKey = "Kellyanne_storage_checkpoint_" + strshardid;
			IDatabaseEngine transientStorageDatabaseEngine = shards[shardid];
			Task<string> readtask2 = Read(transientStorageDatabaseEngine, storageCheckpointKey);
			string[] storageCheckpointReads = new string[] { storageCheckpointKey };
			RedoLogTransaction redoLogTransaction = new RedoLogTransaction();
			RedoLogCheckpoint redoLogCheckpoint = new RedoLogCheckpoint();
			string coordinatorCheckpointJson = await readtask1;
			if(coordinatorCheckpointJson is null){
				return null;
			}
			JsonConvert.PopulateObject(coordinatorCheckpointJson, redoLogCheckpoint);
			string id = redoLogCheckpoint.id;
			if(id is null){
				throw new Exception("Unexpected null redo log head (should not reach here)");
			}
			string shard = redoLogCheckpoint.shard;
			string lastid = null;
			string lastshard = null;
			string endid = await readtask2;
			string storageCheckpointJson = endid;
			if (endid is { }){
				JsonConvert.PopulateObject(endid, redoLogCheckpoint);
				endid = redoLogCheckpoint.id;
			}
			Queue<Task> linkTasks = new Queue<Task>();
			while (id != endid){
				if (id is null)
				{
					throw new Exception("Unexpected redo log end (should not reach here)");
				}
				IDatabaseEngine redoLogShard = redoLogShards[shard];
				string readresult = await Read(redoLogShard, id);
				await File.AppendAllTextAsync("c:\\users\\jessi\\desktop\\ldlog", readresult + '\n');
				redoLogTransaction.changes = null;
				JsonConvert.PopulateObject(readresult, redoLogTransaction);
				if(lastid is { }){
					string nextid = redoLogTransaction.nextid;
					if(nextid is null){
						redoLogTransaction.nextid = lastid;
						redoLogTransaction.nextshard = lastshard;
						linkTasks.Enqueue(redoLogShard.Execute(emptyStringArray, new Dictionary<string, string>(){
							{id, readresult}
						}, new Dictionary<string, string>(){
							{id, JsonConvert.SerializeObject(redoLogTransaction)}
						}));
					}
				}

				lastid = id;
				lastshard = shard;
				id = redoLogTransaction.previd;
				shard = redoLogTransaction.prevshard;
			}
			while (linkTasks.TryDequeue(out Task tsk))
			{
				await tsk;
			}
			if (id is null){
				if (lastid is null){
					throw new Exception("Unexpected invalid redo log checkpoint (should not reach here)");
				}
				id = lastid;
				shard = lastshard;
				goto skipfirst;
			}
		startloop:
			if (id is null){
				throw new Exception("Unexpected invalid redo log checkpoint (should not reach here)");
			}
			redoLogTransaction.changes = null;
			string rdv = await Read(redoLogShards[shard], id);
			JsonConvert.PopulateObject(rdv, redoLogTransaction);
			
			string newid = redoLogTransaction.nextid;
			if (newid is null)
			{
				return id;
			}
			
			id = newid;
			shard = redoLogTransaction.nextshard;
		skipfirst:
			readtask1 = Read(redoLogShards[shard], id);
			redoLogCheckpoint.id = id;
			redoLogCheckpoint.shard = shard;
			string newRedoLogCheckpoint = JsonConvert.SerializeObject(redoLogCheckpoint);
			redoLogTransaction.changes = null;
			JsonConvert.PopulateObject(await readtask1, redoLogTransaction);
			Dictionary<string, string> changes = redoLogTransaction.changes;
			changes.Add(storageCheckpointKey, newRedoLogCheckpoint);
			string srcRedoLogTransaction = (await transientStorageDatabaseEngine.Execute(storageCheckpointReads, new Dictionary<string, string>() {
					{storageCheckpointKey, storageCheckpointJson}
				}, changes))[storageCheckpointKey];

			if (srcRedoLogTransaction == storageCheckpointJson)
			{
				//Won CAS, stick to optimistically assumed values
				await File.AppendAllTextAsync("c:\\users\\jessi\\desktop\\ldlog", "Won CAS!" + '\n');
				storageCheckpointJson = newRedoLogCheckpoint;
				await coordinatorState.Execute(emptyStringArray, emptyDictionary, new Dictionary<string, string>() { {coordinatorCheckpointKey, storageCheckpointJson} });
			}
			else
			{
				//Lost CAS, populate expected values and try again
				await File.AppendAllTextAsync("c:\\users\\jessi\\desktop\\ldlog", "Lost CAS!" + '\n');
				storageCheckpointJson = srcRedoLogTransaction;
				JsonConvert.PopulateObject(storageCheckpointJson, redoLogCheckpoint);
				id = redoLogCheckpoint.id;
				shard = redoLogCheckpoint.shard;
			}
			goto startloop;
		}
		

		[JsonObject(MemberSerialization.Fields)]
		private sealed class RedoLogTransaction{
			public Dictionary<string, string> changes;
			public string prevshard;
			public string previd;
			public string nextshard;
			public string nextid;
		}
		[JsonObject(MemberSerialization.Fields)]
		private sealed class RedoLogCheckpoint{
			public string id;
			public string shard;
		}
		

	}
}
*/