using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace LesbianDB{
	public sealed class Kellyanne : IDatabaseEngine{

		private readonly AsyncMutex asyncMutex = new AsyncMutex();
		private readonly IDatabaseEngine[] redoLogShards;
		private readonly IDatabaseEngine[] ephemeralStorageShards;
		private static readonly ulong heighthash = Misc.HashString4("LesbianDB_reserved_Kellyanne_head") + 1;
		private readonly IDatabaseEngine continueEngine;
		private readonly ulong modulo;

		public Kellyanne(IDatabaseEngine[] redoLogShards, IDatabaseEngine[] ephemeralStorageShards)
		{
			this.redoLogShards = redoLogShards ?? throw new ArgumentNullException(nameof(redoLogShards));
			this.ephemeralStorageShards = ephemeralStorageShards ?? throw new ArgumentNullException(nameof(ephemeralStorageShards));
			continueEngine = ephemeralStorageShards[(int)(heighthash % ((ulong)ephemeralStorageShards.Length))];
			int len = ephemeralStorageShards.Length;
			if(len < 1){
				throw new InvalidOperationException("Kellyanne sharded database must contain at least 1 ephemeral storage shard");
			}
			modulo = (ulong)ephemeralStorageShards.Length;
		}
		[JsonObject(MemberSerialization.Fields)]
		private sealed class RedoLogItem{
			public string nextid;
			public int nextshard;
			public Dictionary<string, string> changes = new Dictionary<string, string>();
		}
		[JsonObject(MemberSerialization.Fields)]
		private sealed class RedoLogItem1
		{
			public readonly string nextid;
			public readonly int nextshard;
			public readonly IReadOnlyDictionary<string, string> changes;

			public RedoLogItem1(IReadOnlyDictionary<string, string> changes)
			{
				this.changes = changes;
			}
		}
		[JsonObject(MemberSerialization.Fields)]
		private sealed class RedoLogHead
		{
			public string id;
			public int shard;
		}
		private static readonly IReadOnlyDictionary<string, string> emptyDictionary = SafeEmptyReadOnlyDictionary<string, string>.instance;
		private static readonly string[] emptyStringArray = new string[0];
		private readonly struct RedoLogSyncResult{
			public readonly string read;
			public readonly string id;
			public readonly int shard;
			public readonly RedoLogItem redoLogItem;

			public RedoLogSyncResult(string read, string id, int shard, RedoLogItem redoLogItem)
			{
				this.read = read;
				this.id = id;
				this.shard = shard;
				this.redoLogItem = redoLogItem;
			}
			public RedoLogSyncResult(RedoLogItem redoLogItem){
				this.redoLogItem = redoLogItem;
				read = null;
				id = "LesbianDB_reserved_Kellyanne_start";
				shard = 0;
			}
		}
		private async Task<RedoLogSyncResult> Sync()
		{
			string[] reads = new string[1];
			async Task<string> Read(IDatabaseEngine databaseEngine, string key)
			{
				reads[0] = key;
				return (await databaseEngine.Execute(reads, emptyDictionary, emptyDictionary))[key];
			}
			Task<string> task = Read(continueEngine, "LesbianDB_reserved_Kellyanne_head");
			RedoLogItem redoLogItem = new RedoLogItem();
			RedoLogHead redoLogHead = new RedoLogHead();
			Task RewriteLogHead()
			{
				return continueEngine.Execute(emptyStringArray, emptyDictionary, new Dictionary<string, string>() { { "LesbianDB_reserved_Kellyanne_head", JsonConvert.SerializeObject(redoLogHead) } });
			}
			Queue<Task> tasks = new Queue<Task>();
			string head = await task;
			string id;
			int shard;
			if (head is null)
			{
				id = "LesbianDB_reserved_Kellyanne_start";
				shard = 0;
			}
			else
			{
				JsonConvert.PopulateObject(head, redoLogHead);
				id = redoLogHead.id;
				if(id is null){
					return new RedoLogSyncResult(redoLogItem);
				}
				shard = redoLogHead.shard;
			}
			while (true)
			{
				Task<string> task1 = Read(redoLogShards[shard], id);
				Dictionary<int, Dictionary<string, string>> shardChanges = new Dictionary<int, Dictionary<string, string>>();
				string read = await task1;
				if (read is null)
				{
					if (id == "LesbianDB_reserved_Kellyanne_start")
					{
						return new RedoLogSyncResult(redoLogItem);
					}
					throw new NullReferenceException("Unexpected null redo log item (should not reach here)");
				}
				redoLogItem.changes?.Clear();
				JsonConvert.PopulateObject(read, redoLogItem);
				Dictionary<string, string> changes = redoLogItem.changes;
				if (changes is null){
					redoLogItem.changes = new Dictionary<string, string>();
				} else{
					foreach (KeyValuePair<string, string> keyValuePair in redoLogItem.changes)
					{
						string key = keyValuePair.Key;
						int hash = (int)(Misc.HashString4(key) % modulo);
						if (!shardChanges.TryGetValue(hash, out Dictionary<string, string> dictionary))
						{
							dictionary = new Dictionary<string, string>();
							shardChanges.Add(hash, dictionary);
						}
						dictionary.Add(key, keyValuePair.Value);
					}
					foreach (KeyValuePair<int, Dictionary<string, string>> keyValuePair1 in shardChanges)
					{
						tasks.Enqueue(ephemeralStorageShards[keyValuePair1.Key].Execute(emptyStringArray, emptyDictionary, keyValuePair1.Value));
					}
					redoLogHead.id = id;
					redoLogHead.shard = shard;
					id = redoLogItem.nextid;
					shard = redoLogItem.nextshard;
					await tasks.ToArray();
				}
				if (id is null)
				{
					await RewriteLogHead();
					return new RedoLogSyncResult(read, redoLogHead.id, redoLogHead.shard, redoLogItem);
				}
				if (Misc.FastRandom(0, 4096) == 0)
				{
					await RewriteLogHead();
				}	
			}
		}
		private static string Random()
		{
			Span<byte> bytes = stackalloc byte[32];
			RandomNumberGenerator.Fill(bytes);
			return Convert.ToBase64String(bytes, Base64FormattingOptions.None);
		}
		private static async void BackgroundPurge(Task prev, string id, IDatabaseEngine databaseEngine){
			await prev;
			Misc.BackgroundAwait(databaseEngine.Execute(emptyStringArray, emptyDictionary, new Dictionary<string, string>() {
				{id, null}
			}));
		}
		public async Task<IReadOnlyDictionary<string, string>> Execute(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes)
		{
			bool write = writes.Count > 0;
			Dictionary<string, bool> readList = new Dictionary<string, bool>();
			int shard;
			string id;
			Task append;
			if (write)
			{
				foreach (string str in conditions.Keys)
				{
					readList.Add(str, false);
				}
				shard = Misc.FastRandom(0, redoLogShards.Length);
				id = Random();
				append = redoLogShards[shard].Execute(emptyStringArray, emptyDictionary, new Dictionary<string, string>() {
					{id, JsonConvert.SerializeObject(new RedoLogItem1(writes))}
				});
			} else{
				shard = 0;
				id = null;
				append = null;
			}
			string[] tmpreads = new string[1];
			async Task<string> Read(IDatabaseEngine databaseEngine, string key)
			{
				tmpreads[0] = key;
				return (await databaseEngine.Execute(tmpreads, emptyDictionary, emptyDictionary))[key];
			}
			foreach (string str in reads){
				readList.TryAdd(str, false);
			}
			Dictionary<string, string> allReads;
			Dictionary<int, Queue<string>> shardReads = new Dictionary<int, Queue<string>>();
			foreach(string key in readList.Keys){
				int hash = (int)(Misc.HashString4(key) % modulo);
				if(!shardReads.TryGetValue(hash, out Queue<string> queue)){
					queue = new Queue<string>();
					shardReads.Add(hash, queue);
				}
				queue.Enqueue(key);
			}
			Dictionary<int, string[]> bakedShardReads = new Dictionary<int, string[]>();

			foreach (KeyValuePair<int, Queue<string>> keyValuePair in shardReads)
			{
				bakedShardReads.Add(keyValuePair.Key, keyValuePair.Value.ToArray());
			}
			Queue<Task<IReadOnlyDictionary<string, string>>> readTasks = new Queue<Task<IReadOnlyDictionary<string, string>>>();
			Dictionary<string, string> returns = new Dictionary<string, string>();
			await asyncMutex.Enter();

			try{
			start:				
				Task<RedoLogSyncResult> synchronizeTask = Sync();
				allReads = new Dictionary<string, string>();
				RedoLogSyncResult redoLogSyncResult = await synchronizeTask;
				foreach(KeyValuePair<int, string[]> keyValuePair in bakedShardReads){
					readTasks.Enqueue(ephemeralStorageShards[keyValuePair.Key].Execute(keyValuePair.Value, emptyDictionary, emptyDictionary));
				}
				string writeback;
				if(write){
					redoLogSyncResult.redoLogItem.nextid = id;
					redoLogSyncResult.redoLogItem.nextshard = shard;
					writeback = JsonConvert.SerializeObject(redoLogSyncResult.redoLogItem);
				} else{
					writeback = null;
				}
				
				while (readTasks.TryDequeue(out Task<IReadOnlyDictionary<string, string>> readTask)){
					foreach(KeyValuePair<string, string> keyValuePair in await readTask){
						allReads.Add(keyValuePair.Key, keyValuePair.Value);
					}
				}
				if(write){
					foreach(KeyValuePair<string, string> keyValuePair in conditions){
						if(allReads[keyValuePair.Key] == keyValuePair.Value){
							continue;
						}
						BackgroundPurge(append, id, ephemeralStorageShards[shard]);
						goto nowrite;
					}
					tmpreads[0] = redoLogSyncResult.id;
					if((await redoLogShards[redoLogSyncResult.shard].Execute(tmpreads, new Dictionary<string, string>() {
						{redoLogSyncResult.id, redoLogSyncResult.read}
					}, new Dictionary<string, string>() {
						{redoLogSyncResult.id, writeback}
					}))[redoLogSyncResult.id] == redoLogSyncResult.read){
						goto nowrite;
					}
					goto start;
				}
			} finally{
				asyncMutex.Exit();
			}
		nowrite:;
			foreach (string key in reads)
			{
				returns.TryAdd(key, allReads[key]);
			}
			return returns;
		}
	}
}