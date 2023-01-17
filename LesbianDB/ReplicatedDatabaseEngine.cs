using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace LesbianDB
{
	public sealed class ReplicatedDatabaseEngine : IDatabaseEngine
	{
		[JsonObject(MemberSerialization.Fields)]
		private sealed class RedoLogItem
		{
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
		private readonly IDatabaseEngine[] redoLogDatabaseEngines;
		private readonly IDatabaseEngine underlyingDatabaseEngine;

		public ReplicatedDatabaseEngine(IDatabaseEngine[] redoLogDatabaseEngines, IDatabaseEngine underlyingDatabaseEngine)
		{
			this.redoLogDatabaseEngines = redoLogDatabaseEngines ?? throw new ArgumentNullException(nameof(redoLogDatabaseEngines));
			this.underlyingDatabaseEngine = underlyingDatabaseEngine ?? throw new ArgumentNullException(nameof(underlyingDatabaseEngine));
		}

		private static readonly string[] emptyStringArray = new string[0];

		public async Task<IReadOnlyDictionary<string, string>> Execute(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes)
		{
			Dictionary<string, string> writes1 = new Dictionary<string, string>(writes);
			Dictionary<string, string> conditions1 = new Dictionary<string, string>(conditions);
			string[] readsarr = new string[1];
			async Task<string> Read(IDatabaseEngine databaseEngine, string key)
			{
				readsarr[0] = key;
				return (await databaseEngine.Execute(readsarr, SafeEmptyReadOnlyDictionary<string, string>.instance, SafeEmptyReadOnlyDictionary<string, string>.instance))[key];
			}
			static string Random()
			{
				Span<byte> bytes = stackalloc byte[32];
				RandomNumberGenerator.Fill(bytes);
				return Convert.ToBase64String(bytes, Base64FormattingOptions.None);
			}
			static IEnumerable<string> AppendExtraRead(IEnumerable<string> enumerable){
				foreach(string str in enumerable){
					yield return str;
				}
				yield return "LesbianDB_reserved_multi_master_head";
			}
			static async void BackgroundPurge(string id, IDatabaseEngine databaseEngine)
			{
				Misc.BackgroundAwait(databaseEngine.Execute(emptyStringArray, SafeEmptyReadOnlyDictionary<string, string>.instance, new Dictionary<string, string>() {
				{id, null}
			}));
			}
			bool dowrite = writes.Count > 0;
			RedoLogHead redoLogHead = new RedoLogHead();
			string writeid;
			int writeshard;
			Task putshard;
			Dictionary<string, bool> readlist;
			if (dowrite){
				writeid = Random();
				writeshard = Misc.FastRandom(0, redoLogDatabaseEngines.Length);
				string writejson = JsonConvert.SerializeObject(new RedoLogItem1(writes));
				putshard = redoLogDatabaseEngines[writeshard].Execute(emptyStringArray, SafeEmptyReadOnlyDictionary<string, string>.instance, new Dictionary<string, string>() { { writeid, writejson } });
				redoLogHead.id = writeid;
				redoLogHead.shard = writeshard;
				writes1.Add("LesbianDB_reserved_multi_master_head", JsonConvert.SerializeObject(redoLogHead));
				readlist = new Dictionary<string, bool>();
				foreach(string str in conditions.Keys){
					readlist.Add(str, false);
				}
				foreach(string str in AppendExtraRead(reads)){
					readlist.TryAdd(str, false);
				}
			} else{
				writeid = null;
				putshard = null;
				writeshard = 0;
				readlist = null;
			}
			
			
			RedoLogItem redoLogItem = new RedoLogItem();
			Dictionary<string, string> tempConditions = new Dictionary<string, string>();
			string read;
			string head;
			string id;
			int shard;

			//Synchronize
			head = await Read(underlyingDatabaseEngine, "LesbianDB_reserved_multi_master_head");
			if (head is null)
			{
				id = "LesbianDB_reserved_Kellyanne_start";
				shard = 0;
				read = await Read(redoLogDatabaseEngines[0], "LesbianDB_reserved_Kellyanne_start");
				if (read is null)
				{
					goto donesync;
				}
				goto nochkey;
			}

		startsync1:
			JsonConvert.PopulateObject(head, redoLogHead);
			id = redoLogHead.id;
			shard = redoLogHead.shard;
		startsync2:
			read = await Read(redoLogDatabaseEngines[shard], id);
		nochkey:
			redoLogItem.changes?.Clear();
			JsonConvert.PopulateObject(read, redoLogItem);
			string nextid = redoLogItem.nextid;
			if (nextid is null)
			{
				goto donesync;
			}
			shard = redoLogItem.nextshard;
			redoLogItem.changes.Clear();
			JsonConvert.PopulateObject(await Read(redoLogDatabaseEngines[shard], nextid), redoLogItem);
			Dictionary<string, string> changes = new Dictionary<string, string>(redoLogItem.changes);
			redoLogHead.id = nextid;
			redoLogHead.shard = shard;
			string newHead = JsonConvert.SerializeObject(redoLogHead);
			changes.Add("LesbianDB_reserved_multi_master_head", newHead);
			tempConditions["LesbianDB_reserved_multi_master_head"] = head;
			readsarr[0] = "LesbianDB_reserved_multi_master_head";
			string oldHead = (await underlyingDatabaseEngine.Execute(readsarr, tempConditions, changes))["LesbianDB_reserved_multi_master_head"];
			if(oldHead == head)
			{
				head = newHead;
				id = nextid;
				goto startsync2;
			} else{
				head = oldHead;
				goto startsync1;
			}
			
		donesync: ;
			
			if (dowrite){
				//slow path
				conditions1["LesbianDB_reserved_multi_master_head"] = head;
				IDatabaseEngine logshard = redoLogDatabaseEngines[writeshard];
				redoLogItem.nextid = writeid;
				redoLogItem.nextshard = writeshard;
				await putshard;

				try{
					readsarr[0] = id;
					if((await redoLogDatabaseEngines[shard].Execute(readsarr, new Dictionary<string, string>() { { id, read } }, new Dictionary<string, string>() { { id, JsonConvert.SerializeObject(redoLogItem)} }))[id] != read){
						BackgroundPurge(writeid, logshard);
						head = await Read(underlyingDatabaseEngine, "LesbianDB_reserved_multi_master_head");
						goto startsync1;
					}
					IReadOnlyDictionary<string, string> allreads = await underlyingDatabaseEngine.Execute(readlist.Keys, conditions1, writes1);
					string srchead = allreads["LesbianDB_reserved_multi_master_head"];
					if (srchead == head)
					{
						Dictionary<string, string> result = new Dictionary<string, string>();
						foreach (string str in reads)
						{
							result.TryAdd(str, allreads[str]);
						}
						return result;
					}
					head = srchead;
					BackgroundPurge(writeid, logshard);
					goto startsync1;
				} catch{
					BackgroundPurge(writeid, logshard);
					throw;
				}
			} else{
				//fast path
				return await underlyingDatabaseEngine.Execute(reads, SafeEmptyReadOnlyDictionary<string, string>.instance, SafeEmptyReadOnlyDictionary<string, string>.instance);
			}


		}
	}
}
