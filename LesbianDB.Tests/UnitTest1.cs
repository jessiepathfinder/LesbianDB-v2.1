using NUnit.Framework;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Collections.Generic;
using System;
using System.IO;
using LesbianDB.Optimism.Core;
using System.IO.Compression;
using System.Threading;
using LesbianDB.Optimism.YuriTables;
using System.Numerics;
using LesbianDB.Optimism.Armitage;

namespace LesbianDB.Tests
{
	public sealed class Tests
	{
		[SetUp]
		public void Setup()
		{
			
		}


		//========== LesbianDB.YuriTables ==========
		[Test]
		public async Task ForwardYuriTablesArrayQueue(){
			//The EnhancedSequentialAccessDictionary is never flushed, so it's effectively a pure memory engine
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0);
			for(int i = 0; i < 4096; ){
				string str = (i++).ToString();
				await optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
					await optimisticExecutionScope.ArrayPushEnd("queue", str);
					return false;
				});
			}
			for (BigInteger i = 0; i < 4096;)
			{
				Assert.AreEqual(i.ToString(), await optimisticExecutionManager.ExecuteOptimisticFunction((IOptimisticExecutionScope optimisticExecutionScope) => optimisticExecutionScope.ArrayGetValue("queue", i)));
				++i;
			}
			for (BigInteger i = 0; i < 4096;)
			{
				ReadResult readResult = await optimisticExecutionManager.ExecuteOptimisticFunction((IOptimisticExecutionScope optimisticExecutionScope) => optimisticExecutionScope.ArrayTryPopFirst("queue"));
				Assert.IsTrue(readResult.exists);
				Assert.AreEqual((i++).ToString(), readResult.value);
			}
			Assert.IsFalse((await optimisticExecutionManager.ExecuteOptimisticFunction((IOptimisticExecutionScope optimisticExecutionScope) => optimisticExecutionScope.ArrayTryPopFirst("queue"))).exists);
			Assert.IsFalse((await optimisticExecutionManager.ExecuteOptimisticFunction((IOptimisticExecutionScope optimisticExecutionScope) => optimisticExecutionScope.ArrayTryPopLast("queue"))).exists);
		}

		[Test]
		public async Task BackwardYuriTablesArrayQueue()
		{
			//The EnhancedSequentialAccessDictionary is never flushed, so it's effectively a pure memory engine
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0);
			for (int i = 0; i < 4096;)
			{
				string str = (i++).ToString();
				await optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
					await optimisticExecutionScope.ArrayPushStart("queue", str);
					return false;
				});
			}
			for (BigInteger i = 0; i < 4096;)
			{
				BigInteger invert = 4095 - i;
				Assert.AreEqual(i.ToString(), await optimisticExecutionManager.ExecuteOptimisticFunction((IOptimisticExecutionScope optimisticExecutionScope) => optimisticExecutionScope.ArrayGetValue("queue", invert)));
				++i;
			}
			for (BigInteger i = 0; i < 4096;)
			{
				ReadResult readResult = await optimisticExecutionManager.ExecuteOptimisticFunction((IOptimisticExecutionScope optimisticExecutionScope) => optimisticExecutionScope.ArrayTryPopLast("queue"));
				Assert.IsTrue(readResult.exists);
				Assert.AreEqual((i++).ToString(), readResult.value);
			}
			Assert.IsFalse((await optimisticExecutionManager.ExecuteOptimisticFunction((IOptimisticExecutionScope optimisticExecutionScope) => optimisticExecutionScope.ArrayTryPopFirst("queue"))).exists);
			Assert.IsFalse((await optimisticExecutionManager.ExecuteOptimisticFunction((IOptimisticExecutionScope optimisticExecutionScope) => optimisticExecutionScope.ArrayTryPopLast("queue"))).exists);
		}

		private static IEnumerable<ushort> Random2(){
			Dictionary<ushort, bool> keyValuePairs = new Dictionary<ushort, bool>();
			while(keyValuePairs.Count < 4096){
				ushort val = (ushort)RandomNumberGenerator.GetInt32(0, 4096);
				if(keyValuePairs.TryAdd(val, false)){
					yield return val;
				}
			}
		}
		[Test]
		public async Task BTreeAddSelect(){
			await new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0).ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				foreach(BigInteger bigInteger in Random2()){
					await optimisticExecutionScope.BTreeTryInsert("mytree", bigInteger);
				}
				BigInteger reference = BigInteger.Zero;
				await foreach (BigInteger bigInteger1 in optimisticExecutionScope.BTreeSelect("mytree", CompareOperator.LessThan, false, 100)){
					Assert.AreEqual(reference++, bigInteger1);
				}
				return false;
			});
		}
		[Test] public async Task SortedSet(){
			await new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0).ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				foreach (BigInteger bigInteger in Random2())
				{
					string strbigint = bigInteger.ToString();
					await optimisticExecutionScope.ZSet("mysortedset", "meow_" + strbigint, strbigint + " LesbianDB now has an ISAM", bigInteger);
				}
				await optimisticExecutionScope.ZSet("mysortedset", "meow_50", "asmr yuri lesbian neck kissing", 50);
				await optimisticExecutionScope.ZSet("mysortedset", "meow_40", "40 LesbianDB now has an ISAM", 30);
				BigInteger reference = BigInteger.Zero;
				bool first30 = true;
				await foreach (SortedSetReadResult sortedSetReadResult in optimisticExecutionScope.ZSelect("mysortedset", CompareOperator.LessThan, 100, false))
				{
					if (reference == 40)
					{
						reference += 1;
					}
					string strbigint = reference.ToString();
					if (sortedSetReadResult.bigInteger == 50){
						Assert.AreEqual("asmr yuri lesbian neck kissing", sortedSetReadResult.value);
					} else{
						if (sortedSetReadResult.bigInteger == 30)
						{
							Assert.True(sortedSetReadResult.key == "meow_" + strbigint || sortedSetReadResult.key == "meow_40");
							Assert.True(sortedSetReadResult.value == "30 LesbianDB now has an ISAM" || sortedSetReadResult.value == "40 LesbianDB now has an ISAM");
							if (first30)
							{
								first30 = false;
								Assert.AreEqual(reference, sortedSetReadResult.bigInteger);
								continue;
							}
						}
						else
						{
							Assert.AreEqual("meow_" + strbigint, sortedSetReadResult.key);
							Assert.AreEqual(strbigint + " LesbianDB now has an ISAM", sortedSetReadResult.value);
						}
					}
					
					Assert.AreEqual(reference++, sortedSetReadResult.bigInteger);
				}
				return false;
			});
		}

		[Test]
		public async Task HashReadWriteDelete(){
			await new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0).ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				await optimisticExecutionScope.HashSet("jessielesbian", "iscute", "true");
				await optimisticExecutionScope.HashSet("jessielesbian", "uses.facebook", "false");
				await optimisticExecutionScope.HashSet("jessielesbian", "uses.bitcoin", "yes");
				Assert.AreEqual("true", await optimisticExecutionScope.HashGet("jessielesbian", "iscute"));
				Assert.AreEqual("false", await optimisticExecutionScope.HashGet("jessielesbian", "uses.facebook"));
				Assert.AreEqual("yes", await optimisticExecutionScope.HashGet("jessielesbian", "uses.bitcoin"));

				await optimisticExecutionScope.HashSet("jessielesbian", "uses.bitcoin", "no");
				Assert.AreEqual("no", await optimisticExecutionScope.HashGet("jessielesbian", "uses.bitcoin"));
				await optimisticExecutionScope.HashSet("jessielesbian", "uses.facebook", null);
				Assert.AreEqual("true", await optimisticExecutionScope.HashGet("jessielesbian", "iscute"));
				Assert.AreEqual(null, await optimisticExecutionScope.HashGet("jessielesbian", "uses.facebook"));
				Assert.AreEqual("no", await optimisticExecutionScope.HashGet("jessielesbian", "uses.bitcoin"));
				await optimisticExecutionScope.HashSet("jessielesbian", "uses.facebook", "false");
				Assert.AreEqual("true", await optimisticExecutionScope.HashGet("jessielesbian", "iscute"));
				Assert.AreEqual("false", await optimisticExecutionScope.HashGet("jessielesbian", "uses.facebook"));
				Assert.AreEqual("no", await optimisticExecutionScope.HashGet("jessielesbian", "uses.bitcoin"));
				return false;
			});
		}
		[Test]
		public async Task Table(){
			await new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0).ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				//insert rows
				await optimisticExecutionScope.TryCreateTable(new Dictionary<string, ColumnType>
				{
					{"name", ColumnType.UniqueString},
					{"id", ColumnType.UniqueString},
					{"height", ColumnType.SortedInt},
					{"description", ColumnType.DataString}
				}, "lesbians");
				await optimisticExecutionScope.TableInsert("lesbians", new Dictionary<string, string> {
					{"name", "jessielesbian" },
					{"id", "1"},
					{"description", "the cute, nice, and lesbian creator of LesbianDB v2.1"}
				}, new Dictionary<string, BigInteger> {
					{"height", 160}
				});
				await optimisticExecutionScope.TableInsert("lesbians", new Dictionary<string, string> {
					{"name", "vnch-chan" },
					{"id", "2"},
					{"description", "busy making vietnam democratic"}
				}, new Dictionary<string, BigInteger> {
					{"height", 155}
				});
				await optimisticExecutionScope.TableInsert("lesbians", new Dictionary<string, string> {
					{"name", "hillary-clinton" },
					{"id", "3"},
					{"description", "running for president"}
				}, new Dictionary<string, BigInteger> {
					{"height", 210}
				});

				//select tests
				Assert.IsNull(await optimisticExecutionScope.TableTrySelectPrimaryKey("lesbians", "name", "adolf-hitler"));

				Row testrow = await optimisticExecutionScope.TableTrySelectPrimaryKey("lesbians", "name", "jessielesbian");
				Assert.NotNull(testrow);
				Assert.AreEqual("jessielesbian", testrow["name"]);
				Assert.AreEqual("1", testrow["id"]);
				Assert.AreEqual("160", testrow["height"]);
				int count = 0;
				await foreach(Row row in optimisticExecutionScope.TableTrySelectSortedRow("lesbians", "height", CompareOperator.LessThan, 200, false)){
					Assert.AreNotEqual("hillary-clinton", row["name"]);
					++count;
				}
				Assert.AreEqual(2, count);

				await optimisticExecutionScope.TableUpdateRow(testrow, "id", "100");
				testrow = await optimisticExecutionScope.TableTrySelectPrimaryKey("lesbians", "id", "100");
				Assert.NotNull(testrow);
				Assert.AreEqual("jessielesbian", testrow["name"]);
				Assert.AreEqual("100", testrow["id"]);
				Assert.AreEqual("160", testrow["height"]);
				await optimisticExecutionScope.TableUpdateSortedIntRow("lesbians", testrow, "height", 300);
				await foreach(Row row in optimisticExecutionScope.TableTrySelectSortedRow("lesbians", "height", CompareOperator.EqualTo, 300, false)){
					Assert.NotNull(row);
					Assert.AreEqual("jessielesbian", row["name"]);
					Assert.AreEqual("100", row["id"]);
					Assert.AreEqual("300", row["height"]);
					++count;
				}
				Assert.AreEqual(3, count);
				await optimisticExecutionScope.TableDeleteRow(testrow);
				await foreach (Row row in optimisticExecutionScope.TableTrySelectSortedRow("lesbians", "height", CompareOperator.GreaterThan, 200, false))
				{
					Assert.AreEqual(4, ++count);
					Assert.NotNull(row);
					Assert.AreEqual("hillary-clinton", row["name"]);
					Assert.AreEqual("3", row["id"]);
					Assert.AreEqual("210", row["height"]);
				}
				await foreach (Row row in optimisticExecutionScope.TableSelectAllOrderedBy("lesbians", "height", false)){
					Assert.NotNull(row);
					if (++count == 5){
						Assert.AreEqual("vnch-chan", row["name"]);
						Assert.AreEqual("2", row["id"]);
						Assert.AreEqual("155", row["height"]);
					} else{
						Assert.AreEqual(6, count);
						Assert.AreEqual("hillary-clinton", row["name"]);
						Assert.AreEqual("3", row["id"]);
						Assert.AreEqual("210", row["height"]);
					}
				}
				return false;
			});
		}


		//========== LesbianDB ==========
		[Test]
		public async Task SpeculativeExecution(){
			SpeculativeExecutionManager<int> speculativeExecutionManager = new SpeculativeExecutionManager<int>(SpeculativeIncrement, new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0), long.MaxValue);
			
			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await speculativeExecutionManager.Call(""));
			}
		}
		private static Task<int> SpeculativeIncrement(string input, IOptimisticExecutionScope optimisticExecutionScope){
			return IncrementOptimisticCounter(optimisticExecutionScope);
		}
		[Test]
		public async Task YuriMallocOptimismCounter()
		{
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(new SequentialAccessAsyncDictionary(new YuriMalloc())), 0);
			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
			}
		}
		[Test]
		public async Task NVYuriCompressZramSaskiaOptimismCounter(){
			EnhancedSequentialAccessDictionary dictionary = new EnhancedSequentialAccessDictionary(new EphemeralSwapHandle(new YuriMalloc()));
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary(new EphemeralSwapHandle(new AsyncCompressionZram(NVYuriCompressCore.Compress, NVYuriCompressCore.Decompress)), CompressionLevel.NoCompression)), 0);
			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
				if (i % 16 == 0)
				{
					await dictionary.Flush();
				}
			}
		}
		[Test]
		public async Task NVYuriCompressYuriMallocSaskiaOptimismCounter()
		{
			EnhancedSequentialAccessDictionary dictionary = new EnhancedSequentialAccessDictionary(new EphemeralSwapHandle(new YuriMalloc()));
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary(new EphemeralSwapHandle(new AsyncCompressionZram(NVYuriCompressCore.Compress, NVYuriCompressCore.Decompress, new YuriMalloc(), 268435456)), CompressionLevel.NoCompression)), 0);
			for (int i = 0; i < 1024;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
				await dictionary.Flush();
			}
			Interlocked.MemoryBarrier();
			byte[] bytes = new byte[268435456];
			Interlocked.MemoryBarrier();
			for (int i = 1024; i < 2048;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
				await dictionary.Flush();
			}
			Interlocked.MemoryBarrier();
			GC.KeepAlive(bytes);
			bytes = null;
			GC.Collect();
			Interlocked.MemoryBarrier();
			for (int i = 2048; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
				if (i % 16 == 0)
				{
					await dictionary.Flush();
				}
			}
		}
		[Test]
		public async Task YuriMallocSaskiaOptimismCounter()
		{
			EnhancedSequentialAccessDictionary dictionary = new EnhancedSequentialAccessDictionary(new EphemeralSwapHandle(new YuriMalloc()));
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(dictionary), 0);
			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
				if(i % 16 == 0){
					await dictionary.Flush();
				}
			}
		}
		[Test]
		public async Task OnDiskSaskiaOptimismCounter()
		{
			EnhancedSequentialAccessDictionary dictionary = new EnhancedSequentialAccessDictionary(new FileSwapHandle(), true);
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(dictionary), 0);
			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
				if (i % 16 == 0)
				{
					await dictionary.Flush();
				}
			}
		}
		[Test]
		public async Task SaskiaCache()
		{
			YuriMalloc yuriMalloc = new YuriMalloc();
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(new RandomReplacementWriteThroughCache(new RandomFlushingCache(() => new EnhancedSequentialAccessDictionary(new EphemeralSwapHandle(yuriMalloc)), 0, true), 0)), 0);
			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
			}
		}
		[Test]
		public async Task ZRamSaskiaOptimismCounter()
		{
			EnhancedSequentialAccessDictionary dictionary = new EnhancedSequentialAccessDictionary();
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(dictionary), 0);
			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
				if (i % 16 == 0)
				{
					await dictionary.Flush();
				}
			}
		}
		[Test]
		public async Task LevelDBOptimismCounter()
		{
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new LevelDBEngine(Misc.GetRandomFileName()), 0);
			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
			}
		}
		[Test]
		public async Task RemoteOptimismCounter()
		{
			string url = Environment.GetEnvironmentVariable("LesbianDB_TestRemoteDatabase");
			if(url is null){
				Assert.Inconclusive();
			}
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new RemoteDatabaseEngine(new Uri(url)), 0);
			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
			}
		}
		[Test]
		public async Task SaskiaZramYuriBinlogOptimisticCounter()
		{
			using MemoryStream binlog = new MemoryStream();
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary(), binlog), 0);
			for (int i = 0; i < 2048;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
			}
			binlog.Seek(0, SeekOrigin.Begin);
			EnhancedSequentialAccessDictionary dictionary = new EnhancedSequentialAccessDictionary();
			await YuriDatabaseEngine.RestoreBinlog(binlog, dictionary);
			optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(dictionary), 0);
			for (int i = 2048; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
			}
		}
		[Test]
		public async Task YuriBinlogLevelDBOptimismCounter()
		{
			using MemoryStream binlog = new MemoryStream();
			string filename = Misc.GetRandomFileName();
			using (LevelDBEngine levelDBEngine = await LevelDBEngine.RestoreBinlog(binlog, filename)) {
				OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(levelDBEngine, 0);
				for (int i = 0; i < 1024;)
				{
					Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
				}
			}
			using (LevelDBEngine levelDBEngine = await LevelDBEngine.RestoreBinlog(binlog, Misc.GetRandomFileName()))
			{
				OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(levelDBEngine, 0);
				for (int i = 1024; i < 2048;)
				{
					Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
				}
			}
			binlog.Seek(1234, SeekOrigin.Begin);
			using (LevelDBEngine levelDBEngine = await LevelDBEngine.RestoreBinlog(binlog, filename))
			{
				OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(levelDBEngine, 0);
				for (int i = 2048; i < 4096;)
				{
					Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
				}
			}
		}
		private static async Task<int> IncrementOptimisticCounter(IOptimisticExecutionScope optimisticExecutionScope){
			int value = Convert.ToInt32(await optimisticExecutionScope.Read("counter"));
			optimisticExecutionScope.Write("counter", (value + 1).ToString());
			return value;
		}
	}
	public static class BuyBitcoinExample{
		private static readonly OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 268435456);
		/// <summary>
		/// A well-written optimistic function
		/// </summary>
		public static Task<string> BuyBitcoin(ulong userid, decimal amount, decimal price)
		{
			string struserid = userid.ToString();
			decimal output = amount / price;
			string usdBalanceKey = struserid + ".balance.usd";
			string bitcoinBalanceKey = struserid + ".balance.bitcoin";
			return optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				decimal newusdbalance = Convert.ToDecimal(await optimisticExecutionScope.Read(usdBalanceKey)) - amount;
				if(newusdbalance < 0){
					return "insufficent USD balance!";
				}
				optimisticExecutionScope.Write(usdBalanceKey, newusdbalance.ToString());
				optimisticExecutionScope.Write(bitcoinBalanceKey, (Convert.ToDecimal(bitcoinBalanceKey) + output).ToString());
				return "successfully purchased bitcoin!";
			});
		}
	}
}