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
using LesbianDB.Optimism.Snapshot;
using LesbianDB.IntelliEX;
using LesbianDB.Optimism.YuriTables.NextGeneration;
using System.Text;

namespace LesbianDB.Tests
{
	public sealed class Tests
	{
		[SetUp]
		public void Setup()
		{
			
		}
		//========== LesbianDB.IntelliEX ===========
		[Test]
		public async Task IntelliEXOptimisticCointer()
		{
			IntelligentExecutionManager intelligentExecutionManager = new IntelligentExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), "thectr");
			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await intelligentExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
			}
		}
		//========== LesbianDB.Snapshots ===========
		[Test]
		public async Task SnapshotConsistentOptimisticCounter()
		{
			SnapshotConsistentExecutioner optimisticExecutionManager = new SnapshotConsistentExecutioner("snapshots", new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0));
			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
			}
		}
		[Test]
		public async Task Snapshot(){
			await new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0).ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				Assert.IsNull(await optimisticExecutionScope.SnapshotRead("snapshots", "test"));
				Assert.IsNull(await optimisticExecutionScope.SnapshotRead("snapshots", "test", 0));
				Task Set(string value){
					return optimisticExecutionScope.SnapshotWrite("snapshots", "test", value);
				}
				async Task SetAndCheck(string str, ulong id){
					await Set(str);
					Assert.AreEqual(str, await optimisticExecutionScope.SnapshotRead("snapshots", "test"));
					Assert.AreEqual(str, await optimisticExecutionScope.SnapshotRead("snapshots", "test", id));
				}
				await SetAndCheck("a", 0);
				await SetAndCheck("b", 0);
				await Set(null);
				Assert.IsNull(await optimisticExecutionScope.SnapshotRead("snapshots", "test"));
				Assert.IsNull(await optimisticExecutionScope.SnapshotRead("snapshots", "test", 0));
				await SetAndCheck("c", 0);
				await optimisticExecutionScope.IncrementSnapshotCounter("snapshots");
				Assert.AreEqual("c", await optimisticExecutionScope.SnapshotRead("snapshots", "test", 0));
				Assert.AreEqual("c", await optimisticExecutionScope.SnapshotRead("snapshots", "test", 1));
				await SetAndCheck("d", 1);
				Assert.AreEqual("c", await optimisticExecutionScope.SnapshotRead("snapshots", "test", 0));
				return false;
			});
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
		[Test]
		public async Task YuriTablesNextGenerationArrayQueue()
		{
			//The EnhancedSequentialAccessDictionary is never flushed, so it's effectively a pure memory engine
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0);
			await optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				Assert.IsTrue(await optimisticExecutionScope.TryCreateArray("array"));
				Assert.IsFalse(await optimisticExecutionScope.TryCreateArray("array"));
				ArrayHandle arrayHandle = await optimisticExecutionScope.OpenArray("array");
				BigInteger end = 4096;
				for (ushort i = 0; i < 4096; ++i){
					await arrayHandle.PushStart(i.ToString());
				}
				for (ushort i = 0; i < 4096; ++i)
				{
					Assert.AreEqual(i.ToString(), await arrayHandle.Read(4095 - i));
				}
				
				Assert.AreEqual(end, await arrayHandle.Length());
				for (ushort i = 0; i < 4096; ++i)
				{
					Assert.AreEqual(i.ToString(), (await arrayHandle.TryPopEnd()).value);
				}
				Assert.AreEqual(BigInteger.Zero, await arrayHandle.Length());
				Assert.False((await arrayHandle.TryPopEnd()).exists);
				Assert.False((await arrayHandle.TryPopStart()).exists);
				for (ushort i = 0; i < 4096; ++i)
				{
					string str = i.ToString();
					await arrayHandle.PushEnd(str);
					Assert.AreEqual(str, await arrayHandle.Read(i));
				}
				Assert.AreEqual(end, await arrayHandle.Length());
				for (ushort i = 0; i < 4096; ++i)
				{
					Assert.AreEqual(i.ToString(), (await arrayHandle.TryPopStart()).value);
				}
				Assert.AreEqual(BigInteger.Zero, await arrayHandle.Length());
				Assert.False((await arrayHandle.TryPopEnd()).exists);
				Assert.False((await arrayHandle.TryPopStart()).exists);
				return false;
			});
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
				BigInteger twenty = 20;
				await optimisticExecutionScope.BTreeTryRemove("mytree", twenty);
				await foreach (BigInteger bigInteger1 in optimisticExecutionScope.BTreeSelect("mytree", CompareOperator.LessThanOrEqual, false, 100)){
					if(bigInteger1 == 19){
						reference += 2;
					} else{
						Assert.AreEqual(reference++, bigInteger1);
					}
				}
				Assert.AreEqual("101", reference.ToString());
				return false;
			});
		}
		[Test]
		public async Task NextGenerationSortedSet()
		{
			await new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0).ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				await optimisticExecutionScope.CreateSortedSet("z");
				SortedSetHandle sortedSetHandle = await optimisticExecutionScope.OpenSortedSet("z");
				foreach (BigInteger bigInteger in Random2())
				{
					string strbigint = bigInteger.ToString();
					await sortedSetHandle.Write("meow_" + strbigint, strbigint + " LesbianDB now has an ISAM", bigInteger);
				}
				await sortedSetHandle.Write("meow_50", "asmr yuri lesbian neck kissing", 50);
				await sortedSetHandle.Write("meow_40", "40 LesbianDB now has an ISAM", 30);
				Assert.IsFalse(await sortedSetHandle.TryDelete("purrfect"));
				await sortedSetHandle.Write("purrfect", "YuriTables Next Generation is better than MySQL", 10);
				Assert.IsTrue(await sortedSetHandle.TryDelete("purrfect"));
				BigInteger reference = BigInteger.Zero;
				bool first30 = true;
				SafepointController safepointController = new SafepointController(optimisticExecutionScope);
				await foreach (SortedSetReadResult sortedSetReadResult in sortedSetHandle.Select(safepointController, CompareOperator.LessThan, 100, false))
				{
					if (reference == 40)
					{
						reference += 1;
					}
					string strbigint = reference.ToString();
					if (sortedSetReadResult.bigInteger == 50)
					{
						Assert.AreEqual("asmr yuri lesbian neck kissing", sortedSetReadResult.value);
					}
					else
					{
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
		public async Task NextGenerationTable() {
			await new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), long.MaxValue).ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) =>
			{
				//Create table
				await optimisticExecutionScope.CreateNGTable(new Dictionary<string, ColumnType>
				{
					{"name", ColumnType.UniqueString},
					{"id", ColumnType.UniqueString},
					{"height", ColumnType.SortedInt},
					{"description", ColumnType.DataString}
				}, "lesbians");
				TableHandle tableHandle = await optimisticExecutionScope.OpenNGTable("lesbians");

				//Insert rows
				await tableHandle.Insert(new Dictionary<string, string> {
					{"name", "jessielesbian" },
					{"id", "1"},
					{"description", "the cute, nice, and lesbian creator of LesbianDB v2.1"}
				}, new Dictionary<string, BigInteger> {
					{"height", 160}
				});
				await tableHandle.Insert(new Dictionary<string, string> {
					{"name", "vnch-chan" },
					{"id", "2"},
					{"description", "busy making vietnam democratic"}
				}, new Dictionary<string, BigInteger> {
					{"height", 155}
				});
				await tableHandle.Insert(new Dictionary<string, string> {
					{"name", "hillary-clinton" },
					{"id", "3"},
					{"description", "running for president"}
				}, new Dictionary<string, BigInteger> {
					{"height", 210}
				});

				//Primary key select tests
				SafepointController safepointController = new SafepointController(optimisticExecutionScope);
				Assert.IsFalse((await tableHandle.TryGetRowByPrimaryKey("name", "adolf-hitler")).found);

				TryGetRowResult testrow = await tableHandle.TryGetRowByPrimaryKey("name", "jessielesbian");
				Assert.IsTrue(testrow.found);
				Assert.AreEqual("jessielesbian", testrow.theRow["name"]);
				Assert.AreEqual("1", testrow.theRow["id"]);
				Assert.AreEqual("160", testrow.theRow["height"]);
				Assert.AreEqual("the cute, nice, and lesbian creator of LesbianDB v2.1", testrow.theRow["description"]);

				//Ordered select tests
				int count = 0;
				await foreach (IReadOnlyDictionary<string, string> keyValuePairs in tableHandle.SelectOrderedBy(safepointController, "height", CompareOperator.LessThan, 200, false))
				{
					Assert.AreNotEqual("hillary-clinton", keyValuePairs["name"]);
					++count;
				}
				Assert.AreEqual(2, count);

				//Modify primary key
				await tableHandle.UpdateRowByPrimaryKey("name", "id", "jessielesbian", "100");
				testrow = await tableHandle.TryGetRowByPrimaryKey("name", "jessielesbian");
				Assert.IsTrue(testrow.found);
				Assert.AreEqual("jessielesbian", testrow.theRow["name"]);
				Assert.AreEqual("100", testrow.theRow["id"]);
				Assert.AreEqual("160", testrow.theRow["height"]);
				Assert.AreEqual("the cute, nice, and lesbian creator of LesbianDB v2.1", testrow.theRow["description"]);

				//Modify sorted data
				BigInteger threehundred = 300;
				await tableHandle.UpdateSortedIntByPrimaryKey("name", "height", "jessielesbian", threehundred);
				await foreach (IReadOnlyDictionary<string, string> row in tableHandle.SelectOrderedBy(safepointController, "height", CompareOperator.EqualTo, threehundred, false))
				{
					Assert.NotNull(row);
					Assert.AreEqual("jessielesbian", row["name"]);
					Assert.AreEqual("100", row["id"]);
					Assert.AreEqual("300", row["height"]);
					++count;
				}
				Assert.AreEqual(3, count);
				await tableHandle.UpdateMultipleRows(safepointController, "height", threehundred, CompareOperator.EqualTo, "description", "purring lesbian catgirl");
				testrow = await tableHandle.TryGetRowByPrimaryKey("name", "jessielesbian");
				Assert.IsTrue(testrow.found);
				Assert.AreEqual("jessielesbian", testrow.theRow["name"]);
				Assert.AreEqual("100", testrow.theRow["id"]);
				Assert.AreEqual("300", testrow.theRow["height"]);
				Assert.AreEqual("purring lesbian catgirl", testrow.theRow["description"]);
				BigInteger twofifty = 250;
				await tableHandle.UpdateMultipleIntRows(safepointController, "height", threehundred, CompareOperator.EqualTo, "height", twofifty);
				testrow = await tableHandle.TryGetRowByPrimaryKey("name", "jessielesbian");
				Assert.IsTrue(testrow.found);
				Assert.AreEqual("jessielesbian", testrow.theRow["name"]);
				Assert.AreEqual("100", testrow.theRow["id"]);
				Assert.AreEqual("250", testrow.theRow["height"]);
				Assert.AreEqual("purring lesbian catgirl", testrow.theRow["description"]);
				await foreach (IReadOnlyDictionary<string, string> row in tableHandle.SelectOrderedBy(safepointController, "height", CompareOperator.EqualTo, twofifty, false))
				{
					Assert.NotNull(row);
					Assert.AreEqual("jessielesbian", row["name"]);
					Assert.AreEqual("100", row["id"]);
					Assert.AreEqual("250", row["height"]);
					++count;
				}
				Assert.AreEqual(4, count);
				await tableHandle.DeleteRowByPrimaryKey("name", "jessielesbian");
				await foreach (IReadOnlyDictionary<string, string> row in tableHandle.SelectOrderedBy(safepointController, "height", CompareOperator.GreaterThan, 200, false))
				{
					Assert.AreEqual(5, ++count);
					Assert.NotNull(row);
					Assert.AreEqual("hillary-clinton", row["name"]);
					Assert.AreEqual("3", row["id"]);
					Assert.AreEqual("210", row["height"]);
				}
				await foreach (IReadOnlyDictionary<string, string> row in tableHandle.SelectAllOrderedBy(safepointController, "height", false))
				{
					Assert.NotNull(row);
					if (++count == 6)
					{
						Assert.AreEqual("vnch-chan", row["name"]);
						Assert.AreEqual("2", row["id"]);
						Assert.AreEqual("155", row["height"]);
					}
					else
					{
						Assert.AreEqual(7, count);
						Assert.AreEqual("hillary-clinton", row["name"]);
						Assert.AreEqual("3", row["id"]);
						Assert.AreEqual("210", row["height"]);
					}
				}
				await tableHandle.DeleteAllRows(safepointController);
				await foreach (IReadOnlyDictionary<string, string> row in tableHandle.SelectAllOrderedBy(safepointController, "height", false))
				{
					Assert.Fail();
				}
				return false;
			});
		}
		[Test]
		public async Task Table(){
			await new IntelligentExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), "thectr").ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
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
		public async Task KellyanneMultiMasterReplication()
		{
			static YuriDatabaseEngine[] Create(){
				return new YuriDatabaseEngine[] { new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()) };
			}
			YuriDatabaseEngine[] redoLog = Create();

			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new Kellyanne[] { new Kellyanne(redoLog, Create()), new Kellyanne(redoLog, Create()) }, 0);

			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
			}
		}
		[Test]
		public async Task MultithreadedMultiMasterReplication()
		{
			static YuriDatabaseEngine Create()
			{
				return new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary());
			}
			YuriDatabaseEngine[] redoLog = new YuriDatabaseEngine[] { Create(), Create() };

			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new ReplicatedDatabaseEngine[] { new ReplicatedDatabaseEngine(redoLog, Create()), new ReplicatedDatabaseEngine(redoLog, Create()) }, 0);

			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
			}
		}
		[Test]
		public async Task KellyanneShardedOptimisticCounter()
		{
			YuriDatabaseEngine[] yuriDatabaseEngines = new YuriDatabaseEngine[] { new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()) };

			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new Kellyanne(yuriDatabaseEngines, yuriDatabaseEngines), 0);

			for (int i = 0; i < 4096;)
			{
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
			}
		}
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
		public async Task ExtremeOptimisticLockingTorture(){
			EnhancedSequentialAccessDictionary dict = new EnhancedSequentialAccessDictionary();
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(dict), long.MaxValue);
			TaskCompletionSource<bool> taskCompletionSource = new TaskCompletionSource<bool>();
			TaskCompletionSource<bool> taskCompletionSource2 = new TaskCompletionSource<bool>();
			int atomctr = 0;
			Task task = taskCompletionSource.Task;
			Task[] tasks = new Task[256];
			for(int i = 0; i < 256; ++i){
				tasks[i] = optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) =>
				{
					Task[] tasks1 = new Task[256];
					NestedTransactionsManager nestedTransactionsManager = new NestedTransactionsManager(optimisticExecutionScope);
					for (int x = 0; x < 256; ++x)
					{
						tasks1[x] = nestedTransactionsManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope child) =>
						{
							await IncrementOptimisticCounter(child);
							if (Interlocked.Increment(ref atomctr) == 65536)
							{
								taskCompletionSource2.SetResult(true);
							}
							await task;
							return false;
						});
					}
					await tasks1;
					return false;
				});
			}
			await taskCompletionSource2.Task;
			taskCompletionSource.SetResult(true);
			await tasks;
			Assert.AreEqual("65536", await dict.Read("counter"));
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
		public async Task PurrfectNGOptimismCounter()
		{
			string tempdir = Misc.GetRandomFileName();
			Directory.CreateDirectory(tempdir);
			try{
				await using (PurrfectNG purrfectNG = new PurrfectNG(tempdir, new EnhancedSequentialAccessDictionary())){
					OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(purrfectNG, 0);
					for (int i = 0; i < 128;)
					{
						Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
					}
				}
				await using (PurrfectNG purrfectNG = new PurrfectNG(tempdir, new EnhancedSequentialAccessDictionary()))
				{
					OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(purrfectNG, 0);
					for (int i = 128; i < 256;)
					{
						Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter));
					}
				}
			} finally{
				Directory.Delete(tempdir, true);
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