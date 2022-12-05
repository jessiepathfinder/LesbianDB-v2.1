using NUnit.Framework;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Collections.Generic;
using System;
using System.IO;
using LesbianDB.Optimism.Core;

namespace LesbianDB.Tests
{
	public class Tests
	{
		[SetUp]
		public void Setup()
		{
			
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
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(await RemoteDatabaseEngine.Connect(new Uri(url), default), 0);
			for (int i = 0; i < 4096;)
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