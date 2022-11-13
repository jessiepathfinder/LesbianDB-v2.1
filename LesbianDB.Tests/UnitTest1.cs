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
		public async Task YuriMalloc()
		{
			byte[] bytes = new byte[32];
			YuriMalloc yuriMalloc = new YuriMalloc();
			RandomNumberGenerator.Fill(bytes);
			Assert.AreEqual(Convert.ToBase64String(bytes, 0, 32), Convert.ToBase64String(await (await yuriMalloc.Write(bytes))(), 0, 32, Base64FormattingOptions.None));
		}
		private static string RandomDictionaryKeyValue(out string key){
			Span<byte> bytes = stackalloc byte[32];
			RandomNumberGenerator.Fill(bytes);
			key = bytes[0].ToString();
			return Convert.ToBase64String(bytes);
		}
		private static async Task TestAsyncDictionary(IAsyncDictionary asyncDictionary){
			Dictionary<string, string> reference = new Dictionary<string, string>();
			for(int i = 0; ++i < 16777217; ){
				string val = RandomDictionaryKeyValue(out string key);
				reference[key] = val;
				await asyncDictionary.Write(key, val);
			}
			foreach(KeyValuePair<string, string> keyValuePair in reference){
				Assert.AreEqual(keyValuePair.Value, await asyncDictionary.Read(keyValuePair.Key));
			}
		}
		[Test]
		public async Task CachedAsyncDictionary(){
			ISwapAllocator malloc = new YuriEncrypt(new SimpleShardedSwapAllocator<YuriMalloc>(16));
			await TestAsyncDictionary(new CachedAsyncDictionary(new ShardedAsyncDictionary(() => new SequentialAccessAsyncDictionary(malloc), 16), 0));
		}
		[Test]
		public async Task PersistentYuriDatabaseEngine(){
			using Stream binlog = new MemoryStream();
			YuriMalloc yuriMalloc = new YuriMalloc();
			YuriDatabaseEngine yuriDatabaseEngine = new YuriDatabaseEngine(new SequentialAccessAsyncDictionary(yuriMalloc), binlog);
			await yuriDatabaseEngine.Execute(new string[0], new Dictionary<string, string>(), new Dictionary<string, string> {
				{ "jessielesbian.iscute", "true" }
			});
			await yuriDatabaseEngine.Execute(new string[0], new Dictionary<string, string>(), new Dictionary<string, string> {
				{ "jessielesbian.iscute", "false" }
			});

			//NOTE: we don't need to dispose of YuriDatabaseEngines, since YuriMalloc will
			//free underlying resources in the next swap garbage collection cycle

			binlog.Seek(0, SeekOrigin.Begin);
			IAsyncDictionary tmp = new SequentialAccessAsyncDictionary(yuriMalloc);
			await YuriDatabaseEngine.RestoreBinlog(binlog, tmp);
			yuriDatabaseEngine = new YuriDatabaseEngine(tmp);
			IReadOnlyDictionary<string, string> res = await yuriDatabaseEngine.Execute(new string[] { "jessielesbian.iscute" }, new Dictionary<string, string>(), new Dictionary<string, string>());
			Assert.AreEqual(res.Count, 1);
			Assert.AreEqual("false", res["jessielesbian.iscute"]);
		}

		[Test]
		public async Task EphemeralYuriDatabaseEngine(){
			YuriDatabaseEngine yuriDatabaseEngine = new YuriDatabaseEngine(new SequentialAccessAsyncDictionary(new YuriMalloc()));
			//unconditional writing
			Assert.AreEqual(0, (await yuriDatabaseEngine.Execute(new string[0], new Dictionary<string, string>(), new Dictionary<string, string> {
				{ "jessielesbian.iscute", "true" }
			})).Count);

			//satisfied conditional write
			IReadOnlyDictionary<string, string> res = await yuriDatabaseEngine.Execute(new string[] { "jessielesbian.iscute"}, new Dictionary<string, string>{
				{ "jessielesbian.iscute", "true" }
			},
			new Dictionary<string, string>{
				{ "jessielesbian.iscute", "false" }
			});
			Assert.AreEqual(res.Count, 1);
			Assert.AreEqual("true", res["jessielesbian.iscute"]);

			//unsatisfied conditional write
			res = await yuriDatabaseEngine.Execute(new string[] { "jessielesbian.iscute" }, new Dictionary<string, string>{
				{ "jessielesbian.iscute", "true" }
			},
			new Dictionary<string, string>{
				{ "jessielesbian.iscute", "true" }
			});
			Assert.AreEqual(res.Count, 1);
			Assert.AreEqual("false", res["jessielesbian.iscute"]);

			res = await yuriDatabaseEngine.Execute(new string[] { "jessielesbian.iscute" }, new Dictionary<string, string>(), new Dictionary<string, string>());
			Assert.AreEqual(res.Count, 1);
			Assert.AreEqual("false", res["jessielesbian.iscute"]);

		}

		[Test]
		public async Task OptimismCounter(){
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(new SequentialAccessAsyncDictionary(new YuriMalloc())), 0);
			for(int i = 0; i < 4096; ){
				Assert.AreEqual(i++, await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementOptimisticCounter, false));
			}
		}
		private static async Task<int> IncrementOptimisticCounter(IOptimisticExecutionScope optimisticExecutionScope){
			int value = Convert.ToInt32(await optimisticExecutionScope.Read("counter"));
			optimisticExecutionScope.Write("counter", (value + 1).ToString());
			return value;
		}
	}
}