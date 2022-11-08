using NUnit.Framework;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Collections.Generic;
using System;
using System.IO;

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
		[Test]
		public async Task SequentialAccessAsyncDictionary(){
			SequentialAccessAsyncDictionary temp = new SequentialAccessAsyncDictionary(new YuriMalloc());

			await temp.Write("jessielesbian.iscute", "true");
			await temp.Write("jessielesbian.balance.bitcoin", "1000");

			//cause I short shitcoins
			await temp.Write("jessielesbian.balance.mintme", "-1000000");
			await temp.Write("jessielesbian.balance.bitcoin", "0");
			Assert.AreEqual("true", await temp.Read("jessielesbian.iscute"));
			Assert.AreEqual("0", await temp.Read("jessielesbian.balance.bitcoin"));
			Assert.AreEqual("-1000000", await temp.Read("jessielesbian.balance.mintme"));
		}
		[Test]
		public async Task PersistentYuriDatabaseEngine(){
			using Stream binlog = new MemoryStream();
			YuriMalloc yuriMalloc = new YuriMalloc();
			YuriDatabaseEngine yuriDatabaseEngine = new YuriDatabaseEngine(new SequentialAccessAsyncDictionary(yuriMalloc), binlog);
			await yuriDatabaseEngine.Execute(new string[0], new Dictionary<string, string>(), new Dictionary<string, string> {
				{ "jessielesbian.iscute", "true" }
			});

			//NOTE: we don't need to dispose of YuriDatabaseEngines, since YuriMalloc will
			//free underlying resources in the next swap garbage collection cycle

			binlog.Seek(0, SeekOrigin.Begin);
			yuriDatabaseEngine = new YuriDatabaseEngine(new SequentialAccessAsyncDictionary(yuriMalloc), binlog);
			await yuriDatabaseEngine.Execute(new string[0], new Dictionary<string, string>(), new Dictionary<string, string> {
				{ "jessielesbian.iscute", "true" }
			});
			IReadOnlyDictionary<string, string> res = await yuriDatabaseEngine.Execute(new string[] { "jessielesbian.iscute" }, new Dictionary<string, string>(), new Dictionary<string, string>());
			Assert.AreEqual(res.Count, 1);
			Assert.AreEqual("true", res["jessielesbian.iscute"]);
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
	}
}