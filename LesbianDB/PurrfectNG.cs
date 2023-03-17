using Newtonsoft.Json;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Bson;
using System.Security.Cryptography;
using System.Threading;

namespace LesbianDB
{
	public sealed class PurrfectNG : IDatabaseEngine, IAsyncDisposable
	{
		private readonly string logdir;
		private readonly Stream[] binlogs = new Stream[65536];
		private readonly AsyncReaderWriterLock[] asyncReaderWriterLocks = new AsyncReaderWriterLock[65536];
		private readonly bool[] activationStatuses = new bool[65536];
		private readonly IAsyncDictionary asyncDictionary;
		private readonly int logdirlen;

		public PurrfectNG(string logdir, IAsyncDictionary asyncDictionary)
		{
			this.asyncDictionary = asyncDictionary ?? throw new ArgumentNullException(nameof(asyncDictionary));
			logdir = Directory.CreateDirectory(logdir ?? throw new ArgumentNullException(nameof(logdir))).FullName;
			
			StringBuilder stringBuilder = new StringBuilder(logdir);
			int len;
			if(logdir.EndsWith(Path.DirectorySeparatorChar)){
				len = logdir.Length;
				this.logdir = logdir;
			} else{
				stringBuilder.Append(Path.DirectorySeparatorChar);
				len = logdir.Length + 1;
				this.logdir = stringBuilder.ToString();
			}
			logdirlen = len;
			for (int i = 0; i < 65536; ++i)
			{
				string name = stringBuilder.Append(i).Append(".binlog").ToString();
				binlogs[i] = new FileStream(name, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.SequentialScan | FileOptions.Asynchronous);
				stringBuilder.Remove(len, name.Length - len);
				asyncReaderWriterLocks[i] = new AsyncReaderWriterLock(true);
			}
		}
		private static ushort Hash(string str){
			return (ushort)(Misc.HashString4(str + "purrfect lesbian catgirls neck kissing") & 65535);
		}
		private async Task ActivateIfNeeded(ushort hash){
			AsyncReaderWriterLock asyncReaderWriterLock = asyncReaderWriterLocks[hash];
			bool upgrading = false;
			bool upgraded = false;
			byte[] buffer = null;
			Stream stream = binlogs[hash];
			JsonSerializer jsonSerializer = new JsonSerializer();
			StringBuilder stringBuilder = new StringBuilder(logdir);
			await asyncReaderWriterLock.AcquireUpgradeableReadLock();
			try
			{
				Exception ex1 = damage;
				if (ex1 is { })
				{
					throw new ObjectDamagedException(ex1);
				}
				if (activationStatuses[hash])
				{
					return;
				}
				buffer = Misc.arrayPool.Rent(256);
				upgrading = true;
				await asyncReaderWriterLock.UpgradeToWriteLock();
				upgraded = true;
				Exception ex2 = damage;
				if (ex2 is { })
				{
					throw new ObjectDamagedException(ex2);
				}
				while (true)
				{
					int read1 = await stream.ReadAsync(buffer, 0, 36);
					if (read1 < 36)
					{
						stream.SetLength(stream.Seek(-read1, SeekOrigin.Current));
						break;
					}
					int len = BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(0, 4));
					if (len < 1)
					{
						stream.SetLength(stream.Seek(-36, SeekOrigin.Current));
						break;
					}
					string filename = stringBuilder.Append(Convert.ToBase64String(buffer, 4, 32, Base64FormattingOptions.None).Replace('/', '=')).Append(".blacklist").ToString();
					if (buffer.Length < len)
					{
						byte[] oldbuf = buffer;
						buffer = Misc.arrayPool.Rent(len);
						Misc.arrayPool.Return(oldbuf, false);
					}
					if (File.Exists(filename))
					{
						stream.SetLength(stream.Seek(-36, SeekOrigin.Current));
						break;
					}

					int read2 = await stream.ReadAsync(buffer, 0, len);
					if (read2 == len)
					{
						IReadOnlyDictionary<string, string> changes;
						try
						{
							using DeflateStream deflateStream = new DeflateStream(new MemoryStream(buffer, 0, len, false, false), CompressionMode.Decompress, false);
							BsonDataReader bsonDataReader = new BsonDataReader(deflateStream)
							{
								CloseInput = false
							};
							changes = jsonSerializer.Deserialize<Dictionary<string, string>>(bsonDataReader);
						}
						catch
						{
							goto loadfail;
						}

						int i = 0;
						Task[] writeTasks1 = new Task[changes.Count];
						foreach (KeyValuePair<string, string> keyValuePair in changes)
						{
							writeTasks1[i++] = asyncDictionary.Write(keyValuePair.Key, keyValuePair.Value);
						}
						stringBuilder.Remove(logdirlen, filename.Length - logdirlen);
						await writeTasks1;
						continue;
					}
				loadfail:
					stream.SetLength(stream.Seek(-36 - read2, SeekOrigin.Current));
					break;
				}
				activationStatuses[hash] = true;
			}
			catch (Exception e) {
				Exception old = Interlocked.CompareExchange(ref damage, e, null);
				if (old is null)
				{
					throw;
				}
				throw new ObjectDamagedException(old);
			}
			finally
			{
				if (buffer is { })
				{
					Misc.arrayPool.Return(buffer);
				}
				if (upgraded)
				{
					asyncReaderWriterLock.FullReleaseUpgradedLock();
				}
				else
				{
					if (upgrading)
					{
						throw new Exception("Unable to determine lock status (should not reach here)");
					}
					asyncReaderWriterLock.ReleaseUpgradeableReadLock();
				}
			}
			Exception ex = damage;
			if (ex is null)
			{
				return;
			}
			throw new ObjectDamagedException(ex);

		}
		private async Task InitializeBuckets(IEnumerable<ushort> keys){
			Queue<Task> activations = new Queue<Task>();
			foreach (ushort hash in keys)
			{
				AsyncReaderWriterLock asyncReaderWriterLock = asyncReaderWriterLocks[hash];
				await asyncReaderWriterLock.AcquireReaderLock();
				try
				{
					if (activationStatuses[hash])
					{
						continue;
					}
				}
				finally
				{
					asyncReaderWriterLock.ReleaseReaderLock();
				}
				activations.Enqueue(ActivateIfNeeded(hash));
			}

			if (activations.Count > 0)
			{
				await activations.ToArray();
			}
		}

		private static Dictionary<ushort, PooledMemoryStream> SerializeWrites(Dictionary<ushort, Dictionary<string, string>> writelist, string logdir, out string blacklistingFileName){
			Span<byte> bytes = stackalloc byte[36];
			Span<byte> subspan = bytes.Slice(4, 32);
			RandomNumberGenerator.Fill(subspan);
			Dictionary<ushort, PooledMemoryStream> keyValuePairs = new Dictionary<ushort, PooledMemoryStream>();
			JsonSerializer jsonSerializer = new JsonSerializer();
			foreach(KeyValuePair<ushort, Dictionary<string, string>> keyValuePair in writelist){
				PooledMemoryStream pooledMemoryStream = new PooledMemoryStream(Misc.arrayPool, 256);
				pooledMemoryStream.Write(bytes);
				using (DeflateStream deflateStream = new DeflateStream(pooledMemoryStream, CompressionLevel.Optimal, true))
				{
					BsonDataWriter bsonDataWriter = new BsonDataWriter(deflateStream)
					{
						CloseOutput = false
					};
					jsonSerializer.Serialize(bsonDataWriter, keyValuePair.Value);
				}
				BinaryPrimitives.WriteInt32LittleEndian(pooledMemoryStream.GetBuffer().AsSpan(0, 4), (int)(pooledMemoryStream.Position - 36));
				pooledMemoryStream.Seek(0, SeekOrigin.Begin);
				keyValuePairs.Add(keyValuePair.Key, pooledMemoryStream);
			}

			blacklistingFileName = keyValuePairs.Count > 1 ? new StringBuilder(logdir).Append(Convert.ToBase64String(subspan, Base64FormattingOptions.None).Replace('/', '=')).Append(".blacklist").ToString() : null;
			return keyValuePairs;
		}
		private static async Task CopyToAndFlush(Stream src, Stream dst){
			await src.CopyToAsync(dst);
			await dst.FlushAsync();
		}
		private volatile Exception damage;
		public async Task<IReadOnlyDictionary<string, string>> Execute(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes)
		{
			Dictionary<ushort, bool> locklist = new Dictionary<ushort, bool>();
			int writecount = writes.Count;
			bool write1 = writecount > 0;
			bool write = write1;
			Dictionary<ushort, Dictionary<string, string>> writelist = write ? new Dictionary<ushort, Dictionary<string, string>>() : null;
			Dictionary<string, bool> deduplicatedReads = new Dictionary<string, bool>();
			Dictionary<string, bool> returningReads = new Dictionary<string, bool>();
			foreach (string key in reads){
				if(key is null){
					return SafeEmptyReadOnlyDictionary<string, string>.instance;
				}
				deduplicatedReads.TryAdd(key, false);
				returningReads.TryAdd(key, false);
			}
			foreach (string key in conditions.Keys)
			{
				if (key is null)
				{
					return SafeEmptyReadOnlyDictionary<string, string>.instance;
				}
				deduplicatedReads.TryAdd(key, false);
			}
			foreach (KeyValuePair<string, string> keyValuePair in writes)
			{
				string key = keyValuePair.Key;
				if (key is null)
				{
					return SafeEmptyReadOnlyDictionary<string, string>.instance;
				}
				ushort hash = Hash(key);
				if(!writelist.TryGetValue(hash, out Dictionary<string, string> shardwrites)){
					shardwrites = new Dictionary<string, string>();
					writelist.Add(hash, shardwrites);
				}
				shardwrites.Add(key, keyValuePair.Value);
				locklist.TryAdd(hash, true);
			}
			foreach (string key in deduplicatedReads.Keys){
				locklist.TryAdd(Hash(key), false);
			}
			Task init = InitializeBuckets(locklist.Keys);
			List<ushort> sortedlocklist = new List<ushort>(locklist.Count);
			sortedlocklist.AddRange(locklist.Keys);
			sortedlocklist.Sort();
			Dictionary<string, string> returns = new Dictionary<string, string>();
			Dictionary<string, Task<string>> asyncReads = new Dictionary<string, Task<string>>();
			Dictionary<ushort, PooledMemoryStream> serializedBinlogWrites;
			string blacklistingFileName;
			if(write){
				serializedBinlogWrites = SerializeWrites(writelist, logdir, out blacklistingFileName);
				if(blacklistingFileName is { }){
					await new FileStream(blacklistingFileName, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, true).DisposeAsync();
				}
				
			} else{
				serializedBinlogWrites = null;
				blacklistingFileName = null;
			}
			await init;
			bool upgrading = false;
			bool upgraded = false;
			Queue<AsyncReaderWriterLock> lockUpgradingQueue;
			if(write){
				lockUpgradingQueue = new Queue<AsyncReaderWriterLock>();
				foreach (ushort lockid in sortedlocklist)
				{
					AsyncReaderWriterLock asyncReaderWriterLock = asyncReaderWriterLocks[lockid];
					if (locklist[lockid])
					{
						Task locktask = asyncReaderWriterLock.AcquireUpgradeableReadLock();
						lockUpgradingQueue.Enqueue(asyncReaderWriterLock);
						await locktask;
					}
					else
					{
						await asyncReaderWriterLock.AcquireReaderLock();
					}
				}
			} else{
				lockUpgradingQueue = null;
				foreach (ushort lockid in sortedlocklist)
				{
					await asyncReaderWriterLocks[lockid].AcquireReaderLock();
				}
			}
			try{
				Exception ex1 = damage;
				if (ex1 is { })
				{
					throw new ObjectDamagedException(ex1);
				}
				foreach (string key in deduplicatedReads.Keys){
					asyncReads.Add(key, asyncDictionary.Read(key));
				}
				foreach(KeyValuePair<string, Task<string>> keyValuePair in asyncReads){
					string key = keyValuePair.Key;
					string value = await keyValuePair.Value;
					if(write){
						if(conditions.TryGetValue(key, out string expected)){
							write = value == expected;
						}
					}
					if(returningReads.ContainsKey(key)){
						returns.Add(key, value);
					}
				}
				if(write){
					upgrading = true;
					while(lockUpgradingQueue.TryDequeue(out AsyncReaderWriterLock asyncReaderWriterLock)){
						await asyncReaderWriterLock.UpgradeToWriteLock();
					}
					upgraded = true;
					Exception ex2 = damage;
					if (ex2 is { })
					{
						throw new ObjectDamagedException(ex2);
					}
					if (blacklistingFileName is null){
						Task[] tasks = new Task[serializedBinlogWrites.Count + writecount];
						int i = 0;
						foreach (KeyValuePair<ushort, PooledMemoryStream> keyValuePair in serializedBinlogWrites)
						{
							tasks[i++] = CopyToAndFlush(keyValuePair.Value, binlogs[keyValuePair.Key]);
						}
						foreach (KeyValuePair<string, string> keyValuePair1 in writes)
						{
							tasks[i++] = asyncDictionary.Write(keyValuePair1.Key, keyValuePair1.Value);
						}
						await tasks;
					} else{
						Task[] tasks1 = new Task[serializedBinlogWrites.Count];
						int i = 0;
						foreach (KeyValuePair<ushort, PooledMemoryStream> keyValuePair in serializedBinlogWrites)
						{
							tasks1[i++] = CopyToAndFlush(keyValuePair.Value, binlogs[keyValuePair.Key]);
						}
						await tasks1;
						int j = 0;
						Task[] tasks2 = new Task[writecount];
						foreach (KeyValuePair<string, string> keyValuePair1 in writes)
						{
							tasks2[j++] = asyncDictionary.Write(keyValuePair1.Key, keyValuePair1.Value);
						}
						File.Delete(blacklistingFileName);
						await tasks2;
					}
				}
			}
			catch(Exception e){
				Exception old = Interlocked.CompareExchange(ref damage, e, null);
				if(old is null){
					throw;
				}
				throw new ObjectDamagedException(old);
			}
			finally{
				if(write1)
				{
					if (upgraded)
					{
						foreach (KeyValuePair<ushort, bool> keyValuePair in locklist)
						{
							AsyncReaderWriterLock asyncReaderWriterLock = asyncReaderWriterLocks[keyValuePair.Key];
							if (keyValuePair.Value)
							{
								asyncReaderWriterLock.FullReleaseUpgradedLock();
							}
							else
							{
								asyncReaderWriterLock.ReleaseReaderLock();
							}
						}
					}
					else
					{
						if (upgrading)
						{
							throw new Exception("Not all locks that should be upgraded got upgraded (should not reach here)");
						}
						foreach (KeyValuePair<ushort, bool> keyValuePair in locklist)
						{
							AsyncReaderWriterLock asyncReaderWriterLock = asyncReaderWriterLocks[keyValuePair.Key];
							if (keyValuePair.Value)
							{
								asyncReaderWriterLock.ReleaseUpgradeableReadLock();
							}
							else
							{
								asyncReaderWriterLock.ReleaseReaderLock();
							}
						}
					}
					foreach (PooledMemoryStream pooledMemoryStream in serializedBinlogWrites.Values){
						pooledMemoryStream.Dispose();
					}
				} else{
					foreach(ushort lockid in sortedlocklist){
						asyncReaderWriterLocks[lockid].ReleaseReaderLock();
					}
				}
			}
			Exception ex = damage;
			if(ex is null){
				return returns;
			}
			throw new ObjectDamagedException(ex);
		}

		public async ValueTask DisposeAsync()
		{
			Task[] tasks = new Task[65536];
			for(int i = 0; i < 65536; ++i){
				tasks[i] = binlogs[i].DisposeAsync().AsTask();
			}
			await tasks;
		}
	}
}
