using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LevelDB;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace LesbianDB
{
	/// <summary>
	/// A LevelDB storage engine with added binlog persistence + optimistic locking
	/// </summary>
	public sealed class LevelDBEngine : IDatabaseEngine, IDisposable
	{
		private readonly DB database;
		private static readonly Options openOptions = new Options { CreateIfMissing = true, CompressionLevel = LevelDB.CompressionLevel.SnappyCompression, WriteBufferSize = 268435456};
		private static readonly ReadOptions readOptions = new ReadOptions { FillCache = true, VerifyCheckSums = true};
		private static readonly WriteOptions syncWriteOptions = new WriteOptions { Sync = true };
		private static readonly WriteOptions asyncWriteOptions = new WriteOptions { Sync = false };

		private readonly AsyncReaderWriterLock[] lockers = new AsyncReaderWriterLock[65536];
		private readonly Stream binlog;
		private readonly AsyncMutex binlogLocker;
		public LevelDBEngine(string filename)
		{
			database = new DB(openOptions, filename);
			for (int i = 0; i < 65536;)
			{
				lockers[i++] = new AsyncReaderWriterLock();
			}
		}
		public LevelDBEngine(string filename, long cache)
		{
			using (Options options = new Options { CreateIfMissing = true, CompressionLevel = LevelDB.CompressionLevel.SnappyCompression, WriteBufferSize = cache })
			{
				database = new DB(options, filename);
			}
			for (int i = 0; i < 65536;)
			{
				lockers[i++] = new AsyncReaderWriterLock();
			}
		}
		private LevelDBEngine(DB database, Stream binlog)
		{
			this.binlog = binlog ?? throw new ArgumentNullException(nameof(binlog));
			this.database = database;
			for (int i = 0; i < 65536;)
			{
				lockers[i++] = new AsyncReaderWriterLock();
			}
			binlogLocker = new AsyncMutex();
		}
		public static async Task<LevelDBEngine> RestoreBinlog(Stream binlog, string filename){
		start:
			DB database = new DB(openOptions, filename);
			if(await RestoreBinlogImpl(binlog, database)){
				database.Dispose();
				Directory.Delete(filename);
				goto start;
			}
			return new LevelDBEngine(database, binlog);
		}
		private sealed class EmptyUnderlying : Exception{
			
		}
		public static async Task<LevelDBEngine> RestoreBinlog(Stream binlog, string filename, long cache)
		{
			DB database;
			using (Options options = new Options { CreateIfMissing = true, CompressionLevel = LevelDB.CompressionLevel.SnappyCompression, WriteBufferSize = cache }){
			start:
				database = new DB(options, filename);
				if (await RestoreBinlogImpl(binlog, database))
				{
					database.Dispose();
					Directory.Delete(filename);
					goto start;
				}
			}

			return new LevelDBEngine(database, binlog);
		}
		private static async Task<bool> RestoreBinlogImpl(Stream binlog, DB database){
			string fastrecover_height;
			try{
				await UltraHeavyThreadPoolAwaitable.instance;
				fastrecover_height = database.Get("LesbianDB_reserved_binlog_height");
			} finally{
				await Task.Yield();
			}
			long fastrecoverbegin = Convert.ToInt64(fastrecover_height);

			if(fastrecoverbegin > binlog.Length){
				//Binlog truncated!
				return true;
			}
			binlog.Seek(fastrecoverbegin, SeekOrigin.Begin);
			byte[] buffer = null;
			try
			{
				buffer = Misc.arrayPool.Rent(256);
				Dictionary<string, string> delta = new Dictionary<string, string>();
				JsonSerializer jsonSerializer = new JsonSerializer();
				while (true)
				{
					int read = await binlog.ReadAsync(buffer, 0, 4);
					if (read != 4)
					{
						if(read > 0){
							binlog.SetLength(binlog.Seek(-read, SeekOrigin.Current));
						}
						return false;
					}
					int len = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(0, 4));
					if (buffer.Length < len)
					{
						try
						{

						}
						finally
						{
							Misc.arrayPool.Return(buffer);
							buffer = Misc.arrayPool.Rent(len);
						}
					}
					read = await binlog.ReadAsync(buffer, 0, len);
					if (read != len)
					{
						binlog.SetLength(binlog.Seek(-4 - read, SeekOrigin.Current));
						return false;
					}
					using (Stream str = new DeflateStream(new MemoryStream(buffer, 0, len, false, false), CompressionMode.Decompress, false))
					{
						BsonDataReader bsonDataReader = new BsonDataReader(str);
						GC.SuppressFinalize(bsonDataReader);
						jsonSerializer.Populate(bsonDataReader, delta);
					}
					using (WriteBatch writeBatch = new WriteBatch(Encoding.UTF8))
					{
						foreach (KeyValuePair<string, string> kvp in delta)
						{
							string val = kvp.Value;
							if (val is null)
							{
								writeBatch.Delete(kvp.Key);
							}
							else{
								writeBatch.Put(kvp.Key, val);
							}
						}
						writeBatch.Put("LesbianDB_reserved_binlog_height", binlog.Position.ToString());
						try{
							await UltraHeavyThreadPoolAwaitable.instance;
							database.Write(writeBatch, asyncWriteOptions);
						} finally{
							await Task.Yield();
						}
					}
					
					delta.Clear();
				}
			}
			finally
			{
				if (buffer is { })
				{
					Misc.arrayPool.Return(buffer);
				}
			}
		}

		private async Task WriteAndFlushBinlog(byte[] buffer, int len)
		{
			await binlog.WriteAsync(buffer, 0, len);
			await binlog.FlushAsync();
		}
		private async Task<string> ReadUnderlying(string key){
			await UltraHeavyThreadPoolAwaitable.instance;
			return database.Get(key, readOptions);
		}
		public async Task<IReadOnlyDictionary<string, string>> Execute(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes)
		{

			//Acquire locks
			Dictionary<string, bool> allReads = new Dictionary<string, bool>();
			foreach (string read in conditions.Keys)
			{
				allReads.Add(read, false);
			}
			foreach (string read in reads){
				allReads.TryAdd(read, false);
			}
			Dictionary<ushort, bool> lockLevels = new Dictionary<ushort, bool>();
			bool dowrite = writes.Count > 0;
			if(dowrite){
				//No writing occours if this reserved key is set
				if(writes.ContainsKey("LesbianDB_reserved_binlog_height")){
					dowrite = false;
				} else{
					foreach (string write in writes.Keys)
					{
						lockLevels.TryAdd((ushort)(write.GetHashCode() & 65535), true);
					}
				}
			}
			foreach (string read in allReads.Keys)
			{
				lockLevels.TryAdd((ushort)(read.GetHashCode() & 65535), false);
			}
			List<ushort> locks = lockLevels.Keys.ToList();
			locks.Sort();
			Dictionary<string, string> returns = new Dictionary<string, string>();
			Dictionary<string, Task<string>> keyValuePairs = new Dictionary<string, Task<string>>();
			bool binlocked = false;
			Task writeBinlog = null;
			foreach (ushort id in locks)
			{
				if (lockLevels[id])
				{
					await lockers[id].AcquireWriterLock();
				}
				else
				{
					await lockers[id].AcquireReaderLock();
				}
			}
			try{
				foreach (string read in allReads.Keys)
				{
					keyValuePairs.Add(read, ReadUnderlying(read));
				}
				foreach(string read in reads){
					returns.Add(read, await keyValuePairs[read]);
				}
				if(!dowrite){
					return returns;
				}
				foreach (KeyValuePair<string, string> keyValuePair in conditions)
				{
					if (keyValuePair.Value != await keyValuePairs[keyValuePair.Key]) {
						return returns;
					}
				}
				writes = Misc.ScrubNoEffectWrites(writes, keyValuePairs);
				WriteOptions writeOptions;
				long binpos;
				if (binlog is { })
				{
					int len;
					byte[] buffer;
					JsonSerializer jsonSerializer = new JsonSerializer();
					using (PooledMemoryStream memoryStream = new PooledMemoryStream(Misc.arrayPool))
					{
						memoryStream.SetLength(4);
						memoryStream.Seek(0, SeekOrigin.End);
						using (Stream deflateStream = new DeflateStream(memoryStream, System.IO.Compression.CompressionLevel.Optimal, true))
						{
							BsonDataWriter bsonDataWriter = new BsonDataWriter(deflateStream);
							jsonSerializer.Serialize(bsonDataWriter, writes);
						}
						len = (int)memoryStream.Position;
						memoryStream.Seek(0, SeekOrigin.Begin);
						buffer = memoryStream.GetBuffer();
					}
					BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(0, 4), len);
					binlocked = true;
					await binlogLocker.Enter();
					binpos = binlog.Position;
					writeBinlog = WriteAndFlushBinlog(buffer, len + 4);
					writeOptions = asyncWriteOptions;
				} else{
					writeOptions = syncWriteOptions;
					binpos = 0;
				}
				using WriteBatch writeBatch = new WriteBatch(Encoding.UTF8);
				foreach(KeyValuePair<string, string> kvp in writes){
					string value = kvp.Value;
					if(value is null){
						writeBatch.Delete(kvp.Key);
					} else{
						writeBatch.Put(kvp.Key, value);
					}
				}
				if(writeBinlog is { }){
					writeBatch.Put("LesbianDB_reserved_binlog_height", binpos.ToString());
				}
				try{
					await UltraHeavyThreadPoolAwaitable.instance;
					database.Write(writeBatch, writeOptions);
				} finally{
					await Task.Yield();
				}
				



			} finally{
				if (binlocked)
				{
					try
					{
						//Binlog writing will always start after binlog locking
						if (writeBinlog is { })
						{
							await writeBinlog;
						}
					}
					finally
					{
						binlogLocker.Exit();
					}
				}
				foreach (KeyValuePair<ushort, bool> keyValuePair in lockLevels)
				{
					if (keyValuePair.Value)
					{
						lockers[keyValuePair.Key].ReleaseWriterLock();
					}
					else
					{
						lockers[keyValuePair.Key].ReleaseReaderLock();
					}
				}
			}
			
			return returns;
		}
		private volatile int disposed;
		~LevelDBEngine(){
			if (Interlocked.Exchange(ref disposed, 1) == 0)
			{
				database.Dispose();
			}
		}
		public void Dispose()
		{
			if(Interlocked.Exchange(ref disposed, 1) == 0){
				GC.SuppressFinalize(this);
				database.Dispose();
			}
		}
	}
}
