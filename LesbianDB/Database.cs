using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Linq;
using System.IO;
using System.IO.Compression;
using Newtonsoft.Json.Bson;
using System.Buffers.Binary;

namespace LesbianDB
{
	public interface IDatabaseEngine{
		public Task<IReadOnlyDictionary<string, string>> Execute(ReadOnlyMemory<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes);
	}

	/// <summary>
	/// A high-performance LesbianDB storage engine
	/// </summary>
	public sealed class YuriDatabaseEngine : IDatabaseEngine
	{
		/// <summary>
		/// Restores a binlog stream into the given IAsyncDictionary
		/// </summary>
		public static async Task RestoreBinlog(Stream binlog, IAsyncDictionary asyncDictionary){
			byte[] buffer = null;
			try{
				buffer = Misc.arrayPool.Rent(256);
				Dictionary<string, string> delta = new Dictionary<string, string>();
				JsonSerializer jsonSerializer = new JsonSerializer();
				while(true){
					int read = await binlog.ReadAsync(buffer, 0, 4);
					if (read != 4)
					{
						binlog.SetLength(binlog.Seek(-read, SeekOrigin.Current));
						return;
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
						return;
					}
					using (Stream str = new DeflateStream(new MemoryStream(buffer, 0, len, false, false), CompressionMode.Decompress, false)){
						BsonDataReader bsonDataReader = new BsonDataReader(str);
						GC.SuppressFinalize(bsonDataReader);
						jsonSerializer.Populate(bsonDataReader, delta);
					}
					Queue<Task> tasks = new Queue<Task>();
					foreach(KeyValuePair<string, string> kvp in delta){
						tasks.Enqueue(asyncDictionary.Write(kvp.Key, kvp.Value));
					}
					delta.Clear();
					while(tasks.TryDequeue(out Task tsk)){
						await tsk;
					}
				}
			} finally{
				if(buffer is { }){
					Misc.arrayPool.Return(buffer);
				}
			}
		}
		private void InitLocks(){
			for(int i = 0; i < 65536; ){
				asyncReaderWriterLocks[i++] = new AsyncReaderWriterLock();
			}
		}
		private readonly IAsyncDictionary asyncDictionary;
		public YuriDatabaseEngine(IAsyncDictionary asyncDictionary){
			this.asyncDictionary = asyncDictionary ?? throw new ArgumentNullException(nameof(asyncDictionary));
			InitLocks();
		}
		public YuriDatabaseEngine(IAsyncDictionary asyncDictionary, Stream binlog)
		{
			this.asyncDictionary = asyncDictionary ?? throw new ArgumentNullException(nameof(asyncDictionary));
			this.binlog = binlog ?? throw new ArgumentNullException(nameof(binlog));
			binlogLock = new AsyncMutex();
			InitLocks();
		}
		private readonly AsyncMutex binlogLock;
		private readonly Stream binlog;
		private readonly AsyncReaderWriterLock[] asyncReaderWriterLocks = new AsyncReaderWriterLock[65536];
		private void CheckReadLocks(Dictionary<ushort, bool> lockLevels, ReadOnlySpan<string> reads){
			foreach(string str in reads){
				lockLevels.TryAdd((ushort)(str.GetHashCode() & 65535), false);
			}
		}
		private void AddReads(ReadOnlySpan<string> reads, Dictionary<string, Task<string>> pendingReads){
			foreach(string read in reads){
				if(!pendingReads.ContainsKey(read)){
					pendingReads.Add(read, asyncDictionary.Read(read));
				}
			}
		}
		private async Task WriteAndFlushBinlog(byte[] buffer, int len){
			await binlog.WriteAsync(buffer, 0, len);
			await binlog.FlushAsync();
		}
		public async Task<IReadOnlyDictionary<string, string>> Execute(ReadOnlyMemory<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes)
		{
			//Lock checking
			Dictionary<ushort, bool> lockLevels = new Dictionary<ushort, bool>();
			bool write = writes.Count > 0;
			if(write){
				foreach (KeyValuePair<string, string> keyValuePair in writes)
				{
					lockLevels.Add((ushort)(keyValuePair.Key.GetHashCode() & 65535), true);
				}
			}
			CheckReadLocks(lockLevels, reads.Span);
			foreach (KeyValuePair<string, string> keyValuePair in conditions)
			{
				lockLevels.TryAdd((ushort)(keyValuePair.Key.GetHashCode() & 65535), false);
			}
			//Lock ordering
			List<ushort> locks = lockLevels.Keys.ToList();
			locks.Sort();

			//Pending reads
			Dictionary<string, Task<string>> pendingReads = new Dictionary<string, Task<string>>();
			Dictionary<string, string> readResults = new Dictionary<string, string>();

			//binlog stuff
			byte[] buffer = null;
			Task writeBinlog = null;
			bool binlocked = false;

			//Acquire locks
			foreach (ushort id in locks)
			{
				if (lockLevels[id])
				{
					await asyncReaderWriterLocks[id].AcquireWriterLock();
				}
				else
				{
					await asyncReaderWriterLocks[id].AcquireReaderLock();
				}
			}
			try
			{
				AddReads(reads.Span, pendingReads);
				foreach (KeyValuePair<string, string> kvp in conditions)
				{
					string key = kvp.Key;
					if (!pendingReads.TryGetValue(key, out Task<string> tsk))
					{
						tsk = asyncDictionary.Read(key);
					}
					write &= kvp.Value == await tsk;
				}
				foreach(KeyValuePair<string, Task<string>> kvp in pendingReads){
					readResults.Add(kvp.Key, await kvp.Value);
				}
				if(write){
					if (binlog is { })
					{
						int len;
						JsonSerializer jsonSerializer = new JsonSerializer();
						using (MemoryStream memoryStream = new MemoryStream()){
							using(Stream deflateStream = new DeflateStream(memoryStream, CompressionLevel.Optimal, true)){
								BsonDataWriter bsonDataWriter = new BsonDataWriter(deflateStream);
								jsonSerializer.Serialize(bsonDataWriter, writes);
							}
							len = (int)memoryStream.Position;
							memoryStream.Seek(0, SeekOrigin.Begin);
							buffer = Misc.arrayPool.Rent(len + 4);
							memoryStream.Read(buffer, 4, len);
						}
						BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(0, 4), len);
						binlocked = true;
						await binlogLock.Enter();
						writeBinlog = WriteAndFlushBinlog(buffer, len + 4);
					}
					Queue<Task> writeTasks = new Queue<Task>();
					foreach(KeyValuePair<string, string> keyValuePair in writes){
						writeTasks.Enqueue(asyncDictionary.Write(keyValuePair.Key, keyValuePair.Value));
					}
					foreach(Task tsk in writeTasks){
						await tsk;
					}
				}
			}
			finally
			{
				//Buffer is only allocated if we are binlogged
				if (buffer is { })
				{
					//Buffer will always be created before binlog locking
					if (binlocked)
					{
						//Binlog writing will always start after binlog locking
						if (writeBinlog is { })
						{
							await writeBinlog;
						}
						binlogLock.Exit();
					}
					Misc.arrayPool.Return(buffer, false);
				}
				foreach (ushort id in locks)
				{
					if (lockLevels[id])
					{
						asyncReaderWriterLocks[id].ReleaseWriterLock();
					}
					else
					{
						asyncReaderWriterLocks[id].ReleaseReaderLock();
					}
				}
			}
			return readResults;
		}
	}
}
