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
using System.Net.Http;
using System.Net;
using System.Net.WebSockets;
using System.Threading;
using System.Security.Cryptography;

namespace LesbianDB
{
	public interface IDatabaseEngine{
		public Task<IReadOnlyDictionary<string, string>> Execute(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes);
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
						byte[] buffer2 = buffer;
						buffer = null;
						Misc.arrayPool.Return(buffer2, false);
						buffer = Misc.arrayPool.Rent(len);
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

		public static YuriDatabaseEngine CreateSelfFlushing(IFlushableAsyncDictionary asyncDictionary, int flushingInterval){
			YuriDatabaseEngine yuriDatabaseEngine = new YuriDatabaseEngine(asyncDictionary);
			SelfFlushingLoop(new WeakReference<YuriDatabaseEngine>(yuriDatabaseEngine, false), flushingInterval);
			return yuriDatabaseEngine;
		}
		public static YuriDatabaseEngine CreateSelfFlushing(IFlushableAsyncDictionary asyncDictionary, Stream binlog, int flushingInterval)
		{
			YuriDatabaseEngine yuriDatabaseEngine = new YuriDatabaseEngine(asyncDictionary, binlog);
			SelfFlushingLoop(new WeakReference<YuriDatabaseEngine>(yuriDatabaseEngine, false), flushingInterval);
			return yuriDatabaseEngine;
		}
		private static async void SelfFlushingLoop(WeakReference<YuriDatabaseEngine> weakReference, int flushingInterval){
		start:
			await Task.Delay(flushingInterval);
			if(weakReference.TryGetTarget(out YuriDatabaseEngine yuriDatabaseEngine)){
				await yuriDatabaseEngine.flushLock.AcquireWriterLock();
				try{
					await ((IFlushableAsyncDictionary)yuriDatabaseEngine.asyncDictionary).Flush();
				} finally{
					yuriDatabaseEngine.flushLock.ReleaseWriterLock();
				}
				goto start;
			}
		}
		private readonly AsyncReaderWriterLock flushLock = new AsyncReaderWriterLock();
		private readonly AsyncMutex binlogLock;
		private readonly Stream binlog;
		private readonly AsyncReaderWriterLock[] asyncReaderWriterLocks = new AsyncReaderWriterLock[65536];
		private async Task WriteAndFlushBinlog(byte[] buffer, int len){
			await binlog.WriteAsync(buffer, 0, len);
			await binlog.FlushAsync();
		}
		public async Task<IReadOnlyDictionary<string, string>> Execute(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes)
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
			foreach (string str in reads)
			{
				lockLevels.TryAdd((ushort)(str.GetHashCode() & 65535), false);
			}
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
				foreach (string read in conditions.Keys){
					pendingReads.Add(read, asyncDictionary.Read(read));
				}
				foreach (string read in reads)
				{
					if (!pendingReads.ContainsKey(read))
					{
						pendingReads.Add(read, asyncDictionary.Read(read));
					}
				}
				foreach (string read in reads)
				{
					readResults.TryAdd(read, await pendingReads[read]);
				}
				if(!write){
					return readResults;
				}
				foreach (KeyValuePair<string, string> kvp in conditions)
				{
					if(kvp.Value != await pendingReads[kvp.Key]){
						return readResults;
					}
				}
				writes = Misc.ScrubNoEffectWrites(writes, pendingReads);
				if(writes.Count == 0){
					return readResults;
				}
				if (binlog is { })
				{
					int len;
					JsonSerializer jsonSerializer = new JsonSerializer();
					byte[] buffer;
					using (PooledMemoryStream memoryStream = new PooledMemoryStream(Misc.arrayPool))
					{
						memoryStream.SetLength(4);
						memoryStream.Seek(4, SeekOrigin.Begin);
						using (Stream deflateStream = new DeflateStream(memoryStream, CompressionLevel.Optimal, true))
						{
							BsonDataWriter bsonDataWriter = new BsonDataWriter(deflateStream);
							jsonSerializer.Serialize(bsonDataWriter, writes);
						}
						len = (int)memoryStream.Position;
						buffer = memoryStream.GetBuffer();
					}
					BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(0, 4), len);
					binlocked = true;
					await binlogLock.Enter();
					writeBinlog = WriteAndFlushBinlog(buffer, len + 4);
				}
				await flushLock.AcquireReaderLock();
				try{
					
					Queue<Task> writeTasks = new Queue<Task>();
					foreach (KeyValuePair<string, string> keyValuePair in writes)
					{
						writeTasks.Enqueue(asyncDictionary.Write(keyValuePair.Key, keyValuePair.Value));
					}
					foreach (Task tsk in writeTasks)
					{
						await tsk;
					}
				} finally{
					flushLock.ReleaseReaderLock();
				}
			}
			finally
			{
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
						binlogLock.Exit();
					}
				}
				foreach (KeyValuePair<ushort, bool> keyValuePair in lockLevels)
				{
					if (keyValuePair.Value)
					{
						asyncReaderWriterLocks[keyValuePair.Key].ReleaseWriterLock();
					}
					else
					{
						asyncReaderWriterLocks[keyValuePair.Key].ReleaseReaderLock();
					}
				}
			}
			return readResults;
		}
	}

	public sealed class RemoteDatabaseEngine : IDatabaseEngine, IDisposable{
		[JsonObject(MemberSerialization.Fields)]
		private sealed class Packet{
			public readonly string id;
			public readonly IEnumerable<string> reads;
			public readonly IReadOnlyDictionary<string, string> conditions;
			public readonly IReadOnlyDictionary<string, string> writes;

			public Packet(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes, string id)
			{
				this.reads = reads;
				this.conditions = conditions;
				this.writes = writes;
				this.id = id;
			}
		}
		[JsonObject(MemberSerialization.Fields)]

		private sealed class Reply{
			public string id;
			public IReadOnlyDictionary<string, string> result;
		}
		private readonly struct PooledMessage{
			private readonly byte[] bytes;
			private readonly int len;

			public PooledMessage(byte[] bytes, int len)
			{
				this.bytes = bytes;
				this.len = len;
			}
			public ReadOnlyMemory<byte> ReadOnlyMemory => bytes.AsMemory(0, len);
			public void Return(){
				Misc.arrayPool.Return(bytes);
			}
		}
		private readonly ConcurrentQueue<PooledMessage> sendQueue = new ConcurrentQueue<PooledMessage>();
		private readonly AsyncManagedSemaphore asyncManagedSemaphore = new AsyncManagedSemaphore(0);
		private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
		private readonly ConcurrentDictionary<string, TaskCompletionSource<IReadOnlyDictionary<string, string>>> completionSources = new ConcurrentDictionary<string, TaskCompletionSource<IReadOnlyDictionary<string, string>>>();
		public RemoteDatabaseEngine(Uri uri){
			EventLoop(uri, cancellationTokenSource, sendQueue, asyncManagedSemaphore, completionSources);
		}

		private static async void EventLoop(Uri uri, CancellationTokenSource cancellationTokenSource, ConcurrentQueue<PooledMessage> sendQueue, AsyncManagedSemaphore semaphore, ConcurrentDictionary<string, TaskCompletionSource<IReadOnlyDictionary<string, string>>> completionSources)
		{
			Task[] tasks = new Task[2];
			CancellationToken cancellationToken = cancellationTokenSource.Token;
		start:
			bool closefirst = false;
			using(ClientWebSocket webSocket = new ClientWebSocket()){
				try
				{
					webSocket.Options.AddSubProtocol("LesbianDB-v2.1");
					await webSocket.ConnectAsync(uri, cancellationToken);
					closefirst = true;
					tasks[0] = SendEventLoop(webSocket, semaphore, sendQueue, cancellationToken);
					tasks[1] = ReceiveEventLoop(webSocket, completionSources, cancellationToken);
					await await Task.WhenAny(tasks);
				}
				catch (Exception e)
				{
					if (e is TaskCanceledException)
					{
						return;
					}
					foreach (KeyValuePair<string, TaskCompletionSource<IReadOnlyDictionary<string, string>>> keyValuePair in completionSources.ToArray())
					{
						if(completionSources.TryRemove(keyValuePair.Key, out TaskCompletionSource<IReadOnlyDictionary<string, string>> taskCompletionSource)){
							taskCompletionSource.SetException(e);
						}
					}
					goto start;
				} finally{
					//Try to be nice to server
					if(closefirst){
						try{
							await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Lesbians are very ASMR!", default);
							try
							{
								await Task.WhenAll(tasks);
							}
							catch
							{

							}
						} catch{
							
						}
					}
				}
			}
		}

		private static async Task SendEventLoop(WebSocket webSocket, AsyncManagedSemaphore semaphore, ConcurrentQueue<PooledMessage> sendQueue, CancellationToken cancellationToken){
			while(true){
				await semaphore.Enter();
				if(sendQueue.TryDequeue(out PooledMessage pooledMessage)){
					try{
						await webSocket.SendAsync(pooledMessage.ReadOnlyMemory, WebSocketMessageType.Text, true, cancellationToken);
					} finally{
						pooledMessage.Return();
					}
				} else{
					return;
				}
			}
		}
		private static async Task ReceiveEventLoop(WebSocket webSocket, ConcurrentDictionary<string, TaskCompletionSource<IReadOnlyDictionary<string, string>>> completionSources, CancellationToken cancellationToken)
		{
			byte[] buffer = null;
			try{
				buffer = Misc.arrayPool.Rent(65536);
				while (true)
				{
					using PooledMemoryStream pooledMemoryStream = new PooledMemoryStream(Misc.arrayPool);
					ReadOnlyMemory<byte> readOnlyMemory;
					while (true)
					{
						WebSocketReceiveResult webSocketReceiveResult = await webSocket.ReceiveAsync(buffer, cancellationToken);
						bool ended = webSocketReceiveResult.EndOfMessage;
						if (ended)
						{
							if (pooledMemoryStream.Position == 0)
							{
								readOnlyMemory = buffer.AsMemory(0, webSocketReceiveResult.Count);
								break;
							}
						}
						pooledMemoryStream.Write(buffer.AsSpan(0, webSocketReceiveResult.Count));
						if (ended)
						{
							readOnlyMemory = pooledMemoryStream.GetBuffer().AsMemory(0, (int)pooledMemoryStream.Position);
							break;
						}
					}
					ThreadPool.QueueUserWorkItem((string str) =>
					{
						Reply reply = JsonConvert.DeserializeObject<Reply>(str);
						if (completionSources.TryGetValue(reply.id, out TaskCompletionSource<IReadOnlyDictionary<string, string>> taskCompletionSource))
						{
							taskCompletionSource.SetResult(reply.result);
						}
					}, Encoding.UTF8.GetString(readOnlyMemory.Span), true);
				}
			} finally{
				if(buffer is { }){
					Misc.arrayPool.Return(buffer, false);
				}
			}
		}
		private volatile int disposed = 0;
		public void Dispose(){
			if(Interlocked.Exchange(ref disposed, 1) == 0){
				GC.SuppressFinalize(this);
				asyncManagedSemaphore.Exit();
				cancellationTokenSource.Cancel();
			}
		}

		public Task<IReadOnlyDictionary<string, string>> Execute(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes)
		{
			string id;
			{
				Span<byte> idbytes = stackalloc byte[32];
				RandomNumberGenerator.Fill(idbytes);
				id = Convert.ToBase64String(idbytes, Base64FormattingOptions.None);
			}
			string str = JsonConvert.SerializeObject(new Packet(reads, conditions, writes, id));
			int len = Encoding.UTF8.GetByteCount(str);
			byte[] bytes = Misc.arrayPool.Rent(len);
			try{
				Encoding.UTF8.GetBytes(str, 0, str.Length, bytes, 0);
			} catch{
				Misc.arrayPool.Return(bytes, false);
				throw;
			}
			TaskCompletionSource<IReadOnlyDictionary<string, string>> taskCompletionSource = new TaskCompletionSource<IReadOnlyDictionary<string, string>>();
			try{
				completionSources.TryAdd(id, taskCompletionSource);
				sendQueue.Enqueue(new PooledMessage(bytes, len));
				asyncManagedSemaphore.Exit();
			} catch(Exception e){
				completionSources.TryRemove(id, out _);
				throw;
			}
			return taskCompletionSource.Task;
		}

		~RemoteDatabaseEngine(){
			if (Interlocked.Exchange(ref disposed, 1) == 0)
			{
				asyncManagedSemaphore.Exit();
				cancellationTokenSource.Cancel();
			}
		}

	}
}
