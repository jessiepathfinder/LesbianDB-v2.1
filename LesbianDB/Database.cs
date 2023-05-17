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
					Task[] tasks = new Task[delta.Count];
					int ctr = 0;
					foreach(KeyValuePair<string, string> kvp in delta){
						tasks[ctr++] = asyncDictionary.Write(kvp.Key, kvp.Value);
					}
					delta.Clear();
					await tasks;
				}
			} finally{
				if(buffer is { }){
					Misc.arrayPool.Return(buffer);
				}
			}
		}

		private readonly IAsyncDictionary asyncDictionary;
		private readonly bool writefastrecover;
		public YuriDatabaseEngine(IAsyncDictionary asyncDictionary){
			this.asyncDictionary = asyncDictionary ?? throw new ArgumentNullException(nameof(asyncDictionary));
		}
		public YuriDatabaseEngine(IAsyncDictionary asyncDictionary, Stream binlog)
		{
			this.asyncDictionary = asyncDictionary ?? throw new ArgumentNullException(nameof(asyncDictionary));
			this.binlog = binlog ?? throw new ArgumentNullException(nameof(binlog));
			binlogLock = new AsyncMutex();
			streamFlushingManager = new StreamFlushingManager(binlog, binlogLock);
		}
		public YuriDatabaseEngine(IAsyncDictionary asyncDictionary, Stream binlog, bool writefastrecover)
		{
			this.asyncDictionary = asyncDictionary ?? throw new ArgumentNullException(nameof(asyncDictionary));
			this.binlog = binlog ?? throw new ArgumentNullException(nameof(binlog));
			this.writefastrecover = writefastrecover;
			binlogLock = new AsyncMutex();
			streamFlushingManager = new StreamFlushingManager(binlog, binlogLock);
		}
		public YuriDatabaseEngine(IFlushableAsyncDictionary asyncDictionary, bool doflush) : this(asyncDictionary)
		{
			if(doflush){
				SelfFlushingLoop(new WeakReference<YuriDatabaseEngine>(this, false));
			}
		}
		public YuriDatabaseEngine(IFlushableAsyncDictionary asyncDictionary, Stream binlog, bool doflush) : this(asyncDictionary, binlog)
		{
			if (doflush)
			{
				SelfFlushingLoop(new WeakReference<YuriDatabaseEngine>(this, false));
			}
		}
		public YuriDatabaseEngine(IFlushableAsyncDictionary asyncDictionary, Stream binlog, bool writefastrecover, bool doflush) : this(asyncDictionary, binlog, writefastrecover)
		{
			if (doflush)
			{
				SelfFlushingLoop(new WeakReference<YuriDatabaseEngine>(this, false));
			}
		}
		private static async void SelfFlushingLoop(WeakReference<YuriDatabaseEngine> weakReference){
		start:
			await Misc.WaitForNextGC();
			if (weakReference.TryGetTarget(out YuriDatabaseEngine yuriDatabaseEngine)){
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
		private readonly VeryASMRLockingManager veryASMRLockingManager = new VeryASMRLockingManager();
		private readonly ConcurrentQueue<Exception> damages = new ConcurrentQueue<Exception>();
		private readonly AsyncReaderWriterLock damageCheckingLock = new AsyncReaderWriterLock();
		private readonly StreamFlushingManager streamFlushingManager;
		private Exception binlogException;
		private readonly LockOrderingComparer lockOrderingComparer = new LockOrderingComparer();
		private async Task WriteAndFlushBinlog(byte[] buffer, int len){
			await binlog.WriteAsync(buffer, 0, len);
			await binlog.FlushAsync();
		}
		public async Task<IReadOnlyDictionary<string, string>> Execute(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes)
		{
			byte minDamagesHeight = 0;
			byte[] binlog_buffer;
			int binlog_writelen;
			//Lock checking
			Dictionary<string, LockMode> lockLevels = new Dictionary<string, LockMode>();
			bool write = writes.Count > 0;
			bool conditional;
			if(write){
				if(writefastrecover){
					if(writes.ContainsKey("LesbianDB_reserved_binlog_height")){
						write = false;
						binlog_buffer = null;
						binlog_writelen = 0;
						conditional = false;
						goto nowrite;
					}
					lockLevels.Add("LesbianDB_reserved_binlog_height", LockMode.Upgradeable);
				}
				foreach (string key in writes.Keys)
				{
					lockLevels.Add(key, LockMode.Upgradeable);
				}
				if (binlog is null)
				{
					binlog_buffer = null;
					binlog_writelen = 0;
				}
				else
				{
					JsonSerializer jsonSerializer = new JsonSerializer();
					using (PooledMemoryStream memoryStream = new PooledMemoryStream(Misc.arrayPool))
					{
						memoryStream.SetLength(4);
						memoryStream.Seek(4, SeekOrigin.Begin);
						using (Stream deflateStream = new DeflateStream(memoryStream, CompressionLevel.Optimal, true))
						{
							BsonDataWriter bsonDataWriter = new BsonDataWriter(deflateStream);
							jsonSerializer.Serialize(bsonDataWriter, writes);
						}
						binlog_writelen = (int)memoryStream.Position;
						binlog_buffer = memoryStream.GetBuffer();
					}
					BinaryPrimitives.WriteInt32BigEndian(binlog_buffer.AsSpan(0, 4), binlog_writelen);
				}
				conditional = conditions.Count > 0;
			} else{
				binlog_buffer = null;
				binlog_writelen = 0;
				conditional = false;
			}


		nowrite:
			foreach (string str in reads)
			{
				lockLevels.TryAdd(str, LockMode.Read);
			}
			if(conditional){
				foreach (string key in conditions.Keys)
				{
					lockLevels.TryAdd(key, LockMode.Read);
				}
			}
			//Lock ordering
			List<string> locks = lockLevels.Keys.ToList();
			locks.Sort(lockOrderingComparer);

			//Pending reads
			Dictionary<string, Task<string>> pendingReads = new Dictionary<string, Task<string>>();
			Dictionary<string, string> readResults = new Dictionary<string, string>();
			Queue<LockHandle> unlockingQueue = new Queue<LockHandle>();
			Queue<LockHandle> upgradingQueue = new Queue<LockHandle>();

			bool upgraded = false;
			bool upgrading = false;

			//Acquire locks
			foreach (string id in locks)
			{
				LockMode lockMode = lockLevels[id];
				LockHandle lockHandle = await veryASMRLockingManager.Lock(id, lockMode);
				if(lockMode == LockMode.Upgradeable){
					upgradingQueue.Enqueue(lockHandle);
				}
				unlockingQueue.Enqueue(lockHandle);
			}
			await damageCheckingLock.AcquireReaderLock();
			try
			{
				foreach (string read in conditions.Keys)
				{
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

				if (!write)
				{
					return readResults;
				}
				if(conditional){
					foreach (KeyValuePair<string, string> kvp in conditions)
					{
						if (kvp.Value != await pendingReads[kvp.Key])
						{
							return readResults;
						}
					}
				}
				//Upgrade locks
				upgrading = true;
				while(upgradingQueue.TryDequeue(out LockHandle lockHandle)){
					await lockHandle.UpgradeToWriteLock();
				}
				upgraded = true;

				writes = Misc.ScrubNoEffectWrites(writes, pendingReads);
				if (writes.Count == 0)
				{
					return readResults;
				}
				
				Queue<Task> writeTasks = new Queue<Task>();
				if (binlog is { })
				{
					await binlogLock.Enter();
					try{
						if(binlogException is { }){
							throw new ObjectDamagedException(binlogException);
						}
						try
						{
							await binlog.WriteAsync(binlog_buffer, 0, binlog_writelen + 4);
							writeTasks.Enqueue(asyncDictionary.Write("LesbianDB_reserved_binlog_height", binlog.Position.ToString()));
						}
						catch (Exception e)
						{
							binlogException = e;
							throw;
						}
					}
					finally
					{
						binlogLock.Exit();
					}
					await streamFlushingManager.Flush();
				}
				await flushLock.AcquireReaderLock();
				try
				{
					foreach (KeyValuePair<string, string> keyValuePair in writes)
					{
						writeTasks.Enqueue(asyncDictionary.Write(keyValuePair.Key, keyValuePair.Value));
					}
					await writeTasks.ToArray();
				}
				finally
				{
					flushLock.ReleaseReaderLock();
				}
			}
			catch (Exception e){
				++minDamagesHeight;
				damages.Enqueue(e);
				throw;
			}
			finally
			{
				try{
					if(upgrading ^ upgraded){
						throw new Exception("Inconsistent lock upgrading (should not reach here)");
					}
					while(unlockingQueue.TryDequeue(out LockHandle lockHandle)){
						lockHandle.Dispose();
					}
				} catch(Exception e){
					++minDamagesHeight;
					damages.Enqueue(e);
					throw;
				} finally{
					damageCheckingLock.ReleaseReaderLock();
					await damageCheckingLock.AcquireWriterLock();
					try{
						if (damages.Count > minDamagesHeight)
						{
							Exception[] arrayOfDamages = damages.ToArray();
							throw new ObjectDamagedException(arrayOfDamages.Length == 1 ? arrayOfDamages[0] : new AggregateException(arrayOfDamages));
						}
					} finally{
						damageCheckingLock.ReleaseWriterLock();
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
						Reply reply = Misc.DeserializeObjectWithFastCreate<Reply>(str);
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
	public sealed class SessionLockingDatabaseEngine : IDatabaseEngine{
		private readonly IDatabaseEngine underlying;
		private readonly string nonce;
		private readonly string lockkey;
		private static readonly string[] emptyStringArray = new string[0];
		public SessionLockingDatabaseEngine(IDatabaseEngine underlying, string lockkey, out Task getlock)
		{
			this.underlying = underlying ?? throw new ArgumentNullException(nameof(underlying));
			this.lockkey = lockkey ?? throw new ArgumentNullException(nameof(lockkey));
			this.underlying = underlying ?? throw new ArgumentNullException(nameof(underlying));
			Span<byte> bytes = stackalloc byte[32];
			RandomNumberGenerator.Fill(bytes);
			nonce = Convert.ToBase64String(bytes, Base64FormattingOptions.None);
			getlock = underlying.Execute(emptyStringArray, SafeEmptyReadOnlyDictionary<string, string>.instance, new Dictionary<string, string>() {
				{lockkey, nonce}
			});
		}
		private IEnumerable<string> AppendToEnd(IEnumerable<string> original, string key){
			foreach(string str in original){
				if(str == key){
					throw new InvalidOperationException("Session lock should not be checked by user code");
				}
				yield return str;
			}
			yield return key;
		}
		public async Task<IReadOnlyDictionary<string, string>> Execute(IEnumerable<string> reads, IReadOnlyDictionary<string, string> conditions, IReadOnlyDictionary<string, string> writes)
		{
			IReadOnlyDictionary<string, string> result = await underlying.Execute(AppendToEnd(reads, lockkey), new Dictionary<string, string>(conditions)
			{
				{ lockkey, nonce }
			}, writes);

			IDictionary<string, string> keyValuePairs;

			if(result is IDictionary<string, string> d){
				keyValuePairs = d;
			} else{
				keyValuePairs = new Dictionary<string, string>(result);
			}
			if(keyValuePairs.TryGetValue(lockkey, out string actual)){
				if(actual == nonce){
					keyValuePairs.Remove(lockkey);
					return (IReadOnlyDictionary<string, string>)keyValuePairs;
				}
				throw new LockStolenException();
			} else{
				throw new Exception("Database does not return lock key (should not reach here)");
			}
			
		}
	}
	public sealed class LockStolenException : Exception{
		public LockStolenException() : base("The lock has been stolen by another thread"){
			
		}
	}

	public readonly struct StreamFlushingManager
	{
		private readonly ConcurrentBag<TaskCompletionSource<bool>> concurrentBag;
		private readonly AsyncManagedSemaphore asyncManagedSemaphore;

		public StreamFlushingManager(Stream stream, AsyncMutex asyncMutex)
		{
			asyncManagedSemaphore = new AsyncManagedSemaphore(0);
			concurrentBag = new ConcurrentBag<TaskCompletionSource<bool>>();
			Loop(concurrentBag, asyncManagedSemaphore, stream, asyncMutex);
		}
		private static async void Loop(ConcurrentBag<TaskCompletionSource<bool>> concurrentBag, AsyncManagedSemaphore asyncManagedSemaphore, Stream stream, AsyncMutex asyncMutex)
		{
			Queue<TaskCompletionSource<bool>> queue = new Queue<TaskCompletionSource<bool>>();
			while (true)
			{

				await asyncManagedSemaphore.Enter();
				if (!concurrentBag.TryTake(out TaskCompletionSource<bool> taskCompletionSource))
				{
					return;
				}
				queue.Enqueue(taskCompletionSource);

				ulong count = asyncManagedSemaphore.GetAll();
				for(ulong i = 0; i < count; ++i){
					if (concurrentBag.TryTake(out TaskCompletionSource<bool> taskCompletionSource1))
					{
						queue.Enqueue(taskCompletionSource1);
						continue;
					}
					return;
				}

				try
				{
					await asyncMutex.Enter();
					try
					{
						await stream.FlushAsync();
					}
					finally
					{
						asyncMutex.Exit();
					}
				}
				catch (Exception e)
				{
					while (queue.TryDequeue(out TaskCompletionSource<bool> taskCompletionSource1))
					{
						taskCompletionSource1.SetException(e);
					}
					return;
				}

				while (queue.TryDequeue(out TaskCompletionSource<bool> taskCompletionSource1))
				{
					taskCompletionSource1.SetResult(false);
				}
			}
		}

		public Task Flush()
		{
			TaskCompletionSource<bool> taskCompletionSource = new TaskCompletionSource<bool>();
			concurrentBag.Add(taskCompletionSource);
			asyncManagedSemaphore.Exit();
			return taskCompletionSource.Task;
		}

		public void Close()
		{
			asyncManagedSemaphore.Exit();
		}
	}
}