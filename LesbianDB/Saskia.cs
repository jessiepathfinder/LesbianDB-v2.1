using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Bson;

namespace LesbianDB
{
	public interface ISwapHandle{
		public Task Set(ReadOnlyMemory<byte> bytes);
		public Task<PooledReadOnlyMemoryStream> Get();
	}
	public sealed class FileSwapHandle : ISwapHandle{
		private readonly string filename;
		private readonly Stream writeStream;
		private readonly ConcurrentBag<Stream> readStreams = new ConcurrentBag<Stream>();
		private readonly FileShare fileShare;
		private readonly AsyncReaderWriterLock locker = new AsyncReaderWriterLock();

		public FileSwapHandle(string filename)
		{
			this.filename = filename ?? throw new ArgumentNullException(nameof(filename));
			writeStream = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
			fileShare = FileShare.ReadWrite;
		}
		public FileSwapHandle(){
			filename = Misc.GetRandomFileName();
			writeStream = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.DeleteOnClose);
			fileShare = FileShare.ReadWrite | FileShare.Delete;
		}

		public async Task Set(ReadOnlyMemory<byte> bytes)
		{
			await locker.AcquireWriterLock();
			try{
				await writeStream.WriteAsync(bytes);
				long position = writeStream.Position;
				await writeStream.FlushAsync();
				if (writeStream.Length > position)
				{
					writeStream.SetLength(position);
				}
				writeStream.Seek(0, SeekOrigin.Begin);
			} finally{
				locker.ReleaseWriterLock();
			}
		}

		public async Task<PooledReadOnlyMemoryStream> Get()
		{
			await locker.AcquireReaderLock();
			try{
				if(!readStreams.TryTake(out Stream stream)){
					stream = new FileStream(filename, FileMode.Open, FileAccess.Read, fileShare, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
					GC.KeepAlive(writeStream);
				}
				int len = (int)stream.Length;
				byte[] bytes = null;
				try{
					bytes = Misc.arrayPool.Rent(len);
					await stream.ReadAsync(bytes, 0, len);
					FlushAndReturn(stream, readStreams);
				} catch{
					if(bytes is { }){
						Misc.arrayPool.Return(bytes, false);
					}
					throw;
				}
				return new PooledReadOnlyMemoryStream(Misc.arrayPool, bytes, len);
			} finally{
				locker.ReleaseReaderLock();
			}
		}
		private static async void FlushAndReturn(Stream str, ConcurrentBag<Stream> recycler){
			await str.FlushAsync();
			str.Seek(0, SeekOrigin.Begin);
			recycler.Add(str);
		}
	}
	public sealed class EphemeralSwapHandle : ISwapHandle{
		private readonly ISwapAllocator swapAllocator;
		private volatile Func<Task<PooledReadOnlyMemoryStream>> underlying;

		public EphemeralSwapHandle(ISwapAllocator swapAllocator)
		{
			this.swapAllocator = swapAllocator ?? throw new ArgumentNullException(nameof(swapAllocator));
		}

		public Task<PooledReadOnlyMemoryStream> Get()
		{
			Func<Task<PooledReadOnlyMemoryStream>> temp = underlying;
			if(temp is null){
				return DefaultResult<PooledReadOnlyMemoryStream>.instance;
			} else{
				return temp();
			}
		}

		public async Task Set(ReadOnlyMemory<byte> bytes)
		{
			underlying = await swapAllocator.Write(bytes);
		}
	}
	public sealed class PooledReadOnlyMemoryStream : MemoryStream{
		private volatile ArrayPool<byte> pool;
		private readonly object initialPool;
		private volatile byte[] bytes;
		public PooledReadOnlyMemoryStream(ArrayPool<byte> pool, byte[] bytes, int length) : base(bytes, 0, length, false, false){
			initialPool = pool ?? throw new ArgumentNullException(nameof(pool));
			this.pool = pool;
			this.bytes = bytes;
		}
		public PooledReadOnlyMemoryStream(byte[] bytes, int length) : base(bytes, 0, length, false, false)
		{
			GC.SuppressFinalize(this);
			this.bytes = bytes;
		}
		/// <summary>
		/// Detaches the current PooledReadOnlyMemoryStream from the array pool and
		/// returns a memory view
		/// </summary>
		public Memory<byte> AsMemory()
		{
			pool = null; //De-annexation
			return (bytes ?? throw new ObjectDisposedException("Stream")).AsMemory(0, (int) Length);
		}
		/// <summary>
		/// Reinstates us if we have the original memory pool
		/// </summary>
		public bool TryReinstate(ArrayPool<byte> pool){
			if(ReferenceEquals(pool, initialPool)){
				this.pool = pool;
				return true;
			}
			return false;
		}
		protected override void Dispose(bool disposing)
		{
			byte[] current = Interlocked.Exchange(ref bytes, null);
			if(current is { }){
				pool?.Return(current, false);
			}
			base.Dispose(disposing);
		}
	}
	public sealed class EnhancedSequentialAccessDictionary : IFlushableAsyncDictionary
	{
		private readonly AsyncReaderWriterLock locker = new AsyncReaderWriterLock();
		private readonly ConcurrentDictionary<string, string> cache = new ConcurrentDictionary<string, string>();
		private readonly CompressionLevel compressionLevel;
		private readonly ISwapHandle swapHandle;
		private bool flushed;

		//zram mode
		private byte[] bytes;
		private int length;

		public EnhancedSequentialAccessDictionary(ISwapHandle swapHandle, CompressionLevel compressionLevel = CompressionLevel.Optimal)
		{
			this.swapHandle = swapHandle ?? throw new ArgumentNullException(nameof(swapHandle));
			this.compressionLevel = compressionLevel;
		}
		public EnhancedSequentialAccessDictionary(ISwapHandle swapHandle, bool preflushed, CompressionLevel compressionLevel = CompressionLevel.Optimal)
		{
			this.swapHandle = swapHandle ?? throw new ArgumentNullException(nameof(swapHandle));
			flushed = preflushed;
			this.compressionLevel = compressionLevel;
		}

		public EnhancedSequentialAccessDictionary(){
			
		}
		public async Task Flush()
		{
			if (cache.IsEmpty)
			{
				return;
			}
			await locker.AcquireWriterLock();
			try{
				using PooledMemoryStream memoryStream = new PooledMemoryStream(Misc.arrayPool);
				using(DeflateStream deflateStream = new DeflateStream(memoryStream, compressionLevel, true)){
					BsonDataWriter bsonDataWriter = new BsonDataWriter(deflateStream);
					GC.SuppressFinalize(bsonDataWriter);
					bsonDataWriter.WriteStartArray();
					foreach (KeyValuePair<string, string> keyValuePair in cache.ToArray())
					{
						string key = keyValuePair.Key;
						string value = keyValuePair.Value;
						if(value is { }){
							bsonDataWriter.WriteValue(key);
							bsonDataWriter.WriteValue(value);
						}
					}
					if (flushed)
					{
						MemoryStream memoryStream1;
						if (swapHandle is null)
						{
							memoryStream1 = new MemoryStream(bytes, 0, length, false, false);
						}
						else
						{
							memoryStream1 = await swapHandle.Get();
							if (memoryStream1.Length == 0)
							{
								memoryStream1.Dispose();
								goto doneflush;
							}
						}
#pragma warning disable IDE0063 // Use simple 'using' statement
						using (Stream stream = new DeflateStream(memoryStream1, CompressionMode.Decompress, false)){
#pragma warning restore IDE0063 // Use simple 'using' statement
							using BsonDataReader bsonDataReader = new BsonDataReader(stream);
							bsonDataReader.ReadRootValueAsArray = true;
							GC.SuppressFinalize(bsonDataReader);
							bsonDataReader.Read();
							while (true)
							{
								string temp = bsonDataReader.ReadAsString();
								if (temp is null)
								{
									break;
								}
								if (cache.ContainsKey(temp))
								{
									bsonDataReader.Read();
								} else{
									bsonDataWriter.WriteValue(temp);
									bsonDataWriter.WriteValue(bsonDataReader.ReadAsString());
								}
							}	
						}
					}
				doneflush:
					bsonDataWriter.WriteEndArray();
					bsonDataWriter.Flush();
				}
				cache.Clear();
				flushed = true;
				if (swapHandle is null)
				{
					bytes = memoryStream.GetBuffer();
					length = (int)memoryStream.Position;
				}
				else {
					await swapHandle.Set(memoryStream.GetBuffer().AsMemory(0, (int)memoryStream.Position));
				}
			} finally{
				locker.ReleaseWriterLock();
			}
		}

		public async Task<string> Read(string key)
		{
			await locker.AcquireReaderLock();
			try{
				if (cache.TryGetValue(key, out string value))
				{
					return value;
				}
				if(flushed){
					Stream memoryStream1;
					if (swapHandle is null)
					{
						memoryStream1 = new MemoryStream(bytes, 0, length, false, false);
					}
					else
					{
						memoryStream1 = await swapHandle.Get();
						if (memoryStream1.Length == 0)
						{
							memoryStream1.Dispose();
							return null;
						}
					}
					using Stream stream = new DeflateStream(memoryStream1, CompressionMode.Decompress, false);
					using BsonDataReader bsonDataReader = new BsonDataReader(stream);
					GC.SuppressFinalize(bsonDataReader);
					bsonDataReader.ReadRootValueAsArray = true;
					bsonDataReader.Read();
					while (true){
						string temp = bsonDataReader.ReadAsString();
						if(temp is null){
							return null;
						}
						if(temp == key){
							return bsonDataReader.ReadAsString();
						}
						bsonDataReader.Read();
					}
				}
				return null;
			} finally{
				locker.ReleaseReaderLock();
			}
		}

		public async Task Write(string key, string value)
		{
			await locker.AcquireReaderLock();
			try
			{
				cache[key] = value;
			}
			finally
			{
				locker.ReleaseReaderLock();
			}
		}
	}
	public sealed class RandomFlushingCache : IFlushableAsyncDictionary, IAsyncDisposable{
		private readonly IFlushableAsyncDictionary[] flushableAsyncDictionaries = new IFlushableAsyncDictionary[65536];
		private static byte Random(out ushort rnd2){
			Span<byte> bytes = stackalloc byte[3];
			RandomNumberGenerator.Fill(bytes);
			rnd2 = BitConverter.ToUInt16(bytes.Slice(0, 2));

			return bytes[2];
		}

		private readonly bool userandomhash;
		private readonly Task evictionLoop;
		public RandomFlushingCache(Func<IFlushableAsyncDictionary> func, long softMemoryLimit, bool userandomhash)
		{
			for(int i = 0; i < 65536; ){
				flushableAsyncDictionaries[i++] = func();
			}
			this.userandomhash = userandomhash;
			evictionLoop = EvictionLoop(new WeakReference<IFlushableAsyncDictionary[]>(flushableAsyncDictionaries, false), softMemoryLimit, cancellationTokenSource.Token);
			Misc.BackgroundAwait(evictionLoop);
		}
		private IFlushableAsyncDictionary Hash(string key){
			return flushableAsyncDictionaries[(userandomhash ? ("Minecraft Alex is lesbian" + key).GetHashCode() : Misc.HashString2(key)) & 65535];
		}
		private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
		private static async Task EvictionLoop(WeakReference<IFlushableAsyncDictionary[]> weakReference, long softMemoryLimit, CancellationToken cancellationToken){
		start:
			byte rnd = Random(out ushort select);
			if (Misc.thisProcess.VirtualMemorySize64 > softMemoryLimit)
			{
				try{
					await Task.Delay(1, cancellationToken);
				} catch(OperationCanceledException){
					return;
				}
			}
			else
			{
				try
				{
					await Task.Delay(rnd + 1, cancellationToken);
				}
				catch (OperationCanceledException)
				{
					return;
				}
			}
			if (weakReference.TryGetTarget(out IFlushableAsyncDictionary[] array))
			{
				await array[select].Flush();
				goto start;
			}
		}

		public async Task Flush()
		{
			Queue<Task> tasks = new Queue<Task>();
			foreach(IFlushableAsyncDictionary flushableAsyncDictionary in flushableAsyncDictionaries){
				tasks.Enqueue(flushableAsyncDictionary.Flush());
			}
			while(tasks.TryDequeue(out Task tsk)){
				await tsk;
			}
		}

		public Task<string> Read(string key)
		{
			return Hash(key).Read(key);
		}

		public Task Write(string key, string value)
		{
			return Hash(key).Write(key, value);
		}

		private volatile int disposed;
		public async ValueTask DisposeAsync()
		{
			if(Interlocked.Exchange(ref disposed, 1) == 0){
				cancellationTokenSource.Cancel();
				await evictionLoop;
				await Flush();
			}
		}
	}
	public sealed class RandomReplacementWriteThroughCache : IFlushableAsyncDictionary{
		private readonly ConcurrentXHashMap<string>[] cache = new ConcurrentXHashMap<string>[256];
		private readonly IAsyncDictionary underlying;

		public RandomReplacementWriteThroughCache(IAsyncDictionary underlying, long softMemoryLimit)
		{
			for (int i = 0; i < 256;)
			{
				cache[i++] = new ConcurrentXHashMap<string>();
			}
			EvictionThread(new WeakReference<ConcurrentXHashMap<string>[]>(cache, false), softMemoryLimit);
			this.underlying = underlying ?? throw new ArgumentNullException(nameof(underlying));
		}
		private static void RandomEvict(ConcurrentXHashMap<string>[] cache)
		{
			Span<byte> bytes = stackalloc byte[1];
			RandomNumberGenerator.Fill(bytes);
			cache[bytes[0]].Clear();
		}
		private static Task RandomWait(){
			Span<byte> bytes = stackalloc byte[1];
			RandomNumberGenerator.Fill(bytes);
			return Task.Delay(bytes[0] + 1);
		}
		private static async void EvictionThread(WeakReference<ConcurrentXHashMap<string>[]> weakReference, long softMemoryLimit){
		start:
			await RandomWait();
			if(weakReference.TryGetTarget(out ConcurrentXHashMap<string>[] cache)){
				if(Misc.thisProcess.VirtualMemorySize64 > softMemoryLimit){
					RandomEvict(cache);
				}
				goto start;
			}
		}
		private ConcurrentXHashMap<string> Hash(string key){
			return cache[("Cute anime lesbians" + key).GetHashCode() & 255];
		}
		public async Task<string> Read(string key)
		{
			ConcurrentXHashMap<string> keyValuePairs = Hash(key);
			if(keyValuePairs.TryGetValue(key, out string value)){
				return value;
			}
			return keyValuePairs.GetOrAdd(new Hash256(key), await underlying.Read(key));
		}

		public Task Write(string key, string value)
		{
			Task task = underlying.Write(key, value);
			Hash(key)[key] = value;
			return task;
		}

		public Task Flush()
		{
			if(underlying is IFlushableAsyncDictionary flushable){
				return flushable.Flush();
			} else{
				return Misc.completed;
			}
		}
	}
}
