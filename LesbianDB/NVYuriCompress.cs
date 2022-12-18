using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LesbianDB
{
	/// <summary>
	/// NVIDIA-accelerated swap compression
	/// </summary>
	public static class NVYuriCompressCore
	{
		private static readonly ConcurrentDictionary<long, TaskCompletionSource<PooledReadOnlyMemoryStream>> pendingTasks = new ConcurrentDictionary<long, TaskCompletionSource<PooledReadOnlyMemoryStream>>();
		private static readonly ConcurrentDictionary<IntPtr, PooledByteArray> pooledByteArrays = new ConcurrentDictionary<IntPtr, PooledByteArray>();
		private static long counter;

		private readonly struct PooledByteArray{
			public readonly GCHandle gchandle;
			public readonly int length;
			public PooledByteArray(int size){
				gchandle = GCHandle.Alloc(Misc.arrayPool.Rent(size), GCHandleType.Pinned);
				length = size;
			}
		}

		[DllImport("nvcomp.dll")]
		private static extern void LesbianDB_nvcomp_compress_async(IntPtr input, IntPtr size, IntPtr allocCallback, IntPtr callback, long id);
		[DllImport("nvcomp.dll")]
		private static extern void LesbianDB_nvcomp_decompress_async(IntPtr input, IntPtr size, IntPtr allocCallback, IntPtr callback, long id);
		private static IntPtr MallocCallback(IntPtr size){
			PooledByteArray pooledByteArray = new PooledByteArray((int)size.ToInt64());
			IntPtr pointer = pooledByteArray.gchandle.AddrOfPinnedObject();
			if (pooledByteArrays.TryAdd(pointer, pooledByteArray)) {
				return pointer;
			}
			throw new Exception("Two managed memory blocks allocated with the same address (should not reach here)");
		}
		private delegate IntPtr MallocCallbackDelegate(IntPtr size);
		private static readonly IntPtr mallocCallback = Marshal.GetFunctionPointerForDelegate(new MallocCallbackDelegate(MallocCallback));
		private delegate void ConpletionCallbackDelegate(IntPtr bytes, long status, byte success);
		private static readonly IntPtr completeCallback = Marshal.GetFunctionPointerForDelegate(new ConpletionCallbackDelegate(CompletionCallback));
		private static void CompletionCallback(IntPtr bytes, long id, byte success)
		{
			PooledByteArray pooledByteArray;
			if(pendingTasks.TryGetValue(id, out TaskCompletionSource<PooledReadOnlyMemoryStream> tsc)){
				if(success == 0){
					tsc.SetException(new Exception("nvcomp gdeflater error"));
				} else{
					pooledByteArray = pooledByteArrays[bytes];
					tsc.SetResult(new PooledReadOnlyMemoryStream(Misc.arrayPool, (byte[])pooledByteArray.gchandle.Target, pooledByteArray.length));
					goto dealloc;
				}
			} else{
				throw new Exception("Unknown nvcomp gdeflater task (should not reach here)");
			}
			if(bytes == IntPtr.Zero){
				return;
			}
		dealloc:
			pooledByteArrays[bytes].gchandle.Free();
		}

		private static long AssignId(TaskCompletionSource<PooledReadOnlyMemoryStream> taskCompletionSource)
		{
		start:
			long id = Interlocked.Increment(ref counter);
			if(pendingTasks.TryAdd(id, taskCompletionSource)){
				return id;
			}
			goto start;
		}
		public static async Task<PooledReadOnlyMemoryStream> Compress(ReadOnlyMemory<byte> bytes){
			TaskCompletionSource<PooledReadOnlyMemoryStream> taskCompletionSource = new TaskCompletionSource<PooledReadOnlyMemoryStream>();
			long id = AssignId(taskCompletionSource);
			using MemoryHandle memoryHandle = bytes.Pin();
			IntPtr intPtr;
			unsafe{
				intPtr = new IntPtr(memoryHandle.Pointer);
			}
			LesbianDB_nvcomp_compress_async(intPtr, new IntPtr(bytes.Length), mallocCallback, completeCallback, id);
			return await taskCompletionSource.Task;
		}
		public static async Task<PooledReadOnlyMemoryStream> Decompress(ReadOnlyMemory<byte> bytes)
		{
			TaskCompletionSource<PooledReadOnlyMemoryStream> taskCompletionSource = new TaskCompletionSource<PooledReadOnlyMemoryStream>();
			long id = AssignId(taskCompletionSource);
			using MemoryHandle memoryHandle = bytes.Pin();
			IntPtr intPtr;
			unsafe
			{
				intPtr = new IntPtr(memoryHandle.Pointer);
			}
			LesbianDB_nvcomp_decompress_async(intPtr, new IntPtr(bytes.Length), mallocCallback, completeCallback, id);
			return await taskCompletionSource.Task;
		}
		public static AsyncCompressionZram TrustedCreateWithPool(ISwapAllocator swapAllocator, long memorylimit){
			return new AsyncCompressionZram(Compress, Decompress, swapAllocator, memorylimit, Misc.arrayPool);
		}
	}
	public sealed class AsyncCompressionZram : ISwapAllocator{
		private readonly Func<ReadOnlyMemory<byte>, Task<PooledReadOnlyMemoryStream>> compress;
		private readonly Func<ReadOnlyMemory<byte>, Task<PooledReadOnlyMemoryStream>> decompress;
		private readonly ArrayPool<byte> reinstatePool;
		private readonly ConcurrentQueue<WeakReference<Whatever>> freeingQueue;
		private sealed class Whatever{
			private volatile Func<Task<PooledReadOnlyMemoryStream>> func;
			private volatile Func<Task<PooledReadOnlyMemoryStream>> cache;
			private volatile ConcurrentQueue<WeakReference<Whatever>> queue;

			public Whatever(Func<Task<PooledReadOnlyMemoryStream>> func)
			{
				this.func = func;
			}

			public Whatever(Func<Task<PooledReadOnlyMemoryStream>> cache, ConcurrentQueue<WeakReference<Whatever>> queue) : this(cache)
			{
				this.queue = queue;
			}

			public async Task<PooledReadOnlyMemoryStream> Get(){
				Func<Task<PooledReadOnlyMemoryStream>> cached = cache;
				if (cached is null)
				{
					ConcurrentQueue<WeakReference<Whatever>> queue1 = queue;
					cached = func;
					if(queue1 is { }){
						byte[] buffer = (await cached()).ToArray();
						int size = buffer.Length;
						cached = () => {
							byte[] buffer2 = null;
							try
							{
								buffer2 = Misc.arrayPool.Rent(size);
								buffer.CopyTo(buffer2, 0);
								return Task.FromResult(new PooledReadOnlyMemoryStream(Misc.arrayPool, buffer2, size));
							}
							catch
							{
								if (buffer2 is { })
								{
									Misc.arrayPool.Return(buffer2, false);
								}
								throw;
							}
						};
						if(Interlocked.CompareExchange(ref cache, cached, null) is null){
							queue1.Enqueue(new WeakReference<Whatever>(this, false));
						}
					}
				}
				return await cached();
				
			}
			public async void Promote(ISwapAllocator swapAllocator, ArrayPool<byte> arrayPool, ConcurrentQueue<WeakReference<Whatever>> weakReferences){
				if(queue is null){
					using PooledReadOnlyMemoryStream stream = await func();
					func = await swapAllocator.Write(stream.AsMemory());
					stream.TryReinstate(arrayPool);
					queue = weakReferences;
				} else{
					cache = null;
				}
			}

		}

		public AsyncCompressionZram(Func<ReadOnlyMemory<byte>, Task<PooledReadOnlyMemoryStream>> compress, Func<ReadOnlyMemory<byte>, Task<PooledReadOnlyMemoryStream>> decompress)
		{
			this.compress = compress ?? throw new ArgumentNullException(nameof(compress));
			this.decompress = decompress ?? throw new ArgumentNullException(nameof(decompress));
		}
		private static async void Collect(WeakReference<ConcurrentQueue<WeakReference<Whatever>>> wr, ISwapAllocator swapAllocator, ArrayPool<byte> reinstatePool, long memoryLimit) {
			if (memoryLimit < 1){
				memoryLimit = 1;
			}
		start:
			await Task.Delay(1);
			if(wr.TryGetTarget(out ConcurrentQueue<WeakReference<Whatever>> freeingQueue)){
				long currentMemory = Misc.thisProcess.VirtualMemorySize64;
				if (currentMemory > memoryLimit){
					long tofree = (freeingQueue.Count * (currentMemory - memoryLimit)) / ((currentMemory + memoryLimit) * 20);
					if(tofree == 0){
						tofree = 1;
					}
					for(long i = 0; i < tofree; ++i){
						if(freeingQueue.TryDequeue(out WeakReference<Whatever> weak)){
							if(weak.TryGetTarget(out Whatever whatever)){
								whatever.Promote(swapAllocator, reinstatePool, freeingQueue);
							}
						}
					}

				}
				goto start;
			}
		}
		private readonly long memoryLimit;
		private readonly ISwapAllocator underlying;
		public AsyncCompressionZram(Func<ReadOnlyMemory<byte>, Task<PooledReadOnlyMemoryStream>> compress, Func<ReadOnlyMemory<byte>, Task<PooledReadOnlyMemoryStream>> decompress, ISwapAllocator underlying, long memoryLimit, ArrayPool<byte> reinstatePool = null) : this(compress, decompress)
		{
			this.memoryLimit = memoryLimit;
			if(underlying is null){
				throw new ArgumentNullException(nameof(underlying));
			}
			this.underlying = underlying ?? throw new ArgumentNullException(nameof(underlying));
			this.reinstatePool = reinstatePool;
			freeingQueue = new ConcurrentQueue<WeakReference<Whatever>>();
			Collect(new WeakReference<ConcurrentQueue<WeakReference<Whatever>>>(freeingQueue, false), underlying, reinstatePool, memoryLimit);
		}

		public async Task<Func<Task<PooledReadOnlyMemoryStream>>> Write(ReadOnlyMemory<byte> bytes)
		{
			int uncompressedSize = bytes.Length;
			byte[] buffer = null;
			bool compressed;
			int compressedSize;
			Func<Task<PooledReadOnlyMemoryStream>> whatever;
			using (PooledReadOnlyMemoryStream stream = await compress(bytes)){
				compressedSize = ((int)stream.Length);
				compressed = uncompressedSize > compressedSize;
				if (compressed)
				{
					if (freeingQueue is { })
					{
						if (Misc.thisProcess.VirtualMemorySize64 > memoryLimit)
						{
							
							Whatever whatever2 = new Whatever(await underlying.Write(stream.AsMemory()), freeingQueue);
							stream.TryReinstate(reinstatePool);
							whatever = whatever2.Get;
							goto prefree;
						}
					}
					buffer = stream.ToArray();
				}
			}
			int size;
			if(buffer is null){
				if (freeingQueue is { })
				{
					if (Misc.thisProcess.VirtualMemorySize64 > memoryLimit)
					{

						whatever = new Whatever(await underlying.Write(bytes), freeingQueue).Get;
						goto prefree;
					}
				}
				buffer = bytes.ToArray();
				size = uncompressedSize;
			} else{
				size = compressedSize;
			}
			if(compressed && freeingQueue is null){
				//fast path
				return () => decompress(buffer.AsMemory());
			}
			if(freeingQueue is { }){
				if(Misc.thisProcess.VirtualMemorySize64 > memoryLimit){
					
					
				}
			}
			whatever = () =>
			{
				byte[] buffer2 = null;
				try
				{
					buffer2 = Misc.arrayPool.Rent(size);
					buffer.CopyTo(buffer2, 0);
					return Task.FromResult(new PooledReadOnlyMemoryStream(Misc.arrayPool, buffer2, size));
				}
				catch
				{
					if (buffer2 is { })
					{
						Misc.arrayPool.Return(buffer2, false);
					}
					throw;
				}
			};
			if (freeingQueue is null){
				return whatever;
			}
		
			Whatever whatever1 = new Whatever(whatever);
			whatever = whatever1.Get;
			freeingQueue.Enqueue(new WeakReference<Whatever>(whatever1, false));
		prefree:
			if (compressed){
				return async () =>
				{
					using PooledReadOnlyMemoryStream stream = await whatever();
					PooledReadOnlyMemoryStream result = await decompress(stream.AsMemory());
					stream.TryReinstate(reinstatePool);
					return result;
				}; 
			}
			return whatever;

		}
	}
}
