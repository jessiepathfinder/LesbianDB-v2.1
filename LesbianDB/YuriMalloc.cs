using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Collections.Concurrent;
using System.Threading;

namespace LesbianDB
{
	public interface ISwapAllocator{
		public Task<Func<Task<PooledReadOnlyMemoryStream>>> Write(ReadOnlyMemory<byte> bytes);
	}
	/// <summary>
	/// YuriMalloc: lock-and-copy swap allocator
	/// </summary>
	public sealed class YuriMalloc : ISwapAllocator
	{
		private static Task RandomWait(){
			//Prevents unwanted synchronization
			Span<byte> bytes = stackalloc byte[2];
			RandomNumberGenerator.Fill(bytes);
			return Task.Delay((BitConverter.ToUInt16(bytes) / 2) + 32768);
		}
		private static async void Collect(WeakReference<YuriMalloc> weakReference){
		start:
			await RandomWait();
			if(weakReference.TryGetTarget(out YuriMalloc yuriMalloc)){
				IEnumerable<WeakReference<ForwardingSwapReference>> relocations = null;
				Queue<WeakReference<ForwardingSwapReference>> liveReferences = new Queue<WeakReference<ForwardingSwapReference>>();
				SimpleSwap newAllocator = null;
				await yuriMalloc.asyncMutex.Enter();
				try{
					int oldlen = yuriMalloc.weakReferences.Count;
					while (yuriMalloc.weakReferences.TryDequeue(out WeakReference<ForwardingSwapReference> wr)){
						if(wr.TryGetTarget(out _)){
							liveReferences.Enqueue(wr);
						}
					}
					yuriMalloc.weakReferences = liveReferences;
					if(oldlen != liveReferences.Count){ //We have dead references
						relocations = liveReferences.ToArray();
						newAllocator = new SimpleSwap();
						yuriMalloc.currentAllocator = newAllocator;
					}
				} finally{
					yuriMalloc.asyncMutex.Exit();
				}
				if(newAllocator is { }){
					foreach(WeakReference<ForwardingSwapReference> weak in relocations){
						if(weak.TryGetTarget(out ForwardingSwapReference forwardingSwapReference)){
							InternalReadonlyMemory internalReadonlyMemory = await forwardingSwapReference.underlying();

							try{
								forwardingSwapReference.underlying = await newAllocator.Write(internalReadonlyMemory.bytes.AsMemory(0, internalReadonlyMemory.size));
							} finally{
								Misc.arrayPool.Return(internalReadonlyMemory.bytes, false);
							}

						}
					}
				}

				goto start;
			}
		}
		
		private readonly AsyncMutex asyncMutex = new AsyncMutex();
		private Queue<WeakReference<ForwardingSwapReference>> weakReferences = new Queue<WeakReference<ForwardingSwapReference>>();
		private SimpleSwap currentAllocator = new SimpleSwap();
		public YuriMalloc(){
			//Garbage collector
			Collect(new WeakReference<YuriMalloc>(this, false));
		}
		private readonly struct InternalReadonlyMemory{
			public readonly byte[] bytes;
			public readonly int size;

			public InternalReadonlyMemory(byte[] bytes, int size)
			{
				this.bytes = bytes;
				this.size = size;
			}
		}
		public async Task<Func<Task<PooledReadOnlyMemoryStream>>> Write(ReadOnlyMemory<byte> bytes)
		{
			ForwardingSwapReference reference = null;
			await asyncMutex.Enter();
			try{
				reference = new ForwardingSwapReference(await currentAllocator.Write(bytes));
			} finally{
				if(reference is { }){
					weakReferences.Enqueue(new WeakReference<ForwardingSwapReference>(reference, false));
				}
				asyncMutex.Exit();
			}
			return async () =>
			{
				InternalReadonlyMemory internalReadonlyMemory = await reference.Dereference();
				return new PooledReadOnlyMemoryStream(Misc.arrayPool, internalReadonlyMemory.bytes, internalReadonlyMemory.size);
			};
		}
		private sealed class ForwardingSwapReference{
			public volatile Func<Task<InternalReadonlyMemory>> underlying;

			public ForwardingSwapReference(Func<Task<InternalReadonlyMemory>> underlying)
			{
				this.underlying = underlying;
			}

			public Task<InternalReadonlyMemory> Dereference(){
				return underlying();
			}
		}

		private sealed class SimpleSwap{
			private readonly FileStream fileStream;
			private readonly ConcurrentBag<Stream> recycler = new ConcurrentBag<Stream>();
			private readonly string fileName = Misc.GetRandomFileName();
			public SimpleSwap(){
				fileStream = new FileStream(fileName, FileMode.CreateNew, FileAccess.Write, FileShare.Read, 256, FileOptions.Asynchronous | FileOptions.DeleteOnClose | FileOptions.SequentialScan);
			}
			private readonly AsyncMutex asyncMutex = new AsyncMutex();
			public async Task<Func<Task<InternalReadonlyMemory>>> Write(ReadOnlyMemory<byte> bytes)
			{
				long address;
				await asyncMutex.Enter();
				try{
					address = fileStream.Seek(0, SeekOrigin.End);
					await fileStream.WriteAsync(bytes);
					Queue<Task> tasks = new Queue<Task>();
					tasks.Enqueue(fileStream.FlushAsync());
					foreach(Stream str in recycler.ToArray()){
						tasks.Enqueue(str.FlushAsync());
					}
					while(tasks.TryDequeue(out Task tsk)){
						await tsk;
					}
				} finally{
					asyncMutex.Exit();
				}
				int size = bytes.Length;
				return () => Read(fileStream, recycler, fileName, address, size);
			}
			private static async Task<InternalReadonlyMemory> Read(object keptalive, ConcurrentBag<Stream> recycler, string filename, long offset, int size){
				byte[] bytes = Misc.arrayPool.Rent(size);
				Stream stream;
				try{
					if (recycler.TryTake(out stream))
					{
						goto gotstream;
					}
				} catch(ObjectDisposedException){
					
				}
				stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 256, FileOptions.RandomAccess | FileOptions.Asynchronous);

				//Keep write stream open until all references are garbage collected
				//Because we may open new read streams
				GC.KeepAlive(keptalive);

			gotstream:
				try{
					stream.Seek(offset, SeekOrigin.Begin);
					await stream.ReadAsync(bytes, 0, size);
				} finally{
					recycler.Add(stream);
				}
				return new InternalReadonlyMemory(bytes, size);
			}
		}
	}

	public sealed class SimpleShardedSwapAllocator<T> : ISwapAllocator where T : ISwapAllocator, new(){
		private readonly BuddyMalloc[] swapAllocators;
		public SimpleShardedSwapAllocator(int count){
			if(count < 2){
				throw new ArgumentOutOfRangeException("Minimum 2 swap allocators per SimpleShardedSwapAllocator");
			}
			swapAllocators = new BuddyMalloc[count];
			for(int i = 0; i < count; ){
				swapAllocators[i++] = new BuddyMalloc(new T());
			}
		}

		public Task<Func<Task<PooledReadOnlyMemoryStream>>> Write(ReadOnlyMemory<byte> bytes)
		{
			return swapAllocators[RandomNumberGenerator.GetInt32(0, swapAllocators.Length)].Write(bytes);
		}
	}
	public sealed class BuddyMalloc : ISwapAllocator
	{
		private readonly ISwapAllocator underlying;
		private readonly ConcurrentBag<WeakReference<DeferredMalloc>> queue = new ConcurrentBag<WeakReference<DeferredMalloc>>();
		private readonly AsyncManagedSemaphore asyncManagedSemaphore = new AsyncManagedSemaphore(0);
		private static readonly Task<Func<Task<PooledReadOnlyMemoryStream>>> empty = Task.FromResult<Func<Task<PooledReadOnlyMemoryStream>>>(CreateEmpty);
		private static Task<PooledReadOnlyMemoryStream> CreateEmpty(){
			return Task.FromResult(new PooledReadOnlyMemoryStream(new byte[0], 0));
		}
		public BuddyMalloc(ISwapAllocator swapAllocator)
		{
			underlying = swapAllocator ?? throw new ArgumentNullException(nameof(swapAllocator));
			AllocLoop(swapAllocator, queue, asyncManagedSemaphore);
		}
		private sealed class DeferredMalloc
		{
			public volatile object backingStore;
			public int offset;
			public int count;
			public async Task<PooledReadOnlyMemoryStream> Read()
			{
				object obj = backingStore;
				if (obj is Func<Task<PooledReadOnlyMemoryStream>> func)
				{
					Task<PooledReadOnlyMemoryStream> tsk = func();
					byte[] bytes = Misc.arrayPool.Rent(count);
					try
					{
						using (Stream str = await tsk)
						{
							str.Seek(offset, SeekOrigin.Begin);
							str.Read(bytes, 0, count);
						}
						return new PooledReadOnlyMemoryStream(Misc.arrayPool, bytes, count);
					}
					catch
					{
						Misc.arrayPool.Return(bytes);
						throw;
					}
				}
				else
				{
					return new PooledReadOnlyMemoryStream((byte[])obj, count);
				}
			}
		}
		private static async void AllocLoop(ISwapAllocator swapAllocator, ConcurrentBag<WeakReference<DeferredMalloc>> queue, AsyncManagedSemaphore asyncManagedSemaphore)
		{
			PooledMemoryStream pooledMemoryStream = new PooledMemoryStream(Misc.arrayPool, 4096);
			Queue<DeferredMalloc> deferredMallocs = new Queue<DeferredMalloc>();
			while (true)
			{

				while (pooledMemoryStream.Position < 4096)
				{
					await asyncManagedSemaphore.Enter();
					WeakReference<DeferredMalloc> weakReference;
					bool take;
					try
					{
						take = queue.TryTake(out weakReference);
					}
					catch (ObjectDisposedException)
					{
						return;
					}
					if (take)
					{
						if (weakReference.TryGetTarget(out DeferredMalloc deferredMalloc))
						{
							deferredMalloc.offset = (int)pooledMemoryStream.Length;
							pooledMemoryStream.Write((byte[])deferredMalloc.backingStore, 0, deferredMalloc.count);
							deferredMallocs.Enqueue(deferredMalloc);
						}
					}
					else
					{
						return;
					}
				}
				Func<Task<PooledReadOnlyMemoryStream>> func = await swapAllocator.Write(pooledMemoryStream.GetBuffer().AsMemory(0, (int)pooledMemoryStream.Position));
				pooledMemoryStream.SetLength(0);
				while (deferredMallocs.TryDequeue(out DeferredMalloc deferredMalloc))
				{
					deferredMalloc.backingStore = func;
				}
			}
		}

		public Task<Func<Task<PooledReadOnlyMemoryStream>>> Write(ReadOnlyMemory<byte> bytes)
		{
			int len = bytes.Length;
			if (len == 0)
			{
				return empty;
			}
			if (len < 64)
			{
				byte[] bytes1 = bytes.ToArray();
				return Task.FromResult<Func<Task<PooledReadOnlyMemoryStream>>>(() => Task.FromResult(new PooledReadOnlyMemoryStream(bytes1, len)));
			}
			if (len < 4096)
			{
				DeferredMalloc deferredMalloc = new DeferredMalloc
				{
					count = len,
					backingStore = bytes.ToArray()
				};
				queue.Add(new WeakReference<DeferredMalloc>(deferredMalloc, false));
				asyncManagedSemaphore.Exit();
				return Task.FromResult<Func<Task<PooledReadOnlyMemoryStream>>>(deferredMalloc.Read);
			}

			return underlying.Write(bytes);
		}

		~BuddyMalloc()
		{
			asyncManagedSemaphore.Exit();
		}
	}
	/// <summary>
	/// A generational swap allocator designed to minimize YuriMalloc swap relocations
	/// </summary>
	public sealed class GenerationalSwapAllocator : ISwapAllocator{
		private readonly ISwapAllocator firstGeneration;
		private readonly ISwapAllocator secondGeneration;
		private readonly int delay;

		public GenerationalSwapAllocator(ISwapAllocator firstGeneration, ISwapAllocator secondGeneration, int delay)
		{
			this.firstGeneration = firstGeneration ?? throw new ArgumentNullException(nameof(firstGeneration));
			this.secondGeneration = secondGeneration ?? throw new ArgumentNullException(nameof(secondGeneration));
			this.delay = delay;
		}

		public async Task<Func<Task<PooledReadOnlyMemoryStream>>> Write(ReadOnlyMemory<byte> bytes)
		{
			return new PromotableSwapHandle(await firstGeneration.Write(bytes), secondGeneration, delay).GetBytes;
		}
		
		private sealed class PromotableSwapHandle{
			private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
			private volatile Func<Task<PooledReadOnlyMemoryStream>> func;

			public PromotableSwapHandle(Func<Task<PooledReadOnlyMemoryStream>> func, ISwapAllocator secondGeneration, int delay)
			{
				this.func = func ?? throw new ArgumentNullException(nameof(func));
				PromoteWait(cancellationTokenSource.Token, new WeakReference<PromotableSwapHandle>(this), secondGeneration, delay);
			}
			private static async void PromoteWait(CancellationToken cancellationToken, WeakReference<PromotableSwapHandle> weakReference, ISwapAllocator secondGeneration, int delay)
			{
				try{
					await Task.Delay(delay, cancellationToken);
				} catch(OperationCanceledException){
					return;
				}
				if(weakReference.TryGetTarget(out PromotableSwapHandle promotableSwapHandle)){
					byte[] buffer = null;
					try{
						using PooledReadOnlyMemoryStream bytes = await promotableSwapHandle.func();
						promotableSwapHandle.func = await secondGeneration.Write(bytes.AsMemory());
						bytes.TryReinstate(Misc.arrayPool); //Reinstatement

					} finally{
						if(buffer is { }){
							Misc.arrayPool.Return(buffer, false);
						}
					}
				}
			}

			public Task<PooledReadOnlyMemoryStream> GetBytes(){
				return func();
			}
			~PromotableSwapHandle(){
				cancellationTokenSource.Cancel();
			}
		}
	}
}
