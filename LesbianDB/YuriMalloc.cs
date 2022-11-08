using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;

namespace LesbianDB
{
	public interface ISwapAllocator{
		public Task<Func<Task<byte[]>>> Write(byte[] bytes);
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
							forwardingSwapReference.underlying = await newAllocator.Write(await forwardingSwapReference.underlying());
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

		public async Task<Func<Task<byte[]>>> Write(byte[] bytes)
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
			return reference.Dereference;
		}

		private sealed class ForwardingSwapReference{
			public volatile Func<Task<byte[]>> underlying;

			public ForwardingSwapReference(Func<Task<byte[]>> underlying)
			{
				this.underlying = underlying;
			}

			public Task<byte[]> Dereference(){
				return underlying();
			}
		}

		private sealed class SimpleSwap{
			private readonly FileStream fileStream = new FileStream(Misc.GetRandomFileName(), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 1, FileOptions.DeleteOnClose | FileOptions.Asynchronous | FileOptions.WriteThrough | FileOptions.RandomAccess);
			public SimpleSwap(){
				GC.SuppressFinalize(fileStream);
			}
			private readonly AsyncMutex asyncMutex = new AsyncMutex();
			public async Task<Func<Task<byte[]>>> Write(byte[] bytes)
			{
				long address;
				await asyncMutex.Enter();
				try{
					address = fileStream.Seek(0, SeekOrigin.End);
					await fileStream.WriteAsync(bytes);
				} finally{
					asyncMutex.Exit();
				}
				int size = bytes.Length;
				return async () => {
					byte[] bytes = new byte[size];
					await asyncMutex.Enter();
					try
					{
						fileStream.Seek(address, SeekOrigin.Begin);
						await fileStream.ReadAsync(bytes, 0, size);
					}
					finally
					{
						asyncMutex.Exit();
					}
					return bytes;
				};
			}
			~SimpleSwap(){
				//Doesn't care about result
				_ = fileStream.DisposeAsync();
			}
		}
	}
}
