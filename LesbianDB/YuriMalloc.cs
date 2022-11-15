using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Collections.Concurrent;

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
			private readonly FileStream fileStream;
			private readonly ConcurrentStack<Stream> recycler = new ConcurrentStack<Stream>();
			private readonly string fileName = Misc.GetRandomFileName();
			public SimpleSwap(){
				fileStream = new FileStream(fileName, FileMode.CreateNew, FileAccess.Write, FileShare.Read, 256, FileOptions.Asynchronous | FileOptions.DeleteOnClose | FileOptions.SequentialScan);
			}
			private readonly AsyncMutex asyncMutex = new AsyncMutex();
			public async Task<Func<Task<byte[]>>> Write(byte[] bytes)
			{
				long address;
				await asyncMutex.Enter();
				try{
					address = fileStream.Seek(0, SeekOrigin.End);
					await fileStream.WriteAsync(bytes);
					await fileStream.FlushAsync();
				} finally{
					asyncMutex.Exit();
				}
				int size = bytes.Length;
				return () => Read(fileStream, recycler, fileName, address, size);
			}
			private static async Task<byte[]> Read(object keptalive, ConcurrentStack<Stream> recycler, string filename, long offset, int size){
				byte[] bytes = new byte[size];
				if(!recycler.TryPop(out Stream stream)){
					stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Write | FileShare.Delete, 256, FileOptions.RandomAccess | FileOptions.Asynchronous);
				}
				try{
					stream.Seek(offset, SeekOrigin.Begin);
					await stream.FlushAsync();
					await stream.ReadAsync(bytes, 0, size);
				} finally{
					recycler.Push(stream);

					//Keep write stream open until all references are garbage collected
					//Because we may open new read streams
					GC.KeepAlive(keptalive);
				}
				return bytes;
			}
		}
	}

	public sealed class SimpleShardedSwapAllocator<T> : ISwapAllocator where T : ISwapAllocator, new(){
		private readonly T[] swapAllocators;
		public SimpleShardedSwapAllocator(int count){
			if(count < 2){
				throw new ArgumentOutOfRangeException("Minimum 2 swap allocators per SimpleShardedSwapAllocator");
			}
			swapAllocators = new T[count];
			for(int i = 0; i < count; ){
				swapAllocators[i++] = new T();
			}
		}

		public Task<Func<Task<byte[]>>> Write(byte[] bytes)
		{
			return swapAllocators[RandomNumberGenerator.GetInt32(0, swapAllocators.Length)].Write(bytes);
		}
	}
	/// <summary>
	/// YuriMalloc swap files create a data remainance risk, so YuriEncrypt protects us by
	/// encrypting the swap files with an AES-256 key that is destroyed after the process exits
	/// </summary>
	public sealed class YuriEncrypt : ISwapAllocator
	{
		private static readonly byte[] encryptionKey = new byte[32];
		private static readonly byte[] iv = new byte[16];
		static YuriEncrypt(){
			RandomNumberGenerator.Fill(encryptionKey);
		}
		private readonly ISwapAllocator underlying;

		public YuriEncrypt(ISwapAllocator underlying)
		{
			this.underlying = underlying ?? throw new ArgumentNullException(nameof(underlying));
		}

		private static byte[] Decrypt(byte[] bytes) {
			int len = bytes.Length;
			using (Aes aes = Aes.Create())
			{
				aes.KeySize = 256;
				aes.Mode = CipherMode.CTS;
				using (CryptoStream cryptoStream = new CryptoStream(new MemoryStream(bytes, 0, len, false, false), aes.CreateDecryptor(encryptionKey, iv), CryptoStreamMode.Read, false))
				{
					byte[] decrypted = new byte[len - 16];
					if(len < 32){
						Span<byte> nothing = stackalloc byte[16];
						cryptoStream.Read(nothing);
						cryptoStream.Read(decrypted, 0, len - 16);
					} else{
						cryptoStream.Read(decrypted, 0, 16);
						cryptoStream.Read(decrypted, 0, len - 16);
					}
					return decrypted;
				}
			}
		}
		public Task<Func<Task<byte[]>>> Write(byte[] bytes)
		{
			using MemoryStream memoryStream = new MemoryStream(bytes.Length + 16);
			using (Aes aes = Aes.Create())
			{
				aes.KeySize = 256;
				aes.Mode = CipherMode.CTS;
				using (CryptoStream cryptoStream = new CryptoStream(memoryStream, aes.CreateEncryptor(encryptionKey, iv), CryptoStreamMode.Write, true))
				{
					{
						Span<byte> salt = stackalloc byte[16];
						RandomNumberGenerator.Fill(salt);
						cryptoStream.Write(salt);
					}
					cryptoStream.Write(bytes, 0, bytes.Length);
					cryptoStream.FlushFinalBlock();
				}
			}
			Task<Func<Task<byte[]>>> encrypted = underlying.Write(memoryStream.ToArray());
			return Task.FromResult<Func<Task<byte[]>>>(async () =>
			{
				return Decrypt(await (await encrypted)());
			});
		}
	}
}
