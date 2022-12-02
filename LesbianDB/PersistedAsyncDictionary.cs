using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Threading.Tasks;
using System.Threading;

namespace LesbianDB
{
	/// <summary>
	/// VERY VERY SLOW DO NOT USE!!!
	/// </summary>
	public sealed class PersistedAsyncDictionary : IAsyncDictionary, IAsyncDisposable
	{
		private readonly AsyncReaderWriterLock locker = new AsyncReaderWriterLock();
		private readonly ConcurrentBag<ReadArchive> readArchives = new ConcurrentBag<ReadArchive>();
		private readonly ConcurrentBag<Stream> readStreams = new ConcurrentBag<Stream>();
		private readonly struct ReadArchive{
			public readonly ZipArchive zipArchive;
			private readonly Stream underlyingStream;
			public ReadArchive(Stream stream){
				underlyingStream = stream;
				zipArchive = new ZipArchive(stream, ZipArchiveMode.Read, true);
			}
			public async Task Dispose(ConcurrentBag<Stream> readStreams)
			{
				zipArchive.Dispose();
				await underlyingStream.FlushAsync();
				underlyingStream.Seek(0, SeekOrigin.Begin);
				readStreams.Add(underlyingStream);
				
			}
			public ValueTask Dispose2(){
				zipArchive.Dispose();
				return underlyingStream.DisposeAsync();
			}
		}
		private readonly Stream writeStream;
		private volatile ZipArchive writeArchive;
		private readonly string filename;
		private volatile int initialized;
		public PersistedAsyncDictionary(){
			filename = Misc.GetRandomFileName();
			writeStream = new FileStream(filename, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.RandomAccess | FileOptions.Asynchronous | FileOptions.DeleteOnClose);
		}
		public PersistedAsyncDictionary(string filename){
			this.filename = filename ?? throw new ArgumentNullException(nameof(filename));
			writeStream = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.RandomAccess | FileOptions.Asynchronous);
		}
		private static string HashKey(string key){
			int len = Encoding.UTF8.GetByteCount(key);
			Span<byte> hash = stackalloc byte[48];
			if(len > 1024){
				byte[] bytes = null;
				try{
					bytes = Misc.arrayPool.Rent(len);
					Encoding.UTF8.GetBytes(key, 0, key.Length, bytes, 0);
					using SHA384 sha384 = SHA384.Create();
					sha384.TryComputeHash(bytes.AsSpan(0, len), hash, out _);
				} finally{
					if(bytes is { }){
						Misc.arrayPool.Return(bytes, false);
					}
				}
			} else{
				Span<byte> bytes = stackalloc byte[len];
				Encoding.UTF8.GetBytes(key, bytes);
				using SHA384 sha384 = SHA384.Create();
				sha384.TryComputeHash(bytes, hash, out _);
			}
			return Convert.ToBase64String(hash.Slice(0, 32), Base64FormattingOptions.None).Replace('/', '=');
		}

		public async Task<string> Read(string key)
		{
			string hash = HashKey(key);
			byte[] buffer = null;
			if(initialized == 0){
				await locker.AcquireWriterLock();
				try{
					if(initialized == 0){
						new ZipArchive(writeStream, ZipArchiveMode.Create, true).Dispose();
						await writeStream.FlushAsync();
						writeStream.Seek(0, SeekOrigin.Begin);
						initialized = 1;
					}
				} finally{
					locker.ReleaseWriterLock();
				}
			}
			await locker.AcquireReaderLock();
			try{
				
				ZipArchiveEntry zipArchiveEntry;
				ZipArchive writeArchive1 = Interlocked.Exchange(ref writeArchive, null);
				if (writeArchive1 is { })
				{
					writeArchive1.Dispose();
					await writeStream.FlushAsync();
					writeStream.Seek(0, SeekOrigin.Begin);
				}
				bool gotzip = readArchives.TryTake(out ReadArchive readArchive);
				try
				{
					//Sync IO is inevitable
					await UltraHeavyThreadPoolAwaitable.instance;
					if (!gotzip)
					{
						if(!readStreams.TryTake(out Stream stream)){
							stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 4096, FileOptions.Asynchronous | FileOptions.RandomAccess);
						}
						readArchive = new ReadArchive(stream);
					}
					zipArchiveEntry = readArchive.zipArchive.GetEntry(hash);
				} finally{
					await Task.Yield();
				}

				
				if (zipArchiveEntry is null){
					readArchives.Add(readArchive);
					return null;
				} else{
					int len = (int)zipArchiveEntry.Length;
					buffer = Misc.arrayPool.Rent(len);
					await using (Stream str = zipArchiveEntry.Open()){
						str.Read(buffer, 0, len);
					}
					readArchives.Add(readArchive);
					return Encoding.UTF8.GetString(buffer, 0, len);
				}
			} finally{
				locker.ReleaseReaderLock();
				if (buffer is { })
				{
					Misc.arrayPool.Return(buffer, false);
				}
			}
		}
		public async Task Write(string key, string value)
		{
			byte[] buffer = null;
			await locker.AcquireWriterLock();
			try{
				Queue<Task> disposeTasks = new Queue<Task>();
				while (readArchives.TryTake(out ReadArchive readArchive)){
					disposeTasks.Enqueue(readArchive.Dispose(readStreams));
				}
				while(disposeTasks.TryDequeue(out Task tsk)){
					await tsk;
				}
				ZipArchive writeArchive1 = writeArchive;
				string hash = HashKey(key);
				ZipArchiveEntry zipArchiveEntry;
				try{
					await UltraHeavyThreadPoolAwaitable.instance;
					if (writeArchive1 is null)
					{
						writeArchive1 = new ZipArchive(writeStream, ZipArchiveMode.Update, true);
						writeArchive = writeArchive1;
						initialized = 1;
					}
					zipArchiveEntry = writeArchive1.GetEntry(hash);
					if (zipArchiveEntry is null)
					{
						if (value is null)
						{
							return;
						}
						zipArchiveEntry = writeArchive1.CreateEntry(hash);
					}
				} finally{
					await Task.Yield();
				}
				
				if(value is null){
					zipArchiveEntry.Delete();
				} else{
					int len = Encoding.UTF8.GetByteCount(value);
					buffer = Misc.arrayPool.Rent(len);
					Encoding.UTF8.GetBytes(value, 0, value.Length, buffer, 0);
					await using Stream str = zipArchiveEntry.Open();
					if (str.Length > len)
					{
						str.SetLength(len);
					}
					await str.WriteAsync(buffer, 0, len);
				}
			} finally{
				locker.ReleaseWriterLock();
				if (buffer is { })
				{
					Misc.arrayPool.Return(buffer, false);
				}
			}
		}
		private bool disposed;
		public async ValueTask DisposeAsync()
		{
			await locker.AcquireWriterLock();
			try{
				if(disposed){
					return;
				}
				writeArchive?.Dispose();
				Queue<ValueTask> valueTasks = new Queue<ValueTask>();
				valueTasks.Enqueue(writeStream.DisposeAsync());
				while (readStreams.TryTake(out Stream stream))
				{
					valueTasks.Enqueue(stream.DisposeAsync());
				}
				while (readArchives.TryTake(out ReadArchive readArchive)){
					valueTasks.Enqueue(readArchive.Dispose2());
				}
				
				while(valueTasks.TryDequeue(out ValueTask valueTask)){
					await valueTask;
				}
			} finally{
				disposed = true;
				locker.ReleaseWriterLock();
			}
		}
	}
	public sealed class YuriStoreV2{
		
	}
}
