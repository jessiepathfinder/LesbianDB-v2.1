using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace LesbianDB
{
	/// <summary>
	/// The're is always the harzard that excessively large databases may integer overflow some limits, so our LargeDataOffloader moves long values to their own dedicated storage engine
	/// </summary>
	public sealed class LargeDataOffloader : IAsyncDictionary
	{
		private readonly IAsyncDictionary underlying;
		private readonly ISwapAllocator swapAllocator;
		private readonly ConcurrentXHashMap<Func<Task<PooledReadOnlyMemoryStream>>> largeKeys = new ConcurrentXHashMap<Func<Task<PooledReadOnlyMemoryStream>>>();
		private readonly CompressionLevel compressionLevel;

		public LargeDataOffloader(IAsyncDictionary underlying, ISwapAllocator swapAllocator, CompressionLevel compressionLevel)
		{
			this.underlying = underlying ?? throw new ArgumentNullException(nameof(underlying));
			this.swapAllocator = swapAllocator ?? throw new ArgumentNullException(nameof(swapAllocator));
			this.compressionLevel = compressionLevel;
		}

		public async Task<string> Read(string key)
		{
			if (largeKeys.TryGetValue(key, out Func<Task<PooledReadOnlyMemoryStream>> func)){
				using PooledMemoryStream pooledMemoryStream = new PooledMemoryStream(Misc.arrayPool);
				using (DeflateStream deflateStream = new DeflateStream(await func(), CompressionMode.Decompress, false))
				{
					byte[] buffer = null;
					try
					{
						buffer = Misc.arrayPool.Rent(65536);
						int len = buffer.Length;
					start:
						int read = deflateStream.Read(buffer, 0, len);
						if (read > 0)
						{
							pooledMemoryStream.Write(buffer, 0, read);
							goto start;
						}
					}
					finally
					{
						if (buffer is { })
						{
							Misc.arrayPool.Return(buffer, false);
						}
					}
				}
				return new string(MemoryMarshal.Cast<byte, char>(pooledMemoryStream.GetBuffer().AsSpan(0, ((int)pooledMemoryStream.Position) + 1)));
			}
			return await underlying.Read(key);
		}

		public async Task Write(string key, string value)
		{
			if(value is null){
				if(largeKeys.TryRemove(new Hash256(key), out _)){
					return;
				}
			} else{
				int len = value.Length;
				if(len > 128){
					Task tsk = underlying.Write(key, null);
					len *= 2;
					byte[] buffer;
					using (PooledMemoryStream pooledMemoryStream = new PooledMemoryStream(Misc.arrayPool, len)){
						using (DeflateStream deflateStream = new DeflateStream(pooledMemoryStream, compressionLevel, true)){
							deflateStream.Write(MemoryMarshal.AsBytes(value.AsSpan()));
						}
						buffer = pooledMemoryStream.GetBuffer();
					}
					largeKeys[key] = await swapAllocator.Write(buffer.AsMemory(0, len));
					await tsk;
					return;
				}
				largeKeys.Remove(key);
			}
			await underlying.Write(key, value);
		}
	}
}
