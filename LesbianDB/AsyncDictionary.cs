using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json;

namespace LesbianDB
{
	public interface IAsyncDictionary
	{
		public Task<string> Read(string key);
		public Task Write(string key, string value);
	}

	public sealed class SequentialAccessAsyncDictionary : IAsyncDictionary{
		private readonly ISwapAllocator allocator;
		private readonly AsyncReaderWriterLock asyncReaderWriterLock = new AsyncReaderWriterLock();

		public SequentialAccessAsyncDictionary(ISwapAllocator allocator)
		{
			this.allocator = allocator ?? throw new ArgumentNullException(nameof(allocator));
		}

		private Task<Func<Task<byte[]>>> current;


		public async Task<string> Read(string key){
			if (key is null)
			{
				throw new ArgumentNullException(nameof(key));
			}
			await asyncReaderWriterLock.AcquireReaderLock();
			try{
				if (current is { }){
					byte[] bytes1 = await (await current)();
					using Stream deflateStream = new DeflateStream(new MemoryStream(bytes1, 0, bytes1.Length, false, false), CompressionMode.Decompress, false);
					BsonDataReader bsonDataReader = new BsonDataReader(deflateStream, true, DateTimeKind.Unspecified);
					GC.SuppressFinalize(bsonDataReader);
					bsonDataReader.Read();
					while (true)
					{
						string temp = bsonDataReader.ReadAsString();

						if (temp is null)
						{
							break;
						}
						else if (temp == key)
						{
							return bsonDataReader.ReadAsString();
						} else{
							bsonDataReader.Read();
						}
					}
				}
			} finally{
				asyncReaderWriterLock.ReleaseReaderLock();
			}
			return null;
		}
		public async Task Write(string key, string value){
			if(key is null){
				throw new ArgumentNullException(nameof(key));
			}
			await asyncReaderWriterLock.AcquireWriterLock();
			try
			{
				using MemoryStream output = new MemoryStream();
				using (Stream outputDeflateStream = new DeflateStream(output, CompressionLevel.Optimal, false)){
					BsonDataWriter bsonDataWriter = new BsonDataWriter(outputDeflateStream);
					GC.SuppressFinalize(bsonDataWriter);
					bsonDataWriter.WriteStartArray();

					if (current is { })
					{
						byte[] bytes1 = await (await current)();
						using Stream input = new DeflateStream(new MemoryStream(bytes1, 0, bytes1.Length, false, false), CompressionMode.Decompress, false);
						BsonDataReader bsonDataReader = new BsonDataReader(input, true, DateTimeKind.Unspecified);
						GC.SuppressFinalize(bsonDataReader);
						bsonDataReader.Read();
						while (true)
						{
							string temp = bsonDataReader.ReadAsString();

							if (temp is null){
								break;
							} else if (temp == key)
							{
								bsonDataReader.Read();
							}
							else
							{
								bsonDataWriter.WriteValue(temp);
								bsonDataWriter.WriteValue(bsonDataReader.ReadAsString());
							}
						}
					}
					if(value is { }){
						bsonDataWriter.WriteValue(key);
						bsonDataWriter.WriteValue(value);
					}
					bsonDataWriter.WriteEndArray();
				}
				current = allocator.Write(output.ToArray());
			}
			finally
			{
				asyncReaderWriterLock.ReleaseWriterLock();
			}
		}
	}


}
