using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.InteropServices;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Collections.Concurrent;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using System.IO.Compression;

namespace LesbianDB
{
	public sealed class PurrfectODD : IFlushableAsyncDictionary{
		/// <summary>
		/// A minimal subset of YuriDatabaseEngine
		/// designed specifically for us
		/// </summary>
		public sealed class ModdedYuriDatabaseEngine
		{
			public readonly IAsyncDictionary asyncDictionary;
			public ModdedYuriDatabaseEngine(IAsyncDictionary asyncDictionary, Stream binlog)
			{
				this.asyncDictionary = asyncDictionary ?? throw new ArgumentNullException(nameof(asyncDictionary));
				this.binlog = binlog ?? throw new ArgumentNullException(nameof(binlog));
			}

			private readonly Stream binlog;
			private async Task WriteAndFlushBinlog(byte[] buffer, int len)
			{
				await binlog.WriteAsync(buffer, 0, len);
				await binlog.FlushAsync();
			}
			public async Task Execute(IReadOnlyDictionary<string, string> writes)
			{
				if(writes.Count == 0){
					return;
				}
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
				Task writeBinlog = WriteAndFlushBinlog(buffer, len + 4);
				Task[] writeTasks = new Task[writes.Count];
				int xctr = 0;
				foreach (KeyValuePair<string, string> keyValuePair in writes)
				{
					writeTasks[xctr++] = asyncDictionary.Write(keyValuePair.Key, keyValuePair.Value);
				}
				await writeTasks;
				await writeBinlog;
			}
		}
		private readonly Stream[] binlogs = new Stream[65536];
		private readonly ConcurrentDictionary<string, string> deferredWriteCache = new ConcurrentDictionary<string, string>();
		private readonly Func<IAsyncDictionary> factory;
		private readonly ModdedYuriDatabaseEngine[] yuriDatabaseEngines = new ModdedYuriDatabaseEngine[65536];

		private readonly Stream lockfile;
		private readonly string mapfile;
		private readonly string nextmapfile;
		private readonly AsyncReaderWriterLock locker = new AsyncReaderWriterLock();
		private readonly AsyncReaderWriterLock locker2 = new AsyncReaderWriterLock();
		private readonly byte[] map = new byte[524288];

		public PurrfectODD(string path, Func<IAsyncDictionary> factory, out Task finish){
			this.factory = factory ?? throw new ArgumentNullException(nameof(factory));
			StringBuilder pathBuilder = new StringBuilder(path);
			int pathlen = path.Length + 13;
			if (path.EndsWith(Path.DirectorySeparatorChar)) {
				pathlen += 1;
				pathBuilder.Append(Path.DirectorySeparatorChar);
			}
			lockfile = new FileStream(pathBuilder.Append("yuripurrfect-lock").ToString(), FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.DeleteOnClose | FileOptions.Asynchronous);
			mapfile = pathBuilder.Remove(pathlen, 4).Append("map").ToString();
			nextmapfile = pathBuilder.Append("-next").ToString();
			pathBuilder.Remove(pathlen, 8).Append("binlog-");
			pathlen += 7;
			for(int i = 0; i < 65536; ++i){
				string name = pathBuilder.Append(i).ToString();
				binlogs[i] = new FileStream(name, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
				pathBuilder.Remove(pathlen, name.Length - pathlen);
			}
			finish = Finish();
		}
		private async Task Finish(){
			long len;
			await using (FileStream fileStream = new FileStream(mapfile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan))
			{
				len = await fileStream.ReadAsync(map, 0, 524288);
				if(len == 0){
					await fileStream.WriteAsync(map, 0, 524288);
				}
			}
			if(len != 0){
				if(len != 524288){
					throw new EndOfStreamException("Unexpected mapfile size: " + len.ToString());
				}
				for(int i = 0; i < 65536; ++i){
					long expectedSize = BinaryPrimitives.ReadInt64BigEndian(map.AsSpan(i * 8, 8));
					Stream binlog = binlogs[i];
					long actualSize = binlog.Length;
					if(expectedSize > actualSize){
						throw new EndOfStreamException(new StringBuilder("Binlog #").Append(i).Append(" is shorter than expected").ToString());
					}
					if(actualSize > expectedSize){
						//Crash or unexpected power interruption before flush
						//Revert potentially inconsistent transactions
						binlog.SetLength(expectedSize);
					}
				}
			}
		}
		private async Task<ModdedYuriDatabaseEngine> GetYuriDatabaseEngine(ushort id){
			await locker2.AcquireReaderLock();
			try{
				ModdedYuriDatabaseEngine yuriDatabaseEngine = yuriDatabaseEngines[id];
				if(yuriDatabaseEngine is { }){
					return yuriDatabaseEngine;
				}
			} finally{
				locker2.ReleaseReaderLock();
			}
			await locker2.AcquireWriterLock();
			try
			{
				ModdedYuriDatabaseEngine yuriDatabaseEngine = yuriDatabaseEngines[id];
				if (yuriDatabaseEngine is null)
				{
					IAsyncDictionary asyncDictionary = factory();
					Stream binlog = binlogs[id];
					await YuriDatabaseEngine.RestoreBinlog(binlog, asyncDictionary);
					yuriDatabaseEngine = new ModdedYuriDatabaseEngine(asyncDictionary, binlog);
					yuriDatabaseEngines[id] = yuriDatabaseEngine;
				}
				return yuriDatabaseEngine;
			}
			finally
			{
				locker2.ReleaseWriterLock();
			}
		}
		private static ushort Hash(string key){
			return (ushort)(Misc.HashString2(key) & 65535);
		}
		public async Task<string> Read(string key)
		{
			lockfile.SetLength(0); //dispose protect
			await locker.AcquireReaderLock();
			try{
				if(deferredWriteCache.TryGetValue(key, out string value)){
					return value;
				}
				return await (await GetYuriDatabaseEngine(Hash(key))).asyncDictionary.Read(key);
			} finally{
				locker.ReleaseReaderLock();
				GC.KeepAlive(lockfile);
			}
		}

		public async Task Write(string key, string value)
		{
			lockfile.SetLength(0); //dispose protect
			await locker.AcquireReaderLock();
			try{
				deferredWriteCache[key] = value;
			} finally{
				locker.ReleaseReaderLock();
				GC.KeepAlive(lockfile);
			}
		}
		private readonly struct DictAndGetEngine{
			public readonly Dictionary<string, string> dictionary;
			public readonly Task<ModdedYuriDatabaseEngine> getDatabaseEngine;

			public DictAndGetEngine(Task<ModdedYuriDatabaseEngine> getDatabaseEngine)
			{
				this.getDatabaseEngine = getDatabaseEngine;
				dictionary = new Dictionary<string, string>();
			}
		}
		public async Task Flush()
		{
			lockfile.SetLength(0); //dispose protect
			if(deferredWriteCache.Count == 0){
				return;
			}
			await locker.AcquireWriterLock();
			try{
				KeyValuePair<string, string>[] keyValuePairs = deferredWriteCache.ToArray();
				if(keyValuePairs.Length == 0){
					return;
				}
				deferredWriteCache.Clear();
				Dictionary<ushort, DictAndGetEngine> dictionaries = new Dictionary<ushort, DictAndGetEngine>();
				foreach(KeyValuePair<string, string> keyValuePair in keyValuePairs){
					string key = keyValuePair.Key;
					ushort hash = Hash(key);
					if (!dictionaries.TryGetValue(hash, out DictAndGetEngine dictAndGetEngine)){
						dictAndGetEngine = new DictAndGetEngine(GetYuriDatabaseEngine(hash));
						dictionaries.Add(hash, dictAndGetEngine);
					}
					dictAndGetEngine.dictionary.Add(key, keyValuePair.Value);
				}
				Task[] flushings = new Task[dictionaries.Count];
				int xctr = 0;
				Queue<ushort> dirty = new Queue<ushort>();
				foreach(KeyValuePair<ushort, DictAndGetEngine> keyValuePair1 in dictionaries){
					DictAndGetEngine dictAndGetEngine = keyValuePair1.Value;
					flushings[xctr++] = (await dictAndGetEngine.getDatabaseEngine).Execute(dictAndGetEngine.dictionary);
					dirty.Enqueue(keyValuePair1.Key);
				}
				await flushings;
				while (dirty.TryDequeue(out ushort ky))
				{
					BinaryPrimitives.WriteInt64BigEndian(map.AsSpan(ky * 8, 8), binlogs[ky].Position);
				}
				try{
					await File.WriteAllBytesAsync(nextmapfile, map);
					File.Replace(nextmapfile, mapfile, null, true);
				} finally{
					try{
						File.Delete(nextmapfile);
					} catch{
						
					}
				}
			} finally{
				locker.ReleaseWriterLock();
				GC.KeepAlive(lockfile);
			}
		}
	}
}
