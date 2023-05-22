using LesbianDB;
using Newtonsoft.Json.Bson;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LesbianDB{
	public interface IDurableDictionary : IAsyncDictionary{
		
	}

	/// <summary>
	/// A high-purrformance ACID-compilant simple key-value store
	/// </summary>
	public sealed class NekomimiShard : IDurableDictionary, IAsyncDisposable
	{
		private readonly string filename;
		private readonly string tempdir;
		private string GetRandomFileName()
		{
			Span<byte> bytes = stackalloc byte[32];
			RandomNumberGenerator.Fill(bytes);
			return Path.Combine(tempdir, Convert.ToBase64String(bytes, Base64FormattingOptions.None).Replace('/', '='));
		}
		private readonly struct ReadCommand{
			public readonly TaskCompletionSource<string> taskCompletionSource;
			public readonly string key;

			public ReadCommand(TaskCompletionSource<string> taskCompletionSource, string key)
			{
				this.taskCompletionSource = taskCompletionSource;
				this.key = key;
			}
		}
		private readonly struct WriteCommand{
			public readonly TaskCompletionSource<bool> taskCompletionSource;
			public readonly string key;
			public readonly string value;

			public WriteCommand(TaskCompletionSource<bool> taskCompletionSource, string key, string value)
			{
				this.taskCompletionSource = taskCompletionSource;
				this.key = key;
				this.value = value;
			}
		}
		private readonly ConcurrentBag<WriteCommand> writeCommands = new ConcurrentBag<WriteCommand>();
		private readonly ConcurrentBag<ReadCommand> readCommands = new ConcurrentBag<ReadCommand>();
		private readonly AsyncManagedSemaphore asyncManagedSemaphore = new AsyncManagedSemaphore(0);
		private readonly Task<string> looptsk;
		private readonly Task looptsk2;

		public NekomimiShard(string filename, string tempdir)
		{
			this.filename = filename ?? throw new ArgumentNullException(nameof(filename));
			this.tempdir = tempdir ?? throw new ArgumentNullException(nameof(tempdir));
			looptsk = Loop();
			TaskCompletionSource<bool> taskCompletionSource = new TaskCompletionSource<bool>();
			looptsk.GetAwaiter().OnCompleted(() => taskCompletionSource.SetResult(false));
			looptsk2 = taskCompletionSource.Task;
		}

		private async Task<string> Loop(){
			Queue<TaskCompletionSource<bool>> taskCompletionSources = new Queue<TaskCompletionSource<bool>>();
			Queue<ReadCommand> readQueue = new Queue<ReadCommand>();
			while (true){
				Dictionary<string, Queue<TaskCompletionSource<string>>> dict = new Dictionary<string, Queue<TaskCompletionSource<string>>>();
				bool write = false;
				try
				{
					await asyncManagedSemaphore.Enter();
					ulong count = asyncManagedSemaphore.GetAll() + 1;
					ulong i = 0;

					Dictionary<string, string> state = new Dictionary<string, string>();
					Queue<string> nullpurge = new Queue<string>();

					for (; i < count; ++i)
					{
						if (writeCommands.TryTake(out WriteCommand writeCommand))
						{
							write = true;
							state[writeCommand.key] = writeCommand.value;
							if (writeCommand.value is null){
								nullpurge.Enqueue(writeCommand.key);
							}
							taskCompletionSources.Enqueue(writeCommand.taskCompletionSource);
							continue;
						}
						if (readCommands.TryTake(out ReadCommand readCommand))
						{
							readQueue.Enqueue(readCommand);
							if(write){
								continue;
							}
							string key8 = readCommand.key;
							if(!dict.TryGetValue(key8, out Queue<TaskCompletionSource<string>> queue)){
								queue = new Queue<TaskCompletionSource<string>>();
								dict.Add(key8, queue);
							}
							queue.Enqueue(readCommand.taskCompletionSource);
							continue;
						}
						throw new ObjectDisposedException(nameof(NekomimiShard));
					}
					ValueTask dispose = default;
					bool needDispose = false;
					FileStream fileStream = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
					if (fileStream.Length == 0)
					{
						dispose = fileStream.DisposeAsync();
						if (write)
						{
							needDispose = true;
							goto dowrite;
						}
						
						while (readQueue.TryDequeue(out ReadCommand readCommand))
						{
							readCommand.taskCompletionSource.SetResult(null);
						}
						await dispose;
						continue;
					}
					await using(Stream stream = new DeflateStream(fileStream, CompressionMode.Decompress, false)){
						BsonDataReader bsonDataReader = new BsonDataReader(stream, true, DateTimeKind.Unspecified) {CloseInput = false};
						await bsonDataReader.ReadAsync();
						while(true){
							string temp = await bsonDataReader.ReadAsStringAsync();
							if (temp is null)
							{
								break;
							}
							string val9 = await bsonDataReader.ReadAsStringAsync();
							if (write){
								state.TryAdd(temp, val9);
							} else{
								if(dict.TryGetValue(temp, out Queue<TaskCompletionSource<string>> queue)){
									while(queue.TryDequeue(out TaskCompletionSource<string> tsc)){
										tsc.SetResult(val9);
									}
								}
							}
						}
					}
					if (!write)
					{
						foreach (Queue<TaskCompletionSource<string>> queue in dict.Values)
						{
							while (queue.TryDequeue(out TaskCompletionSource<string> taskCompletionSource))
							{
								taskCompletionSource.SetResult(null);
							}
						}
						continue;
					}

				dowrite:
					string filename2 = GetRandomFileName();
					await using (Stream outstr = new DeflateStream(new FileStream(filename2, FileMode.Append, FileAccess.Write, FileShare.None, 4096, FileOptions.SequentialScan | FileOptions.Asynchronous), CompressionLevel.Optimal, false))
					{
						BsonDataWriter bsonDataWriter = new BsonDataWriter(outstr) { CloseOutput = false, AutoCompleteOnClose = false };
						Task tsk = bsonDataWriter.WriteStartArrayAsync();
						while (nullpurge.TryDequeue(out string str))
						{
							state.Remove(str);
						}
						await tsk;
						foreach (KeyValuePair<string, string> keyValuePair in state)
						{
							await bsonDataWriter.WriteValueAsync(keyValuePair.Key);
							await bsonDataWriter.WriteValueAsync(keyValuePair.Value);
						}
						await bsonDataWriter.WriteEndArrayAsync();
						await bsonDataWriter.CloseAsync();
					}
					if (needDispose)
					{
						await dispose;
					}
					File.Replace(filename2, filename, null);
					while (taskCompletionSources.TryDequeue(out TaskCompletionSource<bool> task))
					{
						task.SetResult(false);
					}
					while (readQueue.TryDequeue(out ReadCommand readCommand1))
					{
						if (state.TryGetValue(readCommand1.key, out string value))
						{
							readCommand1.taskCompletionSource.SetResult(value);
							continue;
						}
						readCommand1.taskCompletionSource.SetResult(null);
					}
				}
				catch (Exception e)
				{
					while (taskCompletionSources.TryDequeue(out TaskCompletionSource<bool> task))
					{
						task.SetException(e);
					}
					if(write){
						while (readQueue.TryDequeue(out ReadCommand readCommand))
						{
							readCommand.taskCompletionSource.SetException(e);
						}
					} else
					{
						foreach(Queue<TaskCompletionSource<string>> queue in dict.Values){
							while (queue.TryDequeue(out TaskCompletionSource<string> taskCompletionSource))
							{
								taskCompletionSource.SetException(e);
							}
						}
					}
					throw new ObjectDamagedException(e);
				}
			}
		}

		public async Task<string> Read(string key)
		{
			if(key is null){
				throw new ArgumentNullException(nameof(key));
			}
			TaskCompletionSource<string> taskCompletionSource = new TaskCompletionSource<string>();
			readCommands.Add(new ReadCommand(taskCompletionSource, key));
			asyncManagedSemaphore.Exit();

			return (await Task.WhenAny(taskCompletionSource.Task, looptsk)).Result;
		}

		public async Task Write(string key, string value)
		{
			if (key is null)
			{
				throw new ArgumentNullException(nameof(key));
			}
			TaskCompletionSource<bool> taskCompletionSource = new TaskCompletionSource<bool>();
			writeCommands.Add(new WriteCommand(taskCompletionSource, key, value));
			asyncManagedSemaphore.Exit();
			(await Task.WhenAny(taskCompletionSource.Task, looptsk)).GetAwaiter().GetResult();
		}

		private volatile int disposed;
		public ValueTask DisposeAsync()
		{
			if(Interlocked.Exchange(ref disposed, 1) == 0){
				asyncManagedSemaphore.Exit();
				GC.SuppressFinalize(this);
			}
			return new ValueTask(looptsk2);
		}

		~NekomimiShard(){
			if (Interlocked.Exchange(ref disposed, 1) == 0)
			{
				asyncManagedSemaphore.Exit();
				GC.SuppressFinalize(this);
			}
		}
	}
	public sealed class PseudoDurableDictionary : IDurableDictionary{
		private readonly IAsyncDictionary underlying;

		public PseudoDurableDictionary(IAsyncDictionary underlying)
		{
			this.underlying = underlying ?? throw new ArgumentNullException(nameof(underlying));
		}

		public Task<string> Read(string key)
		{
			return underlying.Read(key);
		}

		public Task Write(string key, string value)
		{
			return underlying.Write(key, value);
		}
	}
	public sealed class Nekomimi : IDurableDictionary{
		private readonly NekomimiShard[] nekomimis = new NekomimiShard[65536];
		private static readonly YuriStringHash yuriStringHash = new YuriStringHash(new YuriHash(MemoryMarshal.AsBytes("asmr yuri nekomimi".AsSpan())), new YuriHash(MemoryMarshal.AsBytes("VNCH > CHXHCNVN".AsSpan())));
		public Nekomimi(string foldername){
			Directory.CreateDirectory(foldername);
			StringBuilder sb = new StringBuilder(foldername);
			if(!foldername.EndsWith(Path.DirectorySeparatorChar)){
				sb.Append(Path.DirectorySeparatorChar);
				foldername = sb.ToString();
			}
			int limit = foldername.Length;
			for(int i = 0; i < 65536; ++i){
				nekomimis[i] = new NekomimiShard(sb.Append(i).Append(".nekoshard").ToString(), foldername);
			}
		}

		public static ulong Hash(string key){
			return yuriStringHash.HashString(MemoryMarshal.AsBytes(key.AsSpan())) & 65535;
		}

		public Task<string> Read(string key)
		{
			return nekomimis[Hash(key)].Read(key);
		}

		public Task Write(string key, string value)
		{
			return nekomimis[Hash(key)].Write(key, value);
		}
	}
	public sealed class DurableWriteThroughCache : IDurableDictionary
	{
		private readonly ConcurrentXHashMap<string>[] cache = new ConcurrentXHashMap<string>[256];
		private readonly IDurableDictionary underlying;
		private readonly YuriStringHash yuriStringHash = new YuriStringHash(YuriHash.GetRandom(), YuriHash.GetRandom());

		public DurableWriteThroughCache(IDurableDictionary underlying, long softMemoryLimit)
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
			cache[Misc.FastRandom(0, 256)].Clear();
		}
		private static async void EvictionThread(WeakReference<ConcurrentXHashMap<string>[]> weakReference, long softMemoryLimit)
		{
		start:
			await Misc.WaitForNextGC();
			if (weakReference.TryGetTarget(out ConcurrentXHashMap<string>[] cache))
			{
				if (Misc.thisProcess.VirtualMemorySize64 > softMemoryLimit)
				{
					RandomEvict(cache);
				}
				goto start;
			}
		}
		private ConcurrentXHashMap<string> Hash(string key)
		{
			return cache[yuriStringHash.HashString(MemoryMarshal.AsBytes(key.AsSpan())) & 255];
		}
		public async Task<string> Read(string key)
		{
			ConcurrentXHashMap<string> keyValuePairs = Hash(key);
			if (keyValuePairs.TryGetValue(key, out string value))
			{
				return value;
			}
			return keyValuePairs.GetOrAdd(new Hash256(key), await underlying.Read(key));
		}

		public Task Write(string key, string value)
		{
			ConcurrentXHashMap<string> keyValuePairs = Hash(key);
			if (keyValuePairs.TryGetValue(key, out string value1))
			{
				if (value == value1)
				{
					return Misc.completed;
				}
			}
			Task task = underlying.Write(key, value);
			keyValuePairs[key] = value;
			return task;
		}
	}
}