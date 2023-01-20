﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using CommandLine;
using System.Net;
using System.Net.WebSockets;
using System.Threading;
using System.Text;
using Newtonsoft.Json;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Compression;

namespace LesbianDB.Server
{
	public static class Program
	{
		private sealed class Options
		{
			[Option("listen", Required = true, HelpText = "The HTTP websocket prefix to listen to (e.g https://lesbiandb-eu.scalegrid.com/c160d449395b5fbe70fcd18cef59264b/)")]
			public string Listen { get; set; }
			[Option("engine", Required = true, HelpText = "The storage engine to use (yuri/leveldb/saskia/purrfectodd/kellyanne)")]
			public string Engine { get; set; }
			[Option("purrfectodd.flushinginterval", Required = false, HelpText = "Tells PurrfectODD to flush all writes to disk every N microseconds", Default = 30000)]
			public int PurrfectODDFlushingInterval { get; set; }
			[Option("persist-dir", Required = false, HelpText = "The directory used to store the leveldb/saskia on-disk dictionary (required for leveldb/purrfectodd, optional for saskia, have no effect for yuri/kellyanne)")]
			public string PersistDir { get; set; }
			[Option("binlog", Required = false, HelpText = "The path of the binlog used for persistance/enhanced durability (no effect for kellyanne storage engine).")]
			public string Binlog{ get; set; }
			[Option("soft-memory-limit", Required = false, HelpText = "The soft limit to memory usage (in bytes)", Default = 268435456)]
			public long SoftMemoryLimit { get; set; }

			[Option("yurimalloc.buckets", Required = false, HelpText = "The number of YuriMalloc generation 1 buckets to create (only useful for yuri storage engine, or saskia storage engine without --persist-dir set).", Default = 256)]
			public int YuriMallocBuckets { get; set; }
			[Option("yurimalloc.gen2buckets", Required = false, HelpText = "The number of YuriMalloc generation 2 buckets to create (only useful for yuri storage engine, or saskia storage engine without --persist-dir set, zero means YuriMalloc generation 2 disabled).", Default = 0)]
			public int YuriMallocGen2Buckets { get; set; }
			[Option("yurimalloc.gen2promotiondelay", Required = false, HelpText = "The number of seconds to defer promotion of YuriMalloc data from generation 1 to generation 2 (only useful for yuri storage engine, or saskia storage engine without --persist-dir set, and YuriMalloc generation 2 is enabled).", Default = 256)]
			public int YuriMallocGen2PromotionDelay { get; set; }
			[Option("yuri.buckets", Required = false, HelpText = "The number of buckets to create (only used with Yuri storage engine).", Default = 65536)]
			public int YuriBuckets { get; set; }
			[Option("accelerated-swap-compression", Required = false, HelpText = "How should we use GPU-accelerated swap compression? disable: do not use GPU-accelerated YuriMalloc swap compression, zram: use GPU-accelerated memory compression as a replacement for swapping, zcache: hot data is stored in RAM uncompressed, warm data is stored in RAM compressed, and cold data is swapped to disk compressed. This feature requires NVIDIA CUDA compartiable GPUs.", Default = "disable")]
			public string NVSwapCompression { get; set; }
			[Option("saskia.zram", Required = false, HelpText = "Tells the Saskia storage engine to use (in-CPU) memory compression instead of YuriMalloc for swapping cold data (no effect if persist-dir is specified or yuri/leveldb storage engine is used). This still has effect for PurrfectODD since PurrfectODD uses saskia as it's cache.", Default = false)]
			public bool SaskiaZram { get; set; }
			[Option("saskia.ephemeralbucketscount", Required = false, HelpText = "How many buckets should Saskia use in ephemeral mode (PurrfectODD L2 cache or without --persist-dir)", Default = 65536)]
			public int EphemeralSaskiaBucketsCount { get; set; }
			[Option("clear-binlog", Required = false, HelpText = "Clears the binlog on startup (only applictable for PurrfectODD/Saskia storage engines). Make sure to backup your on-disk dictionary first if you are using Saskia since Saskia cannot tolerate unexpected power failures!", Default = false)]
			public bool ClearBinlog { get; set; }
			[Option("no-read-cache", Required = false, HelpText = "Disables the read cache (useful for databases accessed solely via the Optimistic Functions Framework, recognized by Saskia/PurrfectODD storage engines)", Default = false)]
			public bool NoReadCache { get; set; }
			[Option("transient-storage-shards", Required = false, HelpText = "A file containing the list of transient storage shards for use with the Kellyanne sharded database engine.", Default = null)]
			public string TransientStorageShards { get; set; }
			[Option("redo-log-shards", Required = false, HelpText = "A file containing the list of redo log shards for use with the Kellyanne sharded database engine.", Default = null)]
			public string RedoLogShards { get; set; }

		}
		private static readonly ArrayPool<byte> arrayPool = ArrayPool<byte>.Create();
		private static ISwapAllocator CreateYuriMalloc(Options options, string comptype)
		{
			if(comptype == "zram"){
				return new AsyncCompressionZram(NVYuriCompressCore.Compress, NVYuriCompressCore.Decompress);
			}
			int count = options.YuriMallocBuckets;
			ISwapAllocator swapAllocator = (count < 2) ? new YuriMalloc() : ((ISwapAllocator)new SimpleShardedSwapAllocator<YuriMalloc>(count));
			count = options.YuriMallocGen2Buckets;
			if (count > 0)
			{
				swapAllocator = new GenerationalSwapAllocator(swapAllocator, count == 1 ? new BuddyMalloc(new YuriMalloc()) : ((ISwapAllocator)new SimpleShardedSwapAllocator<YuriMalloc>(count)), options.YuriMallocGen2PromotionDelay);
			}
			if(comptype == "zcache"){
				return NVYuriCompressCore.TrustedCreateWithPool(swapAllocator, options.SoftMemoryLimit);
			}
			return swapAllocator;
		}
		private static EnhancedSequentialAccessDictionary CreateCompressedAsyncDictionary()
		{
			return new EnhancedSequentialAccessDictionary();
		}
		private static async Task SmartRestoreBinlog(IAsyncDictionary asyncDictionary, Stream binlog, bool clearBinlog)
		{
			long height = Convert.ToInt64(await asyncDictionary.Read("LesbianDB_reserved_binlog_height"));
			if (height > 0)
			{
				binlog.Seek(height, SeekOrigin.Begin);
			}
			await YuriDatabaseEngine.RestoreBinlog(binlog, asyncDictionary);
			if(height > 0 & clearBinlog){
				await asyncDictionary.Write("LesbianDB_reserved_binlog_height", null);
				if(asyncDictionary is IFlushableAsyncDictionary flushableAsyncDictionary){
					await flushableAsyncDictionary.Flush();
					binlog.SetLength(0);
				}
			}
			
		}
		private static volatile Action exit;
		private static void Main(string[] args)
		{
			Console.WriteLine("LesbianDB v2.1 server\nMade by Jessie Lesbian (Discord: jessielesbian#8060)\n");

			Console.WriteLine("Registering unobserved task exception handler...");
			TaskScheduler.UnobservedTaskException += TaskScheduler_UnobservedTaskException;
			Console.WriteLine("Parsing arguments...");
			Options options = Parser.Default.ParseArguments<Options>(args).Value;
			
			if (options is null){
				return;
			}
			string engine = options.Engine.ToLower();
			Console.WriteLine("Registering abort blockers...");
			ManualResetEventSlim exitBlocker = new ManualResetEventSlim();
			ManualResetEventSlim inhibitDomainExit = new ManualResetEventSlim();
			Console.CancelKeyPress += (object obj, ConsoleCancelEventArgs e) => {
				e.Cancel = true;
				exitBlocker.Wait();
				if (exit is { })
				{
					exit();
				}
			};
			AppDomain.CurrentDomain.ProcessExit += (object obj, EventArgs e) =>{
				exitBlocker.Wait();
				if (exit is { })
				{
					exit();
				}
				inhibitDomainExit.Wait();
				inhibitDomainExit.Dispose();
			};
			IDatabaseEngine databaseEngine;
			Action closeLevelDB;
			Stream binlog;
			if(engine == "kellyanne"){
				binlog = null;
			} else{
				string binlogname = options.Binlog;
				if (binlogname is null)
				{
					binlog = null;
				}
				else
				{
					Console.WriteLine("Opening binlog...");
					binlog = new FileStream(binlogname, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.SequentialScan | FileOptions.Asynchronous);
				}
			}
			Console.WriteLine("Creating storage engine...");
			Task load;
			Task<LevelDBEngine> getDatabaseEngine;
			ISwapAllocator yuriMalloc;
			IAsyncDictionary asyncDictionary;
			IAsyncDisposable finalFlush;
			string persistdir = options.PersistDir;
			if(persistdir is { }){
				if (!persistdir.EndsWith(Path.DirectorySeparatorChar))
				{
					persistdir += Path.DirectorySeparatorChar;
				}
			}
			string comptype = options.NVSwapCompression.ToLower();
			if(comptype != "zram" && comptype != "zcache"){
				comptype = null;
			}
			string lockfile;
			CompressionLevel compressionLevel;
			switch (engine){
				case "yuri":
					finalFlush = null;
					lockfile = null;
					getDatabaseEngine = null;
					closeLevelDB = null;
					yuriMalloc = CreateYuriMalloc(options,comptype);
					int yuriBuckets = options.YuriBuckets;
					compressionLevel = comptype is null ? CompressionLevel.Optimal : CompressionLevel.NoCompression;
					asyncDictionary = new LargeDataOffloader(yuriBuckets < 2 ? new SequentialAccessAsyncDictionary(yuriMalloc, compressionLevel) : ((IAsyncDictionary)new ShardedAsyncDictionary(() => new SequentialAccessAsyncDictionary(yuriMalloc), yuriBuckets)), yuriMalloc, compressionLevel);
					if(binlog is null){
						load = null;
						databaseEngine = new YuriDatabaseEngine(asyncDictionary);
					} else{
						load = YuriDatabaseEngine.RestoreBinlog(binlog, asyncDictionary);
						databaseEngine = new YuriDatabaseEngine(asyncDictionary, binlog);
					}
					break;
				case "saskia":
					getDatabaseEngine = null;
					finalFlush = null;
					closeLevelDB = null;
					if (persistdir is null){
						lockfile = null;
						if(options.SaskiaZram){
							asyncDictionary = new ShardedAsyncDictionary(CreateCompressedAsyncDictionary, options.EphemeralSaskiaBucketsCount, options.SoftMemoryLimit);
						} else{
							yuriMalloc = CreateYuriMalloc(options, comptype);
							compressionLevel = comptype is null ? CompressionLevel.Optimal : CompressionLevel.NoCompression;
							asyncDictionary = new LargeDataOffloader(new ShardedAsyncDictionary(() => new EnhancedSequentialAccessDictionary(new EphemeralSwapHandle(yuriMalloc), compressionLevel), options.EphemeralSaskiaBucketsCount, options.SoftMemoryLimit), yuriMalloc, compressionLevel);
						}
						if (binlog is null)
						{
							load = null;
						}
						else
						{
							load = YuriDatabaseEngine.RestoreBinlog(binlog, asyncDictionary);
						}
					} else{
						int count = 0;
						lockfile = persistdir + "saskia-lock";
						IDisposable disposable = null;
						try{
							disposable = new FileStream(lockfile, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1, FileOptions.SequentialScan);
						} catch{
							Console.Error.WriteLine("Unable to lock on-disk dictionary, maybe unclean shutdown? Please delete on-disk dictionary and restart if that's the case.");
							throw;
						} finally{
							if(disposable is { }){
								disposable.Dispose();
							}
						}
						persistdir += "saskia-";
						RandomFlushingCache randomFlushingCache = new RandomFlushingCache(() => new EnhancedSequentialAccessDictionary(new FileSwapHandle(persistdir + count++), true), options.SoftMemoryLimit, false);
						asyncDictionary = randomFlushingCache;
						finalFlush = randomFlushingCache;
						if (binlog is null)
						{
							load = null;
						}
						else
						{
							load = SmartRestoreBinlog(asyncDictionary, binlog, options.ClearBinlog);
						}
					}
					if(!options.NoReadCache){
						asyncDictionary = new RandomReplacementWriteThroughCache(asyncDictionary, options.SoftMemoryLimit);
					}
					databaseEngine = binlog is null ? new YuriDatabaseEngine(asyncDictionary) : new YuriDatabaseEngine(asyncDictionary, binlog);

					break;
				case "leveldb":
					finalFlush = null;
					asyncDictionary = null;
					lockfile = null;
					if (persistdir is null){
						throw new Exception("--persist-dir is mandatory for leveldb storage engine!");
					}
					if(binlog is null){
						load = null;
						LevelDBEngine levelDBEngine = new LevelDBEngine(persistdir, options.SoftMemoryLimit);
						closeLevelDB = levelDBEngine.Dispose;
						databaseEngine = levelDBEngine;
						getDatabaseEngine = null;
					} else{
						closeLevelDB = null;
						getDatabaseEngine = LevelDBEngine.RestoreBinlog(binlog, persistdir, options.SoftMemoryLimit);
						load = null;
						databaseEngine = null;
					}
					break;
				case "purrfectodd":
					if (persistdir is null)
					{
						throw new Exception("--persist-dir is mandatory for PurrfectODD storage engine!");
					}
					
					bool saskiazram = options.SaskiaZram;
					ISwapAllocator mallocator;
					Func<IFlushableAsyncDictionary> factory = null;
					if(options.SaskiaZram){
						factory = CreateCompressedAsyncDictionary;
						compressionLevel = CompressionLevel.Optimal;
						mallocator = null;
					} else{
						mallocator = CreateYuriMalloc(options, comptype);
						compressionLevel = comptype is null ? CompressionLevel.Optimal : CompressionLevel.NoCompression;
						factory = () => new EnhancedSequentialAccessDictionary(new EphemeralSwapHandle(mallocator), compressionLevel);
					}
					long memory = options.SoftMemoryLimit;
					IFlushableAsyncDictionary flushableAsyncDictionary = new PurrfectODD(persistdir, mallocator is null ? (() => new ShardedAsyncDictionary(factory, options.EphemeralSaskiaBucketsCount, memory)) : (Func<IAsyncDictionary>)(() => new LargeDataOffloader(new ShardedAsyncDictionary(factory, options.EphemeralSaskiaBucketsCount, memory), mallocator, compressionLevel)), out load);
					IFlushableAsyncDictionary cached = options.NoReadCache ? flushableAsyncDictionary : new RandomReplacementWriteThroughCache(flushableAsyncDictionary, memory);
					if(binlog is null){
						databaseEngine = new YuriDatabaseEngine(cached, options.PurrfectODDFlushingInterval);
					} else{
						Task load2 = load;
						async Task func()
						{
							await load2;
							await SmartRestoreBinlog(flushableAsyncDictionary, binlog, options.ClearBinlog);
						}
						load = func();
						databaseEngine = new YuriDatabaseEngine(cached, binlog, true, options.PurrfectODDFlushingInterval);
					}
					asyncDictionary = cached;
					getDatabaseEngine = null;
					closeLevelDB = null;
					lockfile = null;
					finalFlush = null;
					break;
				case "kellyanne":
					string[] urls = File.ReadAllLines(options.RedoLogShards);
					int len2 = urls.Length;
					if(len2 == 0){
						throw new Exception("Minimum 1 redo log shard required");
					}
					IDatabaseEngine[] redoLogShards = new IDatabaseEngine[len2];
					for(int i = 0; i < len2; ++i){
						redoLogShards[i] = new RemoteDatabaseEngine(new Uri(urls[i]));
					}
					urls = File.ReadAllLines(options.TransientStorageShards);
					len2 = urls.Length;
					if (len2 == 0)
					{
						throw new Exception("Minimum 1 transient storage shard required");
					}
					Task[] locktasks = new Task[len2];
					IDatabaseEngine[] ephemeralStorageShards = new IDatabaseEngine[len2];
					for (int i = 0; i < len2; ++i)
					{
						ephemeralStorageShards[i] = new SessionLockingDatabaseEngine(new RemoteDatabaseEngine(new Uri(urls[i])), "LesbianDB_reserved_Kellyanne_lock", out locktasks[i]);
					}
					load = Task.WhenAll(locktasks);
					asyncDictionary = null;
					getDatabaseEngine = null;
					closeLevelDB = null;
					lockfile = null;
					finalFlush = null;
					databaseEngine = new Kellyanne(redoLogShards, ephemeralStorageShards);
					break;
				default:
					throw new Exception("Unknown storage engine: " + engine);
			}
			Console.WriteLine("Initializing HTTP listener...");
			HttpListener httpListener = new HttpListener();
			httpListener.Prefixes.Add(options.Listen);
			AsyncReaderWriterLock exitLock = new AsyncReaderWriterLock();
			int exithandled = 0;
			TaskCompletionSource<HttpListenerContext> abortSource = new TaskCompletionSource<HttpListenerContext>();


			Console.WriteLine("Starting HTTP listener...");
			httpListener.Start();
			Task coreloop = Main3(httpListener, load, getDatabaseEngine, databaseEngine, exitLock, abortSource.Task);
			Console.WriteLine("Redefining abort handler..");
			exit = () => {
				try
				{
					
				}
				finally
				{
					if (Interlocked.Exchange(ref exithandled, 1) == 1)
					{
						goto end;
					}
					Console.WriteLine("Exitting...");
					exitBlocker.Wait();
					if (httpListener.IsListening)
					{
						Console.WriteLine("Stopping HTTP listener...");
						httpListener.Stop();
					}
					Console.WriteLine("Waiting for core loop to exit...");
					abortSource.SetException(new Exception());
					coreloop.Wait();
					Console.WriteLine("Waiting for all queries to complete...");
					exitLock.AcquireWriterLock().Wait();
					try{
						exitflag = true;
					} finally{
						exitLock.ReleaseWriterLock();
					}
					long binlogHeight;
					if(binlog is null){
						binlogHeight = 0;
					} else{
						Console.WriteLine("Closing binlog...");
						binlogHeight = binlog.Position;
						binlog.Dispose();
					}
					if (closeLevelDB is { })
					{
						Console.WriteLine("Closing LevelDB on-disk dictionary...");
						closeLevelDB();
					}
					else if (finalFlush is { })
					{
						if(binlogHeight > 0){
							Console.WriteLine("Writing fast-recovery checkpoint...");
							asyncDictionary.Write("LesbianDB_reserved_binlog_height", binlogHeight.ToString()).Wait();
						}
						Console.WriteLine("Flushing Saskia on-disk dictionary...");
						finalFlush.DisposeAsync().AsTask().Wait();
						Console.WriteLine("Releasing Saskia on-disk dictionary lock...");
						File.Delete(lockfile);
					} else if(engine == "purrfectodd"){
						if(asyncDictionary is { }){
							Console.WriteLine("Flushing PurrfectODD on-disk dictionary...");
							((IFlushableAsyncDictionary)asyncDictionary).Flush().Wait();
						}
					}
					inhibitDomainExit.Set();

				end:;
				}
			};
			Console.WriteLine("Removing abort blockers...");
			exitBlocker.Set();
			coreloop.Wait();
			inhibitDomainExit.Wait();
		}
		private sealed class UnobservedTaskException : Exception{
			public UnobservedTaskException(Exception e) : base("Unobserved task exception", e){
				
			}
		}
		private static void TaskScheduler_UnobservedTaskException(object sender, UnobservedTaskExceptionEventArgs e)
		{
			throw new UnobservedTaskException(e.Exception);
		}

		private static IFlushableAsyncDictionary CreateSaskia2(ISwapHandle swapHandle){
			return new EnhancedSequentialAccessDictionary(swapHandle, true);
		}
		private static async Task Main3(HttpListener httpListener, Task load, Task<LevelDBEngine> getLevelDBEngine, IDatabaseEngine databaseEngine, AsyncReaderWriterLock exitLock, Task<HttpListenerContext> abort2)
		{
			Action disposeLevelDB = null;
			try{
				if (databaseEngine is null)
				{
					Console.WriteLine("Loading binlog...");
					LevelDBEngine levelDBEngine = await getLevelDBEngine;
					disposeLevelDB = levelDBEngine.Dispose;
					databaseEngine = levelDBEngine;
				} else if (load is { })
				{
					Console.WriteLine("Waiting for database to load...");
					await load;
				}
				Console.WriteLine("Done!");
				Task<HttpListenerContext>[] tasks = new Task<HttpListenerContext>[] {null, abort2};
			start:
				HttpListenerContext httpListenerContext;
				try
				{
					tasks[0] = httpListener.GetContextAsync();
					httpListenerContext = (await Task.WhenAny(tasks)).Result;
				}
				catch
				{
					return;
				}
				if (httpListenerContext is { })
				{
					HandleConnection(httpListenerContext, databaseEngine, exitLock);
					goto start;
				}
			} finally{
				if(disposeLevelDB is { }){
					Console.WriteLine("Closing LevelDB on-disk dictionary...");
					disposeLevelDB();
				}
			}
		}
		private static async void HandleConnection(HttpListenerContext httpListenerContext, IDatabaseEngine databaseEngine, AsyncReaderWriterLock exitLock)
		{
			HttpListenerWebSocketContext httpListenerWebSocketContext;
			try{
				httpListenerWebSocketContext = await httpListenerContext.AcceptWebSocketAsync("LesbianDB-v2.1");
			} catch (Exception e){
				httpListenerContext.Response.Abort();
				return;
			}
			if(httpListenerWebSocketContext is { }){
				Encoding encoding = httpListenerContext.Request.ContentEncoding;
				WebSocket webSocket = httpListenerWebSocketContext.WebSocket;
				AsyncReaderWriterLock disconnectionLock = new AsyncReaderWriterLock();
				AsyncMutex asyncMutex = new AsyncMutex();
				byte[] buffer = null;
				try
				{
					buffer = arrayPool.Rent(65536);
				start:
					using (PooledMemoryStream memoryStream = new PooledMemoryStream(arrayPool)){
						while (true)
						{
							WebSocketReceiveResult webSocketReceiveResult = await webSocket.ReceiveAsync(buffer, default);
							if (webSocketReceiveResult.MessageType.HasFlag(WebSocketMessageType.Close))
							{
								goto disconnect;
							}
							memoryStream.Write(buffer, 0, webSocketReceiveResult.Count);
							if (webSocketReceiveResult.EndOfMessage)
							{
								await disconnectionLock.AcquireReaderLock();
								ExecuteQuery(memoryStream.GetBuffer(), (int)memoryStream.Position, encoding, disconnectionLock.ReleaseReaderLock, asyncMutex, databaseEngine, webSocket, exitLock);
								goto start;
							}
						}
					}
				} catch{
					await disconnectionLock.AcquireWriterLock();
					try
					{
						await webSocket.CloseAsync(WebSocketCloseStatus.ProtocolError, "Socket IO error", default);
					}
					catch
					{
						
					}
				}
			disconnect:
				try{
					await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Client requested close", default);
				} catch{
					
				}
				
			}
		}
		[JsonObject(MemberSerialization.Fields)]
		private sealed class Packet
		{
			public string id;
			public IEnumerable<string> reads;
			public IReadOnlyDictionary<string, string> conditions;
			public IReadOnlyDictionary<string, string> writes;
		}
		[JsonObject(MemberSerialization.Fields)]

		private sealed class Reply
		{
			public readonly string id;
			public readonly IReadOnlyDictionary<string, string> result;

			public Reply(string id, IReadOnlyDictionary<string, string> result)
			{
				this.id = id;
				this.result = result;
			}
		}
		private static bool exitflag;
		private static async void ExecuteQuery(byte[] bytes, int count, Encoding encoding, Action releaseDisconnectionLock, AsyncMutex asyncMutex, IDatabaseEngine databaseEngine, WebSocket webSocket, AsyncReaderWriterLock exitLock)
		{
			try{
				Packet packet;
				try{
					packet = JsonConvert.DeserializeObject<Packet>(encoding.GetString(bytes, 0, count));
				} catch{
					return;
				}
				IReadOnlyDictionary<string, string> result;
				await exitLock.AcquireReaderLock();
				try{
					if(exitflag){
						return;
					}
					result = await databaseEngine.Execute(packet.reads, packet.conditions, packet.writes);
				} finally{
					exitLock.ReleaseReaderLock();
				}
				string reply = JsonConvert.SerializeObject(new Reply(packet.id, result));
				int len = encoding.GetByteCount(reply);
				bytes = null;
				try{
					bytes = arrayPool.Rent(len);
					encoding.GetBytes(reply, 0, reply.Length, bytes, 0);
					await asyncMutex.Enter();
					try
					{
						await webSocket.SendAsync(bytes.AsMemory(0, len), WebSocketMessageType.Text, true, default);
					}
					catch{
						
					}
					finally
					{
						asyncMutex.Exit();
					}
				} finally{
					if(bytes is { }){
						arrayPool.Return(bytes, false);
					}
				}
			} finally{
				releaseDisconnectionLock();
			}
		}
	}
}
