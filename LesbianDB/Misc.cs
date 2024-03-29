﻿using System;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Runtime.InteropServices;
using System.Buffers;
using System.IO;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Threading;
using System.Text;
using System.Buffers.Binary;
using System.Collections.Generic;
using Newtonsoft.Json;
using LesbianDB.Optimism.Core;
using System.Numerics;
using System.Diagnostics.CodeAnalysis;
using System.Collections;
using System.Runtime.CompilerServices;

namespace LesbianDB
{
	public sealed class SafeEmptyReadOnlyDictionary<K, V> : IReadOnlyDictionary<K, V>
	{
		public static readonly SafeEmptyReadOnlyDictionary<K, V> instance = new SafeEmptyReadOnlyDictionary<K, V>();
		private SafeEmptyReadOnlyDictionary(){
			
		}
		private static readonly K[] emptyKeys = new K[0];
		private static readonly V[] emptyValues = new V[0];
		private sealed class EmptyEnumerator<T> : IEnumerator<T>
		{
			private EmptyEnumerator(){
				
			}
			public static readonly EmptyEnumerator<T> instance = new EmptyEnumerator<T>();
			public T Current => default;

			object IEnumerator.Current => null;

			public void Dispose()
			{
				
			}

			public bool MoveNext()
			{
				return false;
			}

			public void Reset()
			{
				
			}
		}
		public V this[K key] => throw new NotImplementedException();

		public IEnumerable<K> Keys => emptyKeys;

		public IEnumerable<V> Values => emptyValues;

		public int Count => 0;

		public bool ContainsKey(K key)
		{
			return false;
		}

		public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
		{
			return EmptyEnumerator<KeyValuePair<K, V>>.instance;
		}

		public bool TryGetValue(K key, [MaybeNullWhen(false)] out V value)
		{
			value = default;
			return false;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return EmptyEnumerator<object>.instance;
		}
	}
	public static class DefaultResult<T>{
		public static readonly Task<T> instance = Task.FromResult<T>(default);
	}
	public static class Misc
	{
		public static async void IgnoreException(Task task)
		{
			try
			{
				await task;
			}
			catch
			{

			}
		}
		internal static readonly Process thisProcess = Process.GetCurrentProcess();
		public static void DieAsap(){
			Marshal.WriteByte(IntPtr.Zero, 1);
		}
		public static T DeserializeObjectWithFastCreate<T>(string json) where T : new(){
			T obj = new T();
			JsonConvert.PopulateObject(json, obj);
			return obj;
		}
		public static readonly Task completed = Task.CompletedTask;
		internal static readonly ArrayPool<byte> arrayPool = ArrayPool<byte>.Create();
		public static readonly string tmpdir = Path.GetTempPath();
		public static readonly string[] emptyStringArray = new string[0];
		public static string GetRandomFileName()
		{
			Span<byte> bytes = stackalloc byte[32];
			RandomNumberGenerator.Fill(bytes);
			return Path.Combine(tmpdir, Convert.ToBase64String(bytes, Base64FormattingOptions.None).Replace('/', '='));
		}
		private sealed class PendingHeavyTask<T>{
			public readonly TaskCompletionSource<T> taskCompletionSource;
			public readonly Func<T> func;

			public PendingHeavyTask(TaskCompletionSource<T> a, Func<T> b)
			{
				taskCompletionSource = a;
				func = b;
			}
		}
		private static void ExecuteAsyncImpl<T>(PendingHeavyTask<T> pendingHeavyTask){
			T res;
			try{
				res = pendingHeavyTask.func();
			} catch(Exception e){
				pendingHeavyTask.taskCompletionSource.SetException(e);
				return;
			}
			pendingHeavyTask.taskCompletionSource.SetResult(res);
		}
		public static async void BackgroundAwait(Task tsk){
			await tsk;
		}
		/// <summary>
		/// Used by LevelDB/Yuri database (NOT storage) engines to scrub away no-effect writes
		/// </summary>
		/// <param name="writes"></param>
		/// <returns></returns>
		public static IReadOnlyDictionary<string, string> ScrubNoEffectWrites(IReadOnlyDictionary<string, string> writes, IReadOnlyDictionary<string, Task<string>> reads){
			Dictionary<string, string> rewritten = new Dictionary<string, string>();
			foreach(KeyValuePair<string, string> keyValuePair in writes){
				string key = keyValuePair.Key;
				string value = keyValuePair.Value;
				if(reads.TryGetValue(key, out Task<string> tsk)){
					//Normally, this function is called on already completed tasks
					if(tsk.Result == value){
						continue;
					}
				}
				rewritten.Add(key, value);
			}
			return rewritten;
		}
		public static async void BackgroundAwait(ValueTask tsk)
		{
			await tsk;
		}
		public static uint HashString3(string str)
		{
			int len = Encoding.UTF8.GetByteCount(str);
			if (len > 1008)
			{
				byte[] bytes = null;
				try
				{
					bytes = arrayPool.Rent(len);
					Encoding.UTF8.GetBytes(str, 0, str.Length, bytes, 0);
					byte[] output;
					using (MD5 md5 = MD5.Create())
					{
						output = md5.ComputeHash(bytes, 0, len);
					}
					return BinaryPrimitives.ReadUInt32BigEndian(output.AsSpan(0, 4));
				}
				finally
				{
					if (bytes is { })
					{
						arrayPool.Return(bytes, false);
					}
				}
			}
			else
			{
				Span<byte> bytes = stackalloc byte[len + 16];
				Encoding.UTF8.GetBytes(str, bytes);
				Span<byte> output = bytes[len..];
				using (MD5 md5 = MD5.Create())
				{
					md5.TryComputeHash(bytes.Slice(0, len), output, out _);
				}
				return BinaryPrimitives.ReadUInt32LittleEndian(output.Slice(0, 4));
			}
		}
		public static int HashString2(string str){
			int len = Encoding.UTF8.GetByteCount(str);
			if(len > 1008){
				byte[] bytes = null;
				try{
					bytes = arrayPool.Rent(len);
					Encoding.UTF8.GetBytes(str, 0, str.Length, bytes, 0);
					byte[] output;
					using(MD5 md5 = MD5.Create()){
						output = md5.ComputeHash(bytes, 0, len);
					}
					return BinaryPrimitives.ReadInt32BigEndian(output.AsSpan(0, 4));
				} finally{
					if(bytes is { }){
						arrayPool.Return(bytes, false);
					}
				}
			} else{
				Span<byte> bytes = stackalloc byte[len + 16];
				Encoding.UTF8.GetBytes(str, bytes);
				Span<byte> output = bytes[len..];
				using (MD5 md5 = MD5.Create())
				{
					md5.TryComputeHash(bytes.Slice(0, len), output, out _);
				}
				return BinaryPrimitives.ReadInt32BigEndian(output);
			}
		}
		public static void CheckDamage(Exception e){
			if(e is { }){
				throw new ObjectDamagedException(e);
			}
		}
		public static ulong HashString4(string str){
			Span<ulong> span = stackalloc ulong[2];
			using(MD5 md5 = MD5.Create()){
				md5.TryComputeHash(MemoryMarshal.AsBytes(str.AsSpan()), MemoryMarshal.AsBytes(span), out _);
			}
			return span[0] ^ span[1];
		}
		public static long HashString5(string str)
		{
			Span<long> span = stackalloc long[2];
			using (MD5 md5 = MD5.Create())
			{
				md5.TryComputeHash(MemoryMarshal.AsBytes(str.AsSpan()), MemoryMarshal.AsBytes(span), out _);
			}
			return span[0] ^ span[1];
		}
		public static async Task AtomicFileRewrite(string filename, ReadOnlyMemory<byte> newContent)
		{
			string tempfile = GetRandomFileName();
			await using Stream str = new FileStream(tempfile, FileMode.CreateNew, FileAccess.Write, FileShare.Read, 4096, FileOptions.SequentialScan | FileOptions.Asynchronous | FileOptions.DeleteOnClose);
			await str.WriteAsync(newContent);
			File.Replace(tempfile, filename, null);
		}
		public static IEnumerable<T> JoinEnumerables<T>(IEnumerable<T> first, IEnumerable<T> second){
			foreach(T temp in first){
				yield return temp;
			}
			foreach(T temp in second){
				yield return temp;
			}
		}
		private static readonly YuriHash yuriHash = YuriHash.GetRandom();
		private static long seed;

		public static ulong FastRandomUlong(){
			Span<long> span = stackalloc long[1];
			Span<ulong> span2 = MemoryMarshal.Cast<long, ulong>(span);
			long current = seed;
		start:

			span[0] = current;
			ulong result = yuriHash.Permute(span2[0]);
			span2[0] = result;

			long x2 = Interlocked.CompareExchange(ref seed, span[0], current);
			if(x2 == current){
				return result;
			}
			current = x2;
			goto start;

		}


		public static int FastRandom(int from, int to){
			long lfrom = from;
			long range = to - lfrom;
			if(range < 1){
				return 0;
			}
			return (int)((long)(FastRandomUlong() % (ulong)range) + lfrom);
		}
		public static string GetChildKey(string parent, string key)
		{
			int len = parent.Length;
			Span<byte> hash = stackalloc byte[48];
			int longlen = (len + key.Length + 2) * 2;
			using (PooledMemoryStream pooledMemoryStream = new PooledMemoryStream(arrayPool, longlen)){
				Span<byte> sliced = hash.Slice(0, 4);
				BinaryPrimitives.WriteInt32BigEndian(sliced, len);
				pooledMemoryStream.Write(sliced);
				pooledMemoryStream.Write(MemoryMarshal.AsBytes(parent.AsSpan()));
				pooledMemoryStream.Write(MemoryMarshal.AsBytes(key.AsSpan()));
				using SHA384 sha384 = SHA384.Create();
				sha384.TryComputeHash(pooledMemoryStream.GetBuffer().AsSpan(0, longlen), hash, out _);
			}

			return Convert.ToBase64String(hash.Slice(0, 32), Base64FormattingOptions.None);
		}
		public static FastMultiAwaiter GetAwaiter(this Task[] tasks){
			return new FastMultiAwaiter(tasks);
		}

		private static readonly ConcurrentBag<TaskCompletionSource<bool>> GCTaskCompletionSources = new ConcurrentBag<TaskCompletionSource<bool>>();
		public static Task WaitForNextGC(){
			TaskCompletionSource<bool> taskCompletionSource = new TaskCompletionSource<bool>();
			GCTaskCompletionSources.Add(taskCompletionSource);
			return taskCompletionSource.Task;
		}
		private static void GCMonitorThread(){
			AppDomain domain = AppDomain.CurrentDomain;
			while (true){
				if (domain.IsFinalizingForUnload())
				{
					return;
				}
				GC.WaitForPendingFinalizers();
				if (domain.IsFinalizingForUnload())
				{
					return;
				}
				while (GCTaskCompletionSources.TryTake(out TaskCompletionSource<bool> taskCompletionSource)){
					taskCompletionSource.SetResult(false);
				}
			}
		}
		static Misc(){
			if(BitConverter.IsLittleEndian){
				Span<long> span = stackalloc long[1];
				RandomNumberGenerator.Fill(MemoryMarshal.AsBytes(span));
				seed = span[0];

				Thread thread = new Thread(GCMonitorThread);
				thread.Name = "LesbianDB GC monitoring thread";
				thread.IsBackground = true;
				thread.Start();
			} else{
				throw new InvalidOperationException("LesbianDB does not properly support big-endian systems");
			}
			
		}
	}
	public sealed class ObjectDamagedException : Exception
	{
		public ObjectDamagedException(Exception innerException) : base("The object is damaged by an unexpected exception", innerException)
		{
		}
	}
}
