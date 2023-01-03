using System;
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

namespace LesbianDB
{
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
		[ThreadStatic] private static Random random;
		public static int FastRandom(int from, int to){
			Random temp = random;
			if (temp is null){
				goto initialize;
			}
			//1/256 chance of reseeding for every random number generated
			if(temp.Next() < 8388608)
			{
				goto initialize;
			}
			goto finish;
		initialize:
			Span<int> seed = stackalloc int[1];
			RandomNumberGenerator.Fill(MemoryMarshal.AsBytes(seed));
			temp = new Random(seed[0]);
			random = temp;
		finish:
			return temp.Next(from, to);
		}
	}
	public sealed class ObjectDamagedException : Exception
	{
		public ObjectDamagedException(Exception innerException) : base("The object is damaged by an unexpected exception", innerException)
		{
		}
	}
}
