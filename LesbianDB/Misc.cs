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

namespace LesbianDB
{
	public static class DefaultResult<T>{
		public static readonly Task<T> instance = Task.FromResult<T>(default);
	}
	public static class Misc
	{
		internal static readonly Process thisProcess = Process.GetCurrentProcess();
		public static void DieAsap(){
			Marshal.WriteByte(IntPtr.Zero, 1);
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
		public static int HashString2(string str){
			str = str.Length + "-LXDB_FASTHASH-" + str;
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
		public static async Task AtomicFileRewrite(string filename, ReadOnlyMemory<byte> newContent)
		{
			string tempfile = GetRandomFileName();
			await using Stream str = new FileStream(tempfile, FileMode.CreateNew, FileAccess.Write, FileShare.Read, 4096, FileOptions.SequentialScan | FileOptions.Asynchronous | FileOptions.DeleteOnClose);
			await str.WriteAsync(newContent);
			File.Replace(tempfile, filename, null);
		}
	}
}
