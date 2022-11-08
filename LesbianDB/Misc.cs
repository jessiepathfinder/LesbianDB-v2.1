using System;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Runtime.InteropServices;
using System.Buffers;
using System.IO;

namespace LesbianDB
{
	public static class Misc
	{
		public static readonly Task completed = Task.CompletedTask;
		internal static ArrayPool<byte> arrayPool = ArrayPool<byte>.Create();
		public static string tmpdir = Path.GetTempPath();
		public static string GetRandomFileName()
		{
			Span<byte> bytes = stackalloc byte[32];
			RandomNumberGenerator.Fill(bytes);
			return Path.Combine(tmpdir, Convert.ToBase64String(bytes, Base64FormattingOptions.None).Replace('/', '='));
		}

		[ThreadStatic] private static ThreadStaticHelper threadStaticHelper;
		private static ThreadStaticHelper GetThreadStaticHelper(){
			if(threadStaticHelper is null){
				threadStaticHelper = new ThreadStaticHelper();
			}
			return threadStaticHelper;
		}
		public static void SecureHash384(ReadOnlySpan<byte> src, Span<byte> dst){
			if(dst.Length < 48){
				throw new IndexOutOfRangeException("destination span must be at least 48 bytes long for sha384 hash");
			}
			GetThreadStaticHelper().sha384.TryComputeHash(src, dst, out _);
		}

		private sealed class ThreadStaticHelper{
			public readonly SHA384 sha384 = SHA384.Create();
			public ThreadStaticHelper(){
				GC.SuppressFinalize(sha384);
			}
			~ThreadStaticHelper(){
				sha384.Dispose();
			}
		}
	}
}
