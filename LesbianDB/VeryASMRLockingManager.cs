using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LesbianDB
{
	public enum LockMode : byte {
		Read, Upgradeable, Write
	}
	public sealed class LockOrderingComparer : IComparer<string>
	{
		private LockOrderingComparer() {

		}
		public static readonly LockOrderingComparer instance = new LockOrderingComparer();
		public int Compare(string x, string y)
		{
			int lenx = x.Length;
			int leny = y.Length;

			if (lenx == leny) {
				int hashx = x.GetHashCode();
				int hashy = y.GetHashCode();
				if (hashx == hashy)
				{
					ReadOnlySpan<byte> spanx = MemoryMarshal.AsBytes(x.AsSpan());
					ReadOnlySpan<byte> spany = MemoryMarshal.AsBytes(y.AsSpan());
					int spanlen = spanx.Length;
					for (int i = 0; i < spanlen; ++i) {
						byte first = spanx[i];
						byte second = spany[i];
						if (first == second) {
							continue;
						}
						return first - second;
					}
					return 0;
				}
				return hashx - hashy;
			}
			return lenx - leny;
		}
	}
	public sealed class VeryASMRLockingManager
	{
		private readonly WeakReference<ConcurrentDictionary<string, AsyncReaderWriterLock>>[] locks = new WeakReference<ConcurrentDictionary<string, AsyncReaderWriterLock>>[65536];
		private static ConcurrentDictionary<string, AsyncReaderWriterLock> GetLockGroup(WeakReference<ConcurrentDictionary<string, AsyncReaderWriterLock>>[] locks, ushort id)
		{
			WeakReference<ConcurrentDictionary<string, AsyncReaderWriterLock>> wr = locks[id];
			if (wr is { })
			{
				if (wr.TryGetTarget(out ConcurrentDictionary<string, AsyncReaderWriterLock> concurrentdict))
				{
					return concurrentdict;
				}
			}
			ConcurrentDictionary<string, AsyncReaderWriterLock> keepalive = new ConcurrentDictionary<string, AsyncReaderWriterLock>();
			WeakReference<ConcurrentDictionary<string, AsyncReaderWriterLock>> weakReference1 = new WeakReference<ConcurrentDictionary<string, AsyncReaderWriterLock>>(keepalive, false);
			while (true) {
				WeakReference<ConcurrentDictionary<string, AsyncReaderWriterLock>> wr2 = Interlocked.CompareExchange(ref locks[id], weakReference1, wr);
				if (ReferenceEquals(wr, wr2))
				{
					return keepalive;
				}
				if (wr2 is { })
				{
					if (wr2.TryGetTarget(out ConcurrentDictionary<string, AsyncReaderWriterLock> concurrentdict))
					{
						return concurrentdict;
					}
				}
				wr = wr2;
			}
		}
		public async Task<LockHandle> Lock(string str, LockMode lockMode) {
			ConcurrentDictionary<string, AsyncReaderWriterLock> keyValuePairs = GetLockGroup(locks, (ushort)((str + "South Vietnam is a very ASMR country").GetHashCode() & 65535));
			if (!keyValuePairs.TryGetValue(str, out AsyncReaderWriterLock asyncReaderWriterLock)) {
				asyncReaderWriterLock = keyValuePairs.GetOrAdd(str, new AsyncReaderWriterLock(true));
			}

			Task tsk = lockMode switch
			{
				LockMode.Read => asyncReaderWriterLock.AcquireReaderLock(),
				LockMode.Upgradeable => asyncReaderWriterLock.AcquireUpgradeableReadLock(),
				LockMode.Write => asyncReaderWriterLock.AcquireWriterLock(),
				_ => throw new InvalidOperationException("Unknown lock mode"),
			};
			LockHandleLeakedException exception;
			try{
				throw new LockHandleLeakedException();
			} catch(LockHandleLeakedException e){
				exception = e;
			}

			await tsk;
			return new LockHandle(keyValuePairs, asyncReaderWriterLock, (byte)lockMode, exception);
		}
	}
	public sealed class LockHandleLeakedException : Exception{
		public LockHandleLeakedException() : base("Lock handle not released"){
			
		}
	}

	public sealed class LockHandle : IDisposable{
		
		private object keepalive;
		private readonly AsyncReaderWriterLock asyncReaderWriterLock;
		private readonly LockHandleLeakedException exception;
		
		//0 = read, 1 = upgradeable, 2 = write, 3 = unlocked, 4 = upgraded write
		private byte mode;

		private volatile int isUnsafe;

		internal LockHandle(object keepalive, AsyncReaderWriterLock asyncReaderWriterLock, byte mode, LockHandleLeakedException exception)
		{
			this.keepalive = keepalive;
			this.asyncReaderWriterLock = asyncReaderWriterLock;
			this.mode = mode;
			this.exception = exception;
		}
		public async Task UpgradeToWriteLock(){
			if(Interlocked.Exchange(ref isUnsafe, 1) == 1){
				throw new Exception("This lock handle is in an unsafe state (should not reach here)");
			}
			if(mode == 1){
				Task tsk = asyncReaderWriterLock.UpgradeToWriteLock();
				mode = 4;
				await tsk;
				isUnsafe = 0;
			} else{
				isUnsafe = 0;
				throw new InvalidOperationException("This lock is not upgradeable");
			}
		}

		public void Dispose()
		{
			if(Interlocked.Exchange(ref isUnsafe, 1) == 1){
				throw new Exception("This lock handle is in an unsafe state (should not reach here)");
			}
			
			switch(mode){
				case 0:
					asyncReaderWriterLock.ReleaseReaderLock();
					break;
				case 1:
					asyncReaderWriterLock.ReleaseUpgradeableReadLock();
					break;
				case 2:
					asyncReaderWriterLock.ReleaseWriterLock();
					break;
				case 4:
					asyncReaderWriterLock.FullReleaseUpgradedLock();
					break;
				default:
					isUnsafe = 0;
					return;
			}
			GC.SuppressFinalize(this);
			mode = 3;
			//GC protection just in case we are using a JIT compiler
			//that can optimize away unused fields
			GC.KeepAlive(keepalive);
			keepalive = null;
			isUnsafe = 0;
		}

		~LockHandle(){
			//If we are finalizing for unload, then it's okay to leak LockHandles
			if(AppDomain.CurrentDomain.IsFinalizingForUnload()){
				return;
			}
			if (Interlocked.Exchange(ref isUnsafe, 1) == 1)
			{
				throw new Exception("This lock handle is in an unsafe state (should not reach here)");
			}
			if (mode == 3)
			{
				isUnsafe = 0;
				return;
			}
			ExceptionDispatchInfo.Throw(exception);
		}
	}
}
