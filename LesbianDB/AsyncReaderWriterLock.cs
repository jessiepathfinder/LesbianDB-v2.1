using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LesbianDB
{
	//Stolen from StackOverflow user xtadex, augmented by Jessie Lesbian
	public sealed class AsyncReaderWriterLock
	{
		private readonly AsyncMutex reader = new AsyncMutex();
		private readonly AsyncMutex writer = new AsyncMutex();
		private long _readerCount;

		public async Task AcquireWriterLock()
		{
			await writer.Enter();
			await SafeAcquireReadSemaphore();
		}
		private volatile int pendingLockUpgrade;

		public async Task<bool> TryUpgradeToWriterLock()
		{
			if (Interlocked.Exchange(ref pendingLockUpgrade, 1) == 0)
			{
				try
				{
					await writer.Enter();
					if (Interlocked.Decrement(ref _readerCount) > 0)
					{
						try
						{
							await SafeAcquireReadSemaphore();
						}
						catch
						{
							Interlocked.Increment(ref _readerCount);
							throw;
						}
					}
				}
				finally
				{
					pendingLockUpgrade = 0;
				}
				return true;
			}
			else
			{
				return false;
			}

		}

		public void ReleaseWriterLock()
		{
			reader.Exit();
			writer.Exit();
		}

		public async Task AcquireReaderLock()
		{
			await writer.Enter();
			if (Interlocked.Increment(ref _readerCount) == 1)
			{
				try
				{
					await SafeAcquireReadSemaphore();
				}
				catch
				{
					Interlocked.Decrement(ref _readerCount);

					throw;
				}
			}
			writer.Exit();
		}

		public void ReleaseReaderLock()
		{
			if (Interlocked.Decrement(ref _readerCount) == 0)
			{
				reader.Exit();
			}
		}

		private async Task SafeAcquireReadSemaphore()
		{
			try
			{
				await reader.Enter();
			}
			catch
			{
				writer.Exit();
				throw;
			}
		}
	}
}
