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
		private readonly AsyncMutex upgradableReader;
		public AsyncReaderWriterLock(){
			
		}
		public AsyncReaderWriterLock(bool upgradeable){
			if(upgradeable){
				upgradableReader = new AsyncMutex();
			}
		}
		private long _readerCount;

		public async Task AcquireUpgradeableReadLock(){
			await AcquireReaderLock();
			await upgradableReader.Enter();
		}
		public void ReleaseUpgradeableReadLock(){
			upgradableReader.Exit();
			ReleaseReaderLock();
		}
		public async Task UpgradeToWriteLock(){
			await writer.Enter();
			if(Interlocked.Decrement(ref _readerCount) > 0){
				await reader.Enter();
			}
		}
		public void FullReleaseUpgradedLock()
		{
			upgradableReader.Exit();
			ReleaseWriterLock();
		}

		public async Task AcquireWriterLock()
		{
			await writer.Enter();
			await reader.Enter();
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
				await reader.Enter();
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
	}
}
