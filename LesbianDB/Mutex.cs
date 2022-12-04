using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Buffers;

namespace LesbianDB
{

	/// <summary>
	/// Truly nonblocking async mutexes
	/// </summary>
	public sealed class AsyncMutex {
		private sealed class LockQueueHead{
			public LockQueueNode first;
			public LockQueueHead(){
				
			}
			public static readonly LockQueueHead starting = new LockQueueHead();
		}
		private sealed class LockQueueNode{
			public LockQueueNode prev;
			public Action<bool> release;
		}
		private volatile LockQueueHead lockQueueHead;


		/// <summary>
		/// Returns a task that completes once we have entered the lock
		/// </summary>
		public Task Enter(){
			LockQueueNode newLockQueueNode = null;
			LockQueueHead newLockQueueHead = null;
			TaskCompletionSource<bool> taskCompletionSource = null;
		start:
			LockQueueHead old = lockQueueHead;
			if(old is null){
				if(Interlocked.CompareExchange(ref lockQueueHead, LockQueueHead.starting, null) is null){
					return Misc.completed;
				}
			} else{
				if(newLockQueueNode is null){
					newLockQueueNode = new LockQueueNode();
					newLockQueueHead = new LockQueueHead();
					newLockQueueHead.first = newLockQueueNode;
					taskCompletionSource = new TaskCompletionSource<bool>();
				}
				newLockQueueNode.prev = old.first;
				newLockQueueNode.release = taskCompletionSource.SetResult;
				if(ReferenceEquals(Interlocked.CompareExchange(ref lockQueueHead, newLockQueueHead, old), old)){
					return taskCompletionSource.Task;
				}
			}
			goto start;
		}
		public void Exit(){
			LockQueueHead newLockQueueHead = null;
		start:
			LockQueueHead old = lockQueueHead;
			if (old is null){
				throw new InvalidOperationException("Attempted to exit unlocked mutex!");
			}
			if (old.first is null)
			{
				if (ReferenceEquals(Interlocked.CompareExchange(ref lockQueueHead, null, old), old))
				{
					return;
				}
			}
			else{
				if (newLockQueueHead is null)
				{
					newLockQueueHead = new LockQueueHead();
				}
				newLockQueueHead.first = old.first.prev;
				if (ReferenceEquals(Interlocked.CompareExchange(ref lockQueueHead, newLockQueueHead, old), old))
				{
					return;
				}
			}
			goto start;
			
			

		}
	}
}
