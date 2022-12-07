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
		private sealed class LockQueueNode{
			public LockQueueNode prev;
			public readonly Action<bool> release;

			public LockQueueNode(Action<bool> release)
			{
				this.release = release;
			}
		}
		private static readonly LockQueueNode[] starting = new LockQueueNode[1];
		private volatile LockQueueNode[] lockQueueHead;


		/// <summary>
		/// Returns a task that completes once we have entered the lock
		/// </summary>
		public Task Enter(){
			LockQueueNode newLockQueueNode = null;
			LockQueueNode[] newLockQueueHead = null;
			TaskCompletionSource<bool> taskCompletionSource = null;
		start:
			LockQueueNode[] old = lockQueueHead;
			if(old is null){
				if(Interlocked.CompareExchange(ref lockQueueHead, starting, null) is null){
					return Misc.completed;
				}
			} else{
				if(newLockQueueNode is null){
					newLockQueueNode = new LockQueueNode(taskCompletionSource.SetResult);
					newLockQueueHead = new LockQueueNode[1];
					newLockQueueHead[0] = newLockQueueNode;
					taskCompletionSource = new TaskCompletionSource<bool>();
				}
				newLockQueueNode.prev = old[0];
				if(ReferenceEquals(Interlocked.CompareExchange(ref lockQueueHead, newLockQueueHead, old), old)){
					return taskCompletionSource.Task;
				}
			}
			goto start;
		}
		public void Exit(){
			LockQueueNode[] newLockQueueHead = null;
		start:
			LockQueueNode[] old = lockQueueHead;
			if (old is null){
				throw new InvalidOperationException("Attempted to exit unlocked mutex!");
			}
			LockQueueNode first = old[0];
			if (first is null)
			{
				if (ReferenceEquals(Interlocked.CompareExchange(ref lockQueueHead, null, old), old))
				{
					return;
				}
			}
			else{
				if (newLockQueueHead is null)
				{
					newLockQueueHead = new LockQueueNode[1];
				}
				newLockQueueHead[0] = first.prev;
				if (ReferenceEquals(Interlocked.CompareExchange(ref lockQueueHead, newLockQueueHead, old), old))
				{
					first.release(false);
					return;
				}
			}
			goto start;
			
			

		}
	}
}
