using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LesbianDB
{
	public sealed class AsyncManagedSemaphore
	{
		private sealed class QueueItem{
			public QueueItem prev;
			public readonly Action<bool> release;

			public QueueItem(Action<bool> release)
			{
				this.release = release;
			}
		}
		private sealed class QueueHead{
			public QueueItem head;
			public ulong count;
		}

		private volatile QueueHead queueHead = new QueueHead();

		public AsyncManagedSemaphore(ulong initialCount)
		{
			queueHead.count = initialCount;
		}

		public Task Enter(){
			QueueHead replace = new QueueHead();
			TaskCompletionSource<bool> taskCompletionSource = null;
			QueueItem queueItem = null;
		start:
			QueueHead temp = queueHead;
			Task tsk;
			if(temp.count == 0){
				replace.count = 0;
				if(queueItem is null){
					taskCompletionSource = new TaskCompletionSource<bool>();
					queueItem = new QueueItem(taskCompletionSource.SetResult);
				}
				queueItem.prev = temp.head;
				replace.head = queueItem;
				tsk = taskCompletionSource.Task;
			} else{
				replace.count = temp.count - 1;
				replace.head = null;
				tsk = Misc.completed;
			}
			if(ReferenceEquals(Interlocked.CompareExchange(ref queueHead, replace, temp), temp)){
				return tsk;
			}
			goto start;
		}
		public void Exit(){
			QueueHead altered = new QueueHead();
		start:
			QueueHead temp = queueHead;
			Action<bool> release;
			if (temp.head is null)
			{
				altered.count = temp.count + 1;
				altered.head = null;
				release = null;
			}
			else{
				altered.head = temp.head.prev;
				altered.count = 0;
				release = temp.head.release;
			}
			if (ReferenceEquals(Interlocked.CompareExchange(ref queueHead, altered, temp), temp))
			{
				if(release is { }){
					release(false);
				}
				return;
			}
			goto start;

		}
	}
}
