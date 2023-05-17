using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Buffers;
using System.Collections.Concurrent;

namespace LesbianDB
{

	/// <summary>
	/// Truly nonblocking async mutexes
	/// </summary>
	public sealed class AsyncMutex {

		private volatile int locked;
		private readonly ConcurrentBag<TaskCompletionSource<bool>> queue = new ConcurrentBag<TaskCompletionSource<bool>>();

		private void Release2(){
			if(Interlocked.Exchange(ref locked, 1) == 0){
				if(queue.TryTake(out TaskCompletionSource<bool> taskCompletionSource)){
					taskCompletionSource.SetResult(false);
					return;
				}
				locked = 0;
			}
		}

		/// <summary>
		/// Returns a task that completes once we have entered the lock
		/// </summary>
		public Task Enter(){
			if(Interlocked.Exchange(ref locked, 1) == 0){
				return Misc.completed;
			}
			TaskCompletionSource<bool> taskCompletionSource = new TaskCompletionSource<bool>();
			queue.Add(taskCompletionSource);
			Release2();
			return taskCompletionSource.Task;
		}
		public void Exit(){
			if (queue.TryTake(out TaskCompletionSource<bool> taskCompletionSource))
			{
				taskCompletionSource.SetResult(false);
				return;
			}
			locked = 0;
			Release2();
		}
	}
}
