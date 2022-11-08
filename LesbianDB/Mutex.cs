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
	public sealed class AsyncMutex{
		private byte locked;
		private readonly Queue<Action<bool>> queue = new Queue<Action<bool>>();
		private readonly object locker = new object();


		/// <summary>
		/// Returns a task that completes once we have entered the lock
		/// </summary>
		public Task Enter(){
			//If the lock is available, we return on the spot
			lock (locker)
			{
				if (locked == 0)
				{
					locked = 1;
					return Misc.completed;
				}
				else
				{
					//Add us to the queue of awaiters
					TaskCompletionSource<bool> taskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.None);
					queue.Enqueue(taskCompletionSource.SetResult);
					return taskCompletionSource.Task;
				}
			}
		}
		public void Exit(){
			ThreadPool.QueueUserWorkItem(ReleaseImpl, this, true);
		}
		private static void ReleaseImpl(AsyncMutex _this){
			lock (_this.locker)
			{
				if (_this.locked == 0)
				{
					throw new InvalidOperationException("Mutex already unlocked");
				}
				else
				{
					if (_this.queue.TryDequeue(out Action<bool> next))
					{
						//Hand over lock
						next(false);
					}
					else
					{
						//Release lock
						_this.locked = 0;
					}
				}
			}
		}
	}
}
