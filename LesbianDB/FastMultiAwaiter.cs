using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LesbianDB
{
	public sealed class FastMultiAwaiter : ICriticalNotifyCompletion
	{
		public FastMultiAwaiter(Task[] tasks){
			count = tasks.Length;
			this.tasks = tasks;
			foreach(Task task in tasks){
				if(task.IsCompleted){
					task.GetAwaiter().GetResult();
					Interlocked.Increment(ref counter);
				} else{
					task.GetAwaiter().OnCompleted(TaskCompleted);
				}
			}
		}
		private void TaskCompleted(){
			int tempcounter = counter;
		start:
			if(tempcounter == count){
				throw new Exception("All tasks already completed (should not reach here)");
			} else{
				int newcounter = tempcounter + 1;
				int oldcounter = Interlocked.CompareExchange(ref counter, newcounter, tempcounter);
				if(oldcounter == tempcounter){
					if (newcounter == count)
					{
						while (actions.TryTake(out Action action))
						{
							action();
						}
					}
					return;
				}
				tempcounter = oldcounter;
				goto start;
				
			}
		}
		private volatile int counter;
		private readonly int count;
		private readonly ConcurrentBag<Action> actions = new ConcurrentBag<Action>();
		private readonly Task[] tasks;
		public bool IsCompleted => count == counter;
		public void GetResult()
		{
			foreach (Task task in tasks)
			{
				if (task.IsCompletedSuccessfully)
				{
					continue;
				}
				task.GetAwaiter().GetResult();
			}
		}
		public void OnCompleted(Action continuation)
		{
			actions.Add(continuation);
			if(counter == count){
				while (actions.TryTake(out Action action))
				{
					action();
				}
			}
		}

		public void UnsafeOnCompleted(Action continuation)
		{
			OnCompleted(continuation);
		}
	}
}
