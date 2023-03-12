using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LesbianDB.Optimism.Core;

namespace LesbianDB.Optimism.Armitage
{
	public sealed class SpeculativeExecutionManager<T>
	{
		private readonly Func<string, IOptimisticExecutionScope, Task<T>> func;
		private readonly IOptimisticExecutionManager optimisticExecutionManager;

		private sealed class SpeculativeExecutionResult{
			private readonly Task<T> task;
			private readonly TaskCompletionSource<bool> taskCompletionSource = new TaskCompletionSource<bool>();

			public SpeculativeExecutionResult(IOptimisticExecutionManager optimisticExecutionManager, string input, Func<string, IOptimisticExecutionScope, Task<T>> func, Task lastrun)
			{
				Task signal = taskCompletionSource.Task;
				Task waitOne = Task.WhenAny(signal, lastrun);
				task = optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
					try
					{
						await waitOne;
					}
					catch
					{

					}
					//Received cancellation signal
					if(signal.IsFaulted){
						await signal;
					}
					T result;
					try{
						result = await func(input, optimisticExecutionScope);
					} catch{
						await signal;
						throw;
					}
					await signal;
					return result;
				});
				Misc.IgnoreException(task);
			}

			private volatile int completed;
			public void Dispose(){
				if (Interlocked.Exchange(ref completed, 1) == 0)
				{
					GC.SuppressFinalize(this);
					taskCompletionSource.SetException(new Exception("Dead speculative branch unexpectedly resurrected (should not reach here)"));
				}
			}
			~SpeculativeExecutionResult(){
				if (Interlocked.Exchange(ref completed, 1) == 0)
				{
					taskCompletionSource.SetException(new Exception("Dead speculative branch unexpectedly resurrected (should not reach here)"));
				}
			}
			public Task<T> GetTask(){
				if(Interlocked.Exchange(ref completed, 1) == 0){
					taskCompletionSource.SetResult(false);
				}
				return task;
			}
		}

		private readonly ConcurrentDictionary<string, SpeculativeExecutionResult>[] cache = new ConcurrentDictionary<string, SpeculativeExecutionResult>[256];

		public SpeculativeExecutionManager(Func<string, IOptimisticExecutionScope, Task<T>> func, OptimisticExecutionManager optimisticExecutionManager, long softMemoryLimit)
		{
			this.func = func ?? throw new ArgumentNullException(nameof(func));
			this.optimisticExecutionManager = optimisticExecutionManager ?? throw new ArgumentNullException(nameof(func));
			for(int i = 0; i < 256; ){
				cache[i++] = new ConcurrentDictionary<string, SpeculativeExecutionResult>();
			}
			Collect(new WeakReference<ConcurrentDictionary<string, SpeculativeExecutionResult>[]>(cache, false), softMemoryLimit);
		}
		private static async void Collect(WeakReference<ConcurrentDictionary<string, SpeculativeExecutionResult>[]> weakReference, long softMemoryLimit){
			AsyncManagedSemaphore asyncManagedSemaphore = new AsyncManagedSemaphore(0);
			Misc.RegisterGCListenerSemaphore(asyncManagedSemaphore);
		start:
			await asyncManagedSemaphore.Enter();
			if (weakReference.TryGetTarget(out ConcurrentDictionary<string, SpeculativeExecutionResult>[] cache)){
				if(Misc.thisProcess.VirtualMemorySize64 > softMemoryLimit){
					cache[Misc.FastRandom(0, 256)].Clear();
				}
				goto start;
			}
		}
		public Task<T> Call(string input){
			ConcurrentDictionary<string, SpeculativeExecutionResult> keyValuePairs = cache[("Optimistic Armitage" + input).GetHashCode() & 255];
			Task<T> task;
			if (keyValuePairs.TryRemove(input, out SpeculativeExecutionResult result)){
				task = result.GetTask();
			} else{
				task = optimisticExecutionManager.ExecuteOptimisticFunction((IOptimisticExecutionScope optimisticExecutionScope) => func(input, optimisticExecutionScope));
			}
			result = new SpeculativeExecutionResult(optimisticExecutionManager, input, func, task);
			if (keyValuePairs.TryAdd(input, result))
			{
				return task;
			}
			result.Dispose();
			return task;
		}
	}
}
