using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace LesbianDB
{
	/// <summary>
	/// A thread pool used when synchronous io is inevitable/we must use Task.Wait() in an async method
	/// </summary>
	public static class UltraHeavyThreadPool
	{
		private static readonly ConcurrentBag<BlockingCollection<Action>> threadPool = new ConcurrentBag<BlockingCollection<Action>>();
		private static volatile int count;
		private static void ExecutorThread(object obj){
			BlockingCollection<Action> blocingCollection2 = (BlockingCollection<Action>)obj;
			while (true)
			{
				blocingCollection2.Take()();
				threadPool.Add(blocingCollection2);
			}
		}
		public static void Execute(Action action){
			if (!threadPool.TryTake(out BlockingCollection<Action> blockingCollection))
			{
				blockingCollection = new BlockingCollection<Action>();
				Thread thread = new Thread(ExecutorThread);
				thread.IsBackground = true;
				thread.Name = "Ultra heavy worker thread #" + Interlocked.Increment(ref count);
				thread.Priority = ThreadPriority.Lowest;
				thread.Start(blockingCollection);
			}
			blockingCollection.Add(action);
		}
	}
	/// <summary>
	/// An awaitable class that allows us to execute parts of an async method
	/// using the ultra heavy thread pool
	/// 
	/// Used when Task.Wait becomes inevitable inside an async method
	/// </summary>
	public sealed class UltraHeavyThreadPoolAwaitable : ICriticalNotifyCompletion
	{
		public static readonly UltraHeavyThreadPoolAwaitable instance = new UltraHeavyThreadPoolAwaitable();
		private UltraHeavyThreadPoolAwaitable(){
			
		}
		public UltraHeavyThreadPoolAwaitable GetAwaiter(){
			return instance;
		}

		public void OnCompleted(Action continuation)
		{
			UltraHeavyThreadPool.Execute(continuation);
		}

		public void UnsafeOnCompleted(Action continuation)
		{
			UltraHeavyThreadPool.Execute(continuation);
		}
		public bool IsCompleted => false;
		public void GetResult(){
			
		}
	}
}
