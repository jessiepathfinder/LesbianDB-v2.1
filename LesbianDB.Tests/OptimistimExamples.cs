using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using LesbianDB.Optimism.Core;
using System.IO;

namespace LesbianDB.Tests
{
	public static class OptimistimExamples
	{
		public static async void Example2(){
			
			//Connect to database server @ localhost:12345
			using RemoteDatabaseEngine databaseEngine = new RemoteDatabaseEngine(new Uri("ws://localhost:12345"));

			//Create optimistic execution manager
			OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(databaseEngine, 16777216);

			//Execute optimistic function
			await optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				optimisticExecutionScope.Write("Lesbians.AreCute", "true");
				optimisticExecutionScope.Write("jessielesbian.IsLesbian", "true");
				return false;
			});

			//Execute optimistic function
			await optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				optimisticExecutionScope.Write("counter", (Convert.ToUInt64(await optimisticExecutionScope.Read("counter")) + 1).ToString());
				return false;
			});

		}


		private static readonly OptimisticExecutionManager optimisticExecutionManager = new OptimisticExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), 0);

		//Here's an example optimistic function that increments a counter by 1 each time it's called.
		//optimistic functions are guaranteed to be ACID-compilant with optimistic locking serializable reads
		//and heavily optimized the use of optimistic caches
		public static Task ExampleIncrementOptimisticCounter(){
			return optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				string read = await optimisticExecutionScope.Read("optimistic_counter");

				optimisticExecutionScope.Write("optimistic_counter", (Convert.ToUInt64(read) + 1).ToString());
				return false;
			});
		}

		public static Task BadOptimisticSideEffect(){
			return optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				//Optimistic functions should not have any side effects other than writing to the OptimisticExecutionScope
				//So this should not be done, since optimistic locking may revert and restart the transaction
				//If the optimistic read cache conflicts with the underlying database
				await File.WriteAllTextAsync("thefile", "Jessie Lesbian is cute!");
				return false;
			});
		}

		public static async Task BadLeakOptimisticExecutionScope()
		{
			IOptimisticExecutionScope leakedOptimisticExecutionScope = null;
			await optimisticExecutionManager.ExecuteOptimisticFunction((IOptimisticExecutionScope optimisticExecutionScope) => {
				//The optimistic execution scope should not be leaked outside of optimistic functions
				leakedOptimisticExecutionScope = optimisticExecutionScope;
				return Task.FromResult(false);
			});

			//Use of OptimisticExecutionScopes outside of optimistic functions is EXTREMELY DANGEROUS
			//and can lead to unpredictable side effects!!!

			//For example, this "write" never touches the database
			leakedOptimisticExecutionScope.Write("jessielesbian.iscute", "true");
		}
		public static Task BadHeavyOperationInsideOptimisticFunction(){
			return optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				//Heavy operations like this should be moved outside of optimistic functions whenever possible!
				string text = await File.ReadAllTextAsync("thefile");
				optimisticExecutionScope.Write("thefile", text);
				return false;
			});
		}
		public static async Task GoodHeavyOperationOutsideOptimisticFunction(){
			//Here, we read the text file outside of the optimistic function.
			//Which is a good performance optimization!
			string text = await File.ReadAllTextAsync("thefile");

			await optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				optimisticExecutionScope.Write("thefile", text);
				return false;
			});
		}
		public static Task CatchRethrowOptimisticFault(){
			return optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				try{
					//Volatile reads may throw spurrious optimistic faults causing the optimistic execution manager
					//to revert and restart the transaction
					await optimisticExecutionScope.VolatileRead(new string[]{"jessielesbian.iscute", "LesbianDB.MadeByLGBTProgrammers"});
				} catch (OptimisticFault){
					//Catching OptimisticFaults without rethrowing can confuse the Optimistic Execution Scope
					//and lead to undefined behavior!!!


					//Legal example 1: rethrow the OptimisticFault
					throw;

					//Legal example 2: throw a diffrent exception (and don't catch)
					throw new Exception();
				} catch(Exception e){
					//If you use a catch-anything block like this
					//you must rethrow any OptimisticFaults
					if(e is OptimisticFault){
						throw;
					}
				}
				return false;
			});
		}
		public static Task BadLeakOptimisticValue()
		{
			return optimisticExecutionManager.ExecuteOptimisticFunction(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				//Values read inside optimistic functions should not be leaked until the optimistic function returns!!!
				//Since optimistically read values are not guaranteed to be the latest value in the database
				//and may be any previously-set values
				Console.WriteLine("Jessie Lesbian has " + await optimisticExecutionScope.Read("jessielesbian.bitcoins") + " bitcoins");
				return false;
			});
		}
		public static async Task GoodReturnOptimisticValue()
		{
			string balance = await optimisticExecutionManager.ExecuteOptimisticFunction((IOptimisticExecutionScope optimisticExecutionScope) => {
				return optimisticExecutionScope.Read("jessielesbian.bitcoins");
			});

			//We MUST wait until the optimistic function returns before we can use anything read by the optimistic function!
			Console.WriteLine("Jessie Lesbian has " + balance + " bitcoins");
		}
	}
}
