using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

using LesbianDB;
using LesbianDB.IntelliEX;
using LesbianDB.Optimism.Core;
using LesbianDB.Optimism.YuriTables;
using NUnit.Framework;

namespace LesbianDB.Tests
{
	public sealed class YuriTablesExamples
	{
		[SetUp]
		public void Setup()
		{

		}
		[Test]
		public async Task RunExamples(){
			//Quickly create an in-memory database
			IntelligentExecutionManager intelligentExecutionManager = new IntelligentExecutionManager(new YuriDatabaseEngine(new EnhancedSequentialAccessDictionary()), "lockctr");

			//Run the examples
			await intelligentExecutionManager.ExecuteOptimisticFunction(CreateTableExample);
			await intelligentExecutionManager.ExecuteOptimisticFunction(InsertTableExample);
			Console.WriteLine(await intelligentExecutionManager.ExecuteOptimisticFunction(SelectTablePrimaryKeyExample));
			Console.WriteLine(await intelligentExecutionManager.ExecuteOptimisticFunction(SelectTableSortedIntExample1));
			Console.WriteLine(await intelligentExecutionManager.ExecuteOptimisticFunction(SelectTableSortedIntExample2));
		}

		private static Task<bool> CreateTableExample(IOptimisticExecutionScope optimisticExecutionScope){
			//Create table
			return optimisticExecutionScope.TryCreateTable(new Dictionary<string, ColumnType>() {
				{"name", ColumnType.UniqueString},
				{"height", ColumnType.SortedInt},
				{"carrer", ColumnType.DataString}
			}, "lesbians");
		}

		private static Task InsertTableImpl(IOptimisticExecutionScope optimisticExecutionScope, string name, string carrer, BigInteger height){
			return optimisticExecutionScope.TableInsert("lesbians", new Dictionary<string, string>() {
				{"name", name},
				{"carrer", carrer}
			}, new Dictionary<string, BigInteger>() {
				{"height", height}
			});
		}
		private static async Task<bool> InsertTableExample(IOptimisticExecutionScope optimisticExecutionScope)
		{
			await InsertTableImpl(optimisticExecutionScope, "jessielesbian", "programmer", 160);
			await InsertTableImpl(optimisticExecutionScope, "minecraft_alex", "minecraft player", 200);
			await InsertTableImpl(optimisticExecutionScope, "kellyanne", "elf archer", 170);
			return false;
		}
		private static async Task<string> SelectTablePrimaryKeyExample(IOptimisticExecutionScope optimisticExecutionScope){
			Row row = await optimisticExecutionScope.TableTrySelectPrimaryKey("lesbians", "name", "jessielesbian");

			//NOTE: Using Console.WriteLine or any side effects other than writing to the Optimistic Execution Scope
			//inside optimistic functions is an anti-pattern, since optimistic functions may be executed multiple times
			//without committing due to optimistic locking. We return it to the caller who writes it instead.
			if (row is null)
			{
				return "Jessie Lesbian does not exist (should not reach here)!";
			}
			else{
				return "Jessie Lesbian is a " + row["carrer"];
			}
		}
		private static async Task<string> SelectTableSortedIntExample1(IOptimisticExecutionScope optimisticExecutionScope){
			await foreach(Row row in optimisticExecutionScope.TableSelectAllOrderedBy("lesbians", "height", true)){
				return "Our tallest lesbian is " + row["name"] + ", who is " + row["height"] + " centimeters tall";
			}
			return "The table is empty!";
		}
		private static async Task<string> SelectTableSortedIntExample2(IOptimisticExecutionScope optimisticExecutionScope){
			await foreach(Row row in optimisticExecutionScope.TableTrySelectSortedRow("lesbians", "height", CompareOperator.LessThan, 165, false)){
				return row["name"] + " is shorter than 165 centimeters";
			}
			return "No one is shorter than 165 centimeters";
		}
	}
}
