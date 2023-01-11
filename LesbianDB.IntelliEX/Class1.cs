using LesbianDB.Optimism.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace LesbianDB.IntelliEX
{
	public sealed class IntelligentExecutionManager : IOptimisticExecutionManager
	{
		private static readonly IReadOnlyDictionary<string, string> emptyDictionary = new Dictionary<string, string>();
		private readonly IDatabaseEngine databaseEngine1;
		private readonly string counterName;
		private readonly string[] counterNameArr;

		public IntelligentExecutionManager(IDatabaseEngine databaseEngine1, string counterName)
		{
			this.databaseEngine1 = databaseEngine1 ?? throw new ArgumentNullException(nameof(databaseEngine1));
			this.counterName = counterName ?? throw new ArgumentNullException(nameof(counterName));
			counterNameArr = new string[] { counterName };
		}

		private sealed class IntelligentOptimisticExecutionScope : ISnapshotReadScope
		{
			public readonly ConcurrentDictionary<string, string> writes = new ConcurrentDictionary<string, string>();
			public readonly ConcurrentDictionary<string, string> reads = new ConcurrentDictionary<string, string>();
			public readonly IDatabaseEngine databaseEngine;
			private readonly BigInteger id;

			public IntelligentOptimisticExecutionScope(IDatabaseEngine databaseEngine, BigInteger id)
			{
				this.databaseEngine = databaseEngine;
				this.id = id;
			}

			public async Task<string> Read(string key)
			{
				int split;
				if(writes.TryGetValue(key, out string val1)){
					return val1;
				}
				if(reads.TryGetValue(key, out string value)){
					if (value is null){
						return null;
					}
					split = value.IndexOf('_');
					goto doneread;
				}
				value = (await databaseEngine.Execute(new string[] { key }, emptyDictionary, emptyDictionary))[key];
				if(value is { }){
					if (ParseWhatever(value, out _) > id)
					{
						throw new OptimisticFault(); //Revert the transaction since the database became inconsistent
					}
				}
				value = reads.GetOrAdd(key, value);
				if (value is null){
					return null;
				}
				split = value.IndexOf('_');



			doneread:
				if (writes.TryGetValue(key, out val1))
				{
					return val1;
				}
				return split > 0 ? value[(split + 1)..] : null;
			}
			private static BigInteger ParseWhatever(string str, out int val){
				val = str.IndexOf('_');
				if(val > 0){
					return BigInteger.Parse(str.AsSpan(0, val), NumberStyles.None);
				} else{
					return BigInteger.Parse(str, NumberStyles.None);
				}
			}

			public async Task<IReadOnlyDictionary<string, string>> VolatileRead(IEnumerable<string> keys)
			{
				Dictionary<string, bool> missing = new Dictionary<string, bool>();
				Dictionary<string, string> results = new Dictionary<string, string>();
				bool morework = false;
				foreach(string key in keys){
					if(writes.TryGetValue(key, out string value)){
						results.TryAdd(key, value);
						continue;
					}
					if (reads.TryGetValue(key, out value))
					{
						if (writes.TryGetValue(key, out string value1))
						{
							results.TryAdd(key, value1);
						} else{
							int split = value.IndexOf('_');
							results.TryAdd(key, split > 0 ? value[(split + 1)..] : null);
						}
						continue;
					}
					morework |= missing.TryAdd(key, false);
				}
				if(morework){
					foreach(KeyValuePair<string, string> keyValuePair in await databaseEngine.Execute(missing.Keys, emptyDictionary, emptyDictionary)){
						string key = keyValuePair.Key;
						string value = keyValuePair.Value;
						if(value is { }){
							if (ParseWhatever(value, out int split) > id)
							{
								throw new OptimisticFault();
							}
						}
						value = reads.GetOrAdd(key, value);
						if(writes.TryGetValue(key, out string val2)){
							results.Add(key, val2);
						} else{
							if (value is null)
							{
								results.Add(key, null);
							}
							else{
								int split = value.IndexOf('_');
								results.Add(key, split > 0 ? value[(split + 1)..] : null);
							}
						}
					}
				}
				return results;
			}

			public void Write(string key, string value)
			{
				writes[key] = value;
			}
		}
		private static readonly BigInteger one = BigInteger.One;
		private static readonly BigInteger zero = BigInteger.Zero;
		public async Task<T> ExecuteOptimisticFunction<T>(Func<IOptimisticExecutionScope, Task<T>> optimisticFunction)
		{
			string read = (await databaseEngine1.Execute(counterNameArr, emptyDictionary, emptyDictionary))[counterName];
			BigInteger id;
			while (true){
				if(read is null){
					id = one;
				} else{
					id = BigInteger.Parse(read, NumberStyles.None);
				}
				string read1 = (await databaseEngine1.Execute(counterNameArr, new Dictionary<string, string>() {
				{counterName, read}},
				new Dictionary<string, string>(){
					{counterName, (id + one).ToString()}
				}))[counterName];
				if(read1 == read){
					break;
				}
				read = read1;
			}
			string appended;
			if(read is null){
				read = "0";
				appended = "0_";
			} else{
				appended = read + '_';
			}
			IReadOnlyDictionary<string, string> repopulate = null;
		start:
			IntelligentOptimisticExecutionScope scope = new IntelligentOptimisticExecutionScope(databaseEngine1, id);
			if (repopulate is { })
			{
				foreach (KeyValuePair<string, string> keyValuePair in repopulate)
				{
					scope.reads.TryAdd(keyValuePair.Key, keyValuePair.Value);
				}
			}
			T result = default;
			Exception exception = null;
			try
			{
				result = await optimisticFunction(scope);
			}
			catch (Exception e)
			{
				exception = e;
			}
			Dictionary<string, string> reads = new Dictionary<string, string>(scope.reads.ToArray());
			Dictionary<string, string> writes = new Dictionary<string, string>();
			if(exception is null){
				foreach(KeyValuePair<string, string> keyValuePair in scope.writes.ToArray()){
					string value = keyValuePair.Value;
					if (value is null) {
						value = read;
					} else{
						value = appended + value;
					}
					writes.Add(keyValuePair.Key, value);
				}
			}
			repopulate = await databaseEngine1.Execute(reads.Keys, writes.Count == 0 ? emptyDictionary : reads, writes);
			foreach (KeyValuePair<string, string> keyValuePair1 in repopulate)
			{
				if (reads[keyValuePair1.Key] == keyValuePair1.Value)
				{
					continue;
				}
				goto start;
			}
			if (exception is null)
			{
				return result;
			}
			ExceptionDispatchInfo.Throw(exception);
			throw exception;
		}
	}
}
