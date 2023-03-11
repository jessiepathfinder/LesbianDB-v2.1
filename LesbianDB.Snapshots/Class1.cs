using LesbianDB.Optimism.Core;
using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Text;
using LesbianDB.Optimism.YuriTables;
using System.Globalization;
using System.Numerics;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;

namespace LesbianDB.Optimism.Snapshot
{
	public static class SnapshottingExtension
	{
		public static async Task<string> SnapshotRead(this IOptimisticExecutionScope optimisticExecutionScope, string name, string key){
			string str = (await optimisticExecutionScope.ArrayTryGetValue(new StringBuilder(name).Append('_').Append(key).Append('_').ToString(), true)).value;
			if(str is null){
				return null;
			}
			int index = str.IndexOf('_');
			if(index < 1){
				return null;
			}
			return str[(index + 1)..];
		}
		private static ulong SnapshotParse(string str, out int split){
			if(str is null){
				split = 0;
				return 0;
			}
			split = str.IndexOf('_');
			if (split > 0)
			{
				return ulong.Parse(str.AsSpan(0, split), NumberStyles.None);
			}
			return ulong.Parse(str, NumberStyles.None);
		}
		private static string SnapshotSplit(string str, int split){
			if (split > 0)
			{
				return str[(split + 1)..];
			}
			else{
				return null;
			}
		}
		private static readonly BigInteger one = BigInteger.One;
		private static readonly BigInteger two = one + one;
		private static readonly BigInteger zero = BigInteger.Zero;
		public static async Task<string> SnapshotRead(this IOptimisticExecutionScope optimisticExecutionScope, string name, string key, ulong id)
		{
			StringBuilder stringBuilder = new StringBuilder(name);
			Task<string> task = optimisticExecutionScope.Read(stringBuilder.Append('_').ToString());
			string prefix = id.ToString() + '_';
			key = stringBuilder.Append(key).Append('_').ToString();
			if (id > Convert.ToUInt64(await task)){
				//If we wrongfully get here, we revert and try again
				throw new InvalidOperationException("this snapshot does not exist yet");
			}
			BigInteger bigInteger = await optimisticExecutionScope.ArrayGetLength(key);
			if(bigInteger.IsZero){
				return null;
			}
			BigInteger high = bigInteger;
			BigInteger low = zero;
		start:
			string read = null;
			int split = 0;
			//Ported from OpenZeppelin
			while (low < high)
			{
				BigInteger mid = (low + high) / two;

				read = await optimisticExecutionScope.ArrayGetValue(key, mid);
				split = read.IndexOf('_');
				if ((split > 0 ? ulong.Parse(read.AsSpan(0, split), NumberStyles.None) : ulong.Parse(read)) > id)
				{
					high = mid;
				}
				else
				{
					low = mid + 1;
				}
			}
			// At this point `low` is the exclusive upper bound. We will return the inclusive upper bound.
			Task<string> task3 = optimisticExecutionScope.ArrayGetValue(key, low - one);
			BigInteger subtracted = low - one;
			if (subtracted.Sign > 0)
			{
				string read3 = await task3;
				int split3 = read3.IndexOf('_');
				ulong val3;
				if(split3 > 0){
					val3 = ulong.Parse(read3.AsSpan(0, split3), NumberStyles.None);
				} else{
					val3 = ulong.Parse(read3, NumberStyles.None);
				}
				if(val3 == id){
					return SnapshotSplit(read3, split3);
				}
			}
			return SnapshotSplit(read, split);

			
		}
		private static string FormatSnapshotWrite(string read, string value){
			if (read is null)
			{
				return value is null ? "0" : ("0_" + value);
			}
			else{
				return value is null ? read : new StringBuilder(read).Append('_').Append(value).ToString();
			}
		}
		public static async Task<ulong> IncrementSnapshotCounter(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			name += '_';
			string read = await optimisticExecutionScope.Read(name);
			if(read is null){
				optimisticExecutionScope.Write(name, "1");
				return 0;
			}
			ulong value = ulong.Parse(read, NumberStyles.None);
			optimisticExecutionScope.Write(name, (value + 1).ToString());
			return value;
		}
		public static async Task SnapshotWrite(this IOptimisticExecutionScope optimisticExecutionScope, string name, string key, string value)
		{
			StringBuilder stringBuilder = new StringBuilder(name).Append('_');
			name = stringBuilder.ToString();
			Task<string> task = optimisticExecutionScope.Read(name);
			key = stringBuilder.Append(key).Append('_').ToString();
			Task<ReadResult> task1 = optimisticExecutionScope.ArrayTryGetValue(key, true);
			string read = await task;
			ulong head = Convert.ToUInt64(read);
			string read1 = (await task1).value;
			if(read1 is null){
				goto end;
			}
			int split = read1.IndexOf('_');
			ulong val2;
			if (split > 0)
			{
				val2 = ulong.Parse(read1.AsSpan(0, split), NumberStyles.None);
			}
			else
			{
				val2 = Convert.ToUInt64(read1);
			}
			if(head == val2)
			{
				await optimisticExecutionScope.ArraySetValue(key, (await optimisticExecutionScope.ArrayGetLength(key)) - one, FormatSnapshotWrite(read, value));
				return;
			} else if(head < val2)
			{
				throw new Exception("Snapshot height higher than head (should not reach here)");
			}
		end:
			await optimisticExecutionScope.ArrayPushEnd(key, FormatSnapshotWrite(read, value));


		}
	}

	public sealed class SnapshotConsistentExecutioner : IOptimisticExecutionManager
	{
		private readonly string name;
		private readonly IOptimisticExecutionManager optimisticExecutionManager;

		public SnapshotConsistentExecutioner(string name, IOptimisticExecutionManager optimisticExecutionManager)
		{
			this.name = name ?? throw new ArgumentNullException(nameof(name));
			this.optimisticExecutionManager = optimisticExecutionManager ?? throw new ArgumentNullException(nameof(optimisticExecutionManager));
		}
		private Task<ulong> IncrementSnapshotCounter(IOptimisticExecutionScope optimisticExecutionScope){
			return optimisticExecutionScope.IncrementSnapshotCounter(name);
		}
		private sealed class SnapshotConsistentScope : ISnapshotReadScope{
			private readonly ulong id;
			private readonly string name;
			private readonly IOptimisticExecutionScope optimisticExecutionScope;
			public readonly ConcurrentDictionary<string, string> readCache = new ConcurrentDictionary<string, string>();
			public readonly ConcurrentDictionary<string, string> writeCache = new ConcurrentDictionary<string, string>();

			public SnapshotConsistentScope(ulong id, string name, IOptimisticExecutionScope optimisticExecutionScope)
			{
				this.id = id;
				this.name = name;
				this.optimisticExecutionScope = optimisticExecutionScope ?? throw new ArgumentNullException(nameof(optimisticExecutionScope));
			}

			public async Task<string> Read(string key)
			{
				if(!readCache.TryGetValue(key, out string value)){
					value = readCache.GetOrAdd(key, await optimisticExecutionScope.SnapshotRead(name, key, id));
				}
				if(writeCache.TryGetValue(key, out string value1)){
					return value1;
				}
				return value;
				
				
			}

			public async Task<IReadOnlyDictionary<string, string>> VolatileRead(IEnumerable<string> keys)
			{
				Dictionary<string, string> dict = new Dictionary<string, string>();
				foreach(string key in keys){
					dict.TryAdd(key, await Read(key));
				}
				return dict;
			}

			public void Write(string key, string value)
			{
				writeCache[key] = value;
			}

			public Task Safepoint()
			{
				return Misc.completed;
			}
		}
		public async Task<T> ExecuteOptimisticFunction<T>(Func<IOptimisticExecutionScope, Task<T>> optimisticFunction)
		{
			ulong id = await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementSnapshotCounter);
			return await optimisticExecutionManager.ExecuteOptimisticFunction<T>(async (IOptimisticExecutionScope optimisticExecutionScope) => {
				SnapshotConsistentScope snapshotConsistentScope = new SnapshotConsistentScope(id, name, optimisticExecutionScope);
				Exception exception = null;
				T result = default;
				try{
					result = await optimisticFunction(snapshotConsistentScope);
				} catch(Exception e){
					exception = e;
				}
				foreach(KeyValuePair<string, string> keyValuePair in snapshotConsistentScope.readCache.ToArray())
				{
					if((await optimisticExecutionScope.SnapshotRead(name, keyValuePair.Key)) == keyValuePair.Value){
						continue;
					}

					//This is an anti-pattern, but it's the most efficent way to deoptimize
					id = await optimisticExecutionManager.ExecuteOptimisticFunction(IncrementSnapshotCounter);
					throw new OptimisticFault();
				}
				foreach(KeyValuePair<string, string> keyValuePair1 in snapshotConsistentScope.writeCache.ToArray()){
					//Instant completion expected, since shit is already cached
					await optimisticExecutionScope.SnapshotWrite(name, keyValuePair1.Key, keyValuePair1.Value);
				}

				if (exception is null){
					return result;
				}
				ExceptionDispatchInfo.Throw(exception);
				throw exception;
			});
		}
	}
}
