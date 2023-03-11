using System;
using System.Collections.Generic;
using System.Text;
using System.Security.Cryptography;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using LesbianDB.Optimism.Core;
using System.Numerics;
using System.Globalization;
using System.Threading;
using System.Collections.Concurrent;
using Newtonsoft.Json;

namespace LesbianDB.Optimism.YuriTables.NextGeneration
{
	public static class Extensions
	{
		private static async Task EnsureYuriTablesType(this IOptimisticExecutionScope optimisticExecutionScope, string key, string type)
		{
			string actual = await optimisticExecutionScope.Read(Misc.GetChildKey(key, "YuriTablesNGType"));
			if (type == actual)
			{
				return;
			}
			if (actual is null)
			{
				throw new InvalidOperationException("YuriTables Next Generation object does not exists: " + key);
			}
			throw new InvalidOperationException(new StringBuilder("YuriTables Next Generation type mismatch: ").Append(key).Append("(Expected: ").Append(type).Append(", Actual: ").Append(actual).Append(')').ToString());
		}
		private static async Task<bool> TryAssignYuriTablesType(this IOptimisticExecutionScope optimisticExecutionScope, string key, string type)
		{
			string childkey = Misc.GetChildKey(key, "YuriTablesNGType");
			if (await optimisticExecutionScope.Read(childkey) is null)
			{
				optimisticExecutionScope.Write(childkey, type);
				return true;
			}
			return false;
		}
		public static async Task<bool> TryCreateArray(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			if (await optimisticExecutionScope.TryAssignYuriTablesType(name, "array"))
			{
				optimisticExecutionScope.Write(Misc.GetChildKey(name, "bounds"), "0_0");
				return true;
			}
			return false;
		}
		public static async Task CreateArray(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			if (await optimisticExecutionScope.TryCreateArray(name))
			{
				return;
			}
			throw new InvalidOperationException("YuriTables Next Generation object already exists: " + name);
		}
		public static async Task<ArrayHandle> OpenArray(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			await optimisticExecutionScope.EnsureYuriTablesType(name, "array");
			return new ArrayHandle(optimisticExecutionScope, name);
		}
		public static Task<bool> TryCreateBTree(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			return optimisticExecutionScope.TryAssignYuriTablesType(name, "btree");
		}
		public static async Task CreateBTree(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			if (await optimisticExecutionScope.TryCreateBTree(name))
			{
				return;
			}
			throw new InvalidOperationException("YuriTables Next Generation object already exists: " + name);
		}
		public static async Task<BTreeHandle> OpenBTree(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			Task check = optimisticExecutionScope.EnsureYuriTablesType(name, "btree");
			string childname = Misc.GetChildKey(name, "btree");
			await check;
			return new BTreeHandle(childname, optimisticExecutionScope);
		}
		public static async Task<bool> TryCreateHash(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			if (await optimisticExecutionScope.TryAssignYuriTablesType(name, "hash"))
			{
				await optimisticExecutionScope.CreateArray(Misc.GetChildKey(name, "array"));
				return true;
			}
			return false;
		}
		public static async Task CreateHash(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			if (await optimisticExecutionScope.TryCreateHash(name))
			{
				return;
			}
			throw new InvalidOperationException("YuriTables Next Generation object already exists: " + name);
		}
		public static async Task<HashHandle> OpenHash(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			Task check = optimisticExecutionScope.EnsureYuriTablesType(name, "hash");
			Task<ArrayHandle> getArray = optimisticExecutionScope.OpenArray(Misc.GetChildKey(name, "array"));
			string dictionary = Misc.GetChildKey(name, "dictionary");
			await check;
			return new HashHandle(optimisticExecutionScope, await getArray, dictionary);
		}
		public static async Task<bool> TryCreateLargeHash(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			if (await optimisticExecutionScope.TryAssignYuriTablesType(name, "largehash"))
			{
				await optimisticExecutionScope.CreateBTree(Misc.GetChildKey(name, "btree"));
				return true;
			}
			return false;
		}
		public static async Task CreateLargeHash(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			if (await optimisticExecutionScope.TryCreateLargeHash(name))
			{
				return;
			}
			throw new InvalidOperationException("YuriTables Next Generation object already exists: " + name);
		}
		public static async Task<LargeHashHandle> OpenLargeHash(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			Task check = optimisticExecutionScope.EnsureYuriTablesType(name, "largehash");
			Task<BTreeHandle> getBTree = optimisticExecutionScope.OpenBTree(Misc.GetChildKey(name, "btree"));
			await check;
			return new LargeHashHandle(optimisticExecutionScope, await getBTree, name);
		}
		public static async Task<bool> TryCreateSortedSet(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			if (await optimisticExecutionScope.TryAssignYuriTablesType(name, "zset"))
			{
				await optimisticExecutionScope.CreateBTree(Misc.GetChildKey(name, "btree"));
				return true;
			}
			return false;
		}
		public static async Task CreateSortedSet(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			if (await optimisticExecutionScope.TryCreateSortedSet(name))
			{
				return;
			}
			throw new InvalidOperationException("YuriTables Next Generation object already exists: " + name);
		}
		public static async Task<SortedSetHandle> OpenSortedSet(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			Task check = optimisticExecutionScope.EnsureYuriTablesType(name, "zset");
			Task<BTreeHandle> getBTree = optimisticExecutionScope.OpenBTree(Misc.GetChildKey(name, "btree"));
			string dictionary = Misc.GetChildKey(name, "dictionary");
			await check;
			return new SortedSetHandle(await getBTree, name, dictionary, optimisticExecutionScope);
		}
		public static async Task<bool> TryCreateNGTable(this IOptimisticExecutionScope optimisticExecutionScope, IReadOnlyDictionary<string, ColumnType> keyValuePairs, string name)
		{
			Task<bool> check = optimisticExecutionScope.TryAssignYuriTablesType(name, "table");
			foreach (ColumnType columnType in keyValuePairs.Values)
			{
				if (columnType == ColumnType.SortedInt | columnType == ColumnType.UniqueString)
				{
					goto create;
				}
			}
			throw new InvalidOperationException("Attempted to create write-only table");
		create:
			string columnsKey = Misc.GetChildKey(name, "columns");
			string columnsJson = JsonConvert.SerializeObject(keyValuePairs);
			string bTreeRoot = Misc.GetChildKey(name, "zsets");
			Queue<string> createzsets = new Queue<string>();
			foreach(KeyValuePair<string, ColumnType> keyValuePair in keyValuePairs){
				if(keyValuePair.Value == ColumnType.SortedInt){
					createzsets.Enqueue(Misc.GetChildKey(bTreeRoot, keyValuePair.Key));
				}
			}
			if (await check)
			{
				int x = createzsets.Count;
				Task[] tasks = x == 0 ? null : new Task[x];
				while(createzsets.TryDequeue(out string zname)){
					tasks[createzsets.Count] = optimisticExecutionScope.CreateSortedSet(zname);
				}
				optimisticExecutionScope.Write(name, JsonConvert.SerializeObject(keyValuePairs));
				optimisticExecutionScope.Write(columnsKey, columnsJson);
				await tasks;
				return true;
			}
			return false;
		}
		public static async Task CreateNGTable(this IOptimisticExecutionScope optimisticExecutionScope, IReadOnlyDictionary<string, ColumnType> keyValuePairs, string name){
			if(await optimisticExecutionScope.TryCreateNGTable(keyValuePairs, name)){
				return;
			}
			throw new InvalidOperationException("YuriTables Next Generation object already exists: " + name);
		}
		public static async Task<TableHandle> OpenNGTable(this IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			Task check = optimisticExecutionScope.EnsureYuriTablesType(name, "table");
			Task<string> read = optimisticExecutionScope.Read(Misc.GetChildKey(name, "columns"));
			Dictionary<string, bool> dataColumns = new Dictionary<string, bool>();
			Dictionary<string, string> primaryKeys = new Dictionary<string, string>();
			Dictionary<string, SortedSetHandle> indexes = new Dictionary<string, SortedSetHandle>();
			Dictionary<string, ColumnType> keyValuePairs = new Dictionary<string, ColumnType>();
			string bTreeRoot = Misc.GetChildKey(name, "zsets");
			string primaryKeysRoot = Misc.GetChildKey(name, "primaryKeys");
			JsonConvert.PopulateObject(await read, keyValuePairs);
			Queue<NewKeyValuePair<string, Task<SortedSetHandle>>> queue = new Queue<NewKeyValuePair<string, Task<SortedSetHandle>>>();
			foreach (KeyValuePair<string, ColumnType> keyValuePair in keyValuePairs)
			{
				ColumnType columnType = keyValuePair.Value;
				if (columnType == ColumnType.UniqueString)
				{
					string key = keyValuePair.Key;
					primaryKeys.Add(key, Misc.GetChildKey(primaryKeysRoot, key));
					continue;
				}
				if(columnType == ColumnType.DataString){
					dataColumns.Add(keyValuePair.Key, false);
				}
			}
			await check;
			foreach (KeyValuePair<string, ColumnType> keyValuePair in keyValuePairs)
			{
				if (keyValuePair.Value == ColumnType.SortedInt)
				{
					string key = keyValuePair.Key;
					queue.Enqueue(new NewKeyValuePair<string, Task<SortedSetHandle>>(key, optimisticExecutionScope.OpenSortedSet(Misc.GetChildKey(bTreeRoot, key))));
				}
			}
			while(queue.TryDequeue(out NewKeyValuePair<string, Task<SortedSetHandle>> kvpx)){
				indexes.Add(kvpx.key, await kvpx.value);
			}
			return new TableHandle(primaryKeys, indexes, dataColumns, optimisticExecutionScope);
		}
	}
	public readonly struct BTreeHandle{
		private readonly string name;
		private readonly PseudoSnapshotReadScope pseudoSnapshotReadScope;
		private readonly IOptimisticExecutionScope optimisticExecutionScope;
		internal BTreeHandle(string name, IOptimisticExecutionScope optimisticExecutionScope){
			this.name = name;
			pseudoSnapshotReadScope = new PseudoSnapshotReadScope(optimisticExecutionScope);
			this.optimisticExecutionScope = optimisticExecutionScope;
		}
		public IAsyncEnumerable<BigInteger> SelectAll(SafepointController safepointController, bool reverse){
			safepointController.EnsureScopeIs(optimisticExecutionScope);
			SelectPseudoSnapshotReadScope selectPseudoSnapshotReadScope = new SelectPseudoSnapshotReadScope(optimisticExecutionScope);
			return EnableSafepointing(selectPseudoSnapshotReadScope, safepointController, selectPseudoSnapshotReadScope.BTreeSelectAll(name, reverse));
		}
		public IAsyncEnumerable<BigInteger> Select(SafepointController safepointController, BigInteger bigInteger, CompareOperator compareOperator, bool reverse){
			safepointController.EnsureScopeIs(optimisticExecutionScope);
			if (compareOperator == CompareOperator.EqualTo)
			{
				return pseudoSnapshotReadScope.BTreeSelect(name, CompareOperator.EqualTo, false, bigInteger);
			}
			else{
				SelectPseudoSnapshotReadScope selectPseudoSnapshotReadScope = new SelectPseudoSnapshotReadScope(optimisticExecutionScope);
				return EnableSafepointing(selectPseudoSnapshotReadScope, safepointController, selectPseudoSnapshotReadScope.BTreeSelect(name, compareOperator, reverse, bigInteger));
			}
		}
		private static async IAsyncEnumerable<BigInteger> EnableSafepointing(SelectPseudoSnapshotReadScope selectPseudoSnapshotReadScope, SafepointController safepointController, IAsyncEnumerable<BigInteger> bigIntegers)
		{
			await foreach(BigInteger bigInteger in bigIntegers){
				if(Interlocked.Exchange(ref selectPseudoSnapshotReadScope.shouldTrySafepoint, 0) == 1){
					await safepointController.SafepointIfNeeded();
				}
				yield return bigInteger;
			}
		}
		public Task<bool> TryInsert(BigInteger bigInteger){
			return pseudoSnapshotReadScope.BTreeTryInsert(name, bigInteger);
		}
		public Task<bool> TryRemove(BigInteger bigInteger)
		{
			return pseudoSnapshotReadScope.BTreeTryRemove(name, bigInteger);
		}
		public async Task<bool> Contains(BigInteger bigInteger){
			await using IAsyncEnumerator<BigInteger> asyncEnumerator = pseudoSnapshotReadScope.BTreeSelect(name, CompareOperator.EqualTo, false, bigInteger).GetAsyncEnumerator();
			return await asyncEnumerator.MoveNextAsync();
		}
		public async Task<bool> NotEmpty()
		{
			await using IAsyncEnumerator<BigInteger> asyncEnumerator = pseudoSnapshotReadScope.BTreeSelectAll(name, false).GetAsyncEnumerator();
			return await asyncEnumerator.MoveNextAsync();
		}

		//HACK: Trick legacy b-tree functions into thinking that we are using a snapshot read scope
		private sealed class SelectPseudoSnapshotReadScope : ISnapshotReadScope{
			public volatile int shouldTrySafepoint;
			private readonly IOptimisticExecutionScope optimisticExecutionScope;

			public SelectPseudoSnapshotReadScope(IOptimisticExecutionScope optimisticExecutionScope)
			{
				this.optimisticExecutionScope = optimisticExecutionScope;
			}

			public Task<string> Read(string key)
			{
				if(key is null){
					throw new ArgumentNullException(nameof(key));
				}
				shouldTrySafepoint = 1;
				return optimisticExecutionScope.Read(key);
			}

			public Task<IReadOnlyDictionary<string, string>> VolatileRead(IEnumerable<string> keys)
			{
				throw new NotImplementedException();
			}

			public void Write(string key, string value)
			{
				throw new NotImplementedException();
			}

			public Task Safepoint()
			{
				throw new NotImplementedException();
			}
		}
		//HACK: Trick legacy b-tree functions into thinking that we are using a snapshot read scope
		private sealed class PseudoSnapshotReadScope : ISnapshotReadScope{
			private readonly IOptimisticExecutionScope optimisticExecutionScope;
			
			public PseudoSnapshotReadScope(IOptimisticExecutionScope optimisticExecutionScope)
			{
				this.optimisticExecutionScope = optimisticExecutionScope;
			}

			public Task<string> Read(string key)
			{
				return optimisticExecutionScope.Read(key ?? throw new ArgumentNullException(nameof(key)));
			}

			public Task Safepoint()
			{
				//Not used by legacy b-tree functions
				throw new NotImplementedException();
			}

			public Task<IReadOnlyDictionary<string, string>> VolatileRead(IEnumerable<string> keys)
			{
				//Not used by legacy b-tree functions
				throw new NotImplementedException();
			}

			public void Write(string key, string value)
			{
				optimisticExecutionScope.Write(key ?? throw new ArgumentNullException(nameof(key)), value);
			}
		}
	}
	public readonly struct HashHandle{
		private readonly IOptimisticExecutionScope optimisticExecutionScope;
		private readonly ArrayHandle arrayHandle;
		private readonly string name;

		internal HashHandle(IOptimisticExecutionScope optimisticExecutionScope, ArrayHandle arrayHandle, string name)
		{
			this.optimisticExecutionScope = optimisticExecutionScope;
			this.arrayHandle = arrayHandle;
			this.name = name;
		}
		public Task<BigInteger> Length()
		{
			return arrayHandle.Length();
		}
		public async IAsyncEnumerable<NewKeyValuePair<string, string>> ToAsyncEnumerable(SafepointController safepointController)
		{
			await foreach (string key in arrayHandle.ToAsyncEnumerable(safepointController))
			{
				yield return new NewKeyValuePair<string, string>(key, await Read(key));
			}
		}
		public async Task<string> Read(string key)
		{
			if (key is null)
			{
				throw new ArgumentNullException(nameof(key));
			}
			string str = await optimisticExecutionScope.Read(Misc.GetChildKey(name, key));
			if (str is null)
			{
				return str;
			}
			return str[(str.IndexOf('_') + 1)..];
		}
		public async Task Write(string key, string value)
		{
			if(key is null){
				throw new ArgumentNullException(nameof(key));
			}
			Task<BigInteger> getarrlen = arrayHandle.Length();
			string key2 = Misc.GetChildKey(name, key);
			string oldval = await optimisticExecutionScope.Read(key2);
			string write;
			if (oldval is null)
			{
				if (value is null)
				{
					return;
				}
				BigInteger index = await getarrlen;
				await arrayHandle.PushEnd(key);
				write = new StringBuilder(index.ToString()).Append('_').Append(value).ToString();
			}
			else
			{
				int slice = oldval.IndexOf('_');
				if (value is null)
				{
					if ((await getarrlen) == 1)
					{
						if ((await arrayHandle.TryPopEnd()).exists)
						{
							optimisticExecutionScope.Write(key2, null);
							return;
						}
						throw new Exception("Unexpected zero length keylist (should not reach here)");
					}
					BigInteger index = BigInteger.Parse(oldval.AsSpan(0, slice), NumberStyles.None);
					ReadResult readResult = await arrayHandle.TryPopEnd();
					if (readResult.exists)
					{
						if (readResult.value != oldval)
						{
							Task tsk = arrayHandle.Write(index, readResult.value);
							string movekey = Misc.GetChildKey(name, readResult.value);
							Task<string> rereadTask = optimisticExecutionScope.Read(movekey);
							StringBuilder index2 = new StringBuilder(oldval.AsSpan(0, slice).ToString()).Append('_');
							string reread = await rereadTask;
							optimisticExecutionScope.Write(movekey, index2.Append(reread.AsSpan(reread.IndexOf('_') + 1)).ToString());
							await tsk;
						}
					}
					else
					{
						throw new Exception("Unexpected zero length keylist (should not reach here)");
					}
					write = null;
				}
				else
				{
					slice += 1;
					write = new StringBuilder(oldval, 0, slice, slice + value.Length).Append(value).ToString();
				}
			}
			optimisticExecutionScope.Write(key2, write);
		}
	}
	public readonly struct ArrayHandle{
		private readonly IOptimisticExecutionScope optimisticExecutionScope;
		private readonly string name;
		private readonly string boundsKey;
		private static readonly BigInteger one = BigInteger.One;

		internal ArrayHandle(IOptimisticExecutionScope optimisticExecutionScope, string name)
		{
			this.optimisticExecutionScope = optimisticExecutionScope;
			this.name = name;
			boundsKey = Misc.GetChildKey(name, "bounds");
		}
		public async Task<BigInteger> Length(){
			string bounds = await optimisticExecutionScope.Read(boundsKey);
			int split = bounds.IndexOf('_');
			return BigInteger.Parse(bounds.AsSpan(split + 1), NumberStyles.AllowLeadingSign) - BigInteger.Parse(bounds.AsSpan(0, split), NumberStyles.AllowLeadingSign);
		}
		private static bool IsLengthZero(string bounds, int split){
			int offset = split + 1;
			if((split * 2) + 1 == bounds.Length)
			{
				for(int i = 0; i < split; ++i){
					if(bounds[i] != bounds[offset + i]){
						return false;
					}
				}
				return true;
			}
			return false;
		}
		private async Task<string> Resolve(BigInteger index){
			if(index.Sign < 0){
				throw new IndexOutOfRangeException("Negative YuriTables Next Generation array index");
			}
			string bounds = await optimisticExecutionScope.Read(boundsKey);
			int split = bounds.IndexOf('_');
			index += BigInteger.Parse(bounds.AsSpan(0, split), NumberStyles.AllowLeadingSign);
			if(index < BigInteger.Parse(bounds.AsSpan(split + 1), NumberStyles.AllowLeadingSign)){
				return Misc.GetChildKey(name, index.ToString());
			}
			throw new IndexOutOfRangeException("YuriTables Next Generation array index out of bounds");
		}
		public async Task<string> Read(BigInteger index){
			return await optimisticExecutionScope.Read(await Resolve(index));
		}
		public async Task Write(BigInteger index, string value){
			optimisticExecutionScope.Write(await Resolve(index), value);
		}
		public async Task PushStart(string value){
			string bounds = await optimisticExecutionScope.Read(boundsKey);
			int split = bounds.IndexOf('_');
			string begin = (BigInteger.Parse(bounds.AsSpan(0, split), NumberStyles.AllowLeadingSign) - one).ToString();
			optimisticExecutionScope.Write(Misc.GetChildKey(name, begin), value);
			optimisticExecutionScope.Write(boundsKey, begin + bounds[split..]);
		}
		public async Task PushEnd(string value)
		{
			string bounds = await optimisticExecutionScope.Read(boundsKey);
			int split = bounds.IndexOf('_');
			BigInteger index = BigInteger.Parse(bounds.AsSpan(split + 1), NumberStyles.AllowLeadingSign);
			optimisticExecutionScope.Write(Misc.GetChildKey(name, index.ToString()), value);
			optimisticExecutionScope.Write(boundsKey, bounds.Substring(0, split + 1) + (index + one).ToString());
		}
		public async Task<ReadResult> TryPopStart(){
			string bounds = await optimisticExecutionScope.Read(boundsKey);
			int split = bounds.IndexOf('_');
			if (IsLengthZero(bounds, split)){
				return default;
			}
			BigInteger index = BigInteger.Parse(bounds.AsSpan(0, split), NumberStyles.AllowLeadingSign);
			string strindex = index.ToString();
			string childkey = Misc.GetChildKey(name, strindex);
			Task<string> readtsk = optimisticExecutionScope.Read(childkey);
			optimisticExecutionScope.Write(boundsKey, (index + one).ToString() + bounds[split..]);
			string result = await readtsk;
			optimisticExecutionScope.Write(childkey, null);
			return new ReadResult(result);
		}
		public async Task<ReadResult> TryPopEnd()
		{
			string bounds = await optimisticExecutionScope.Read(boundsKey);
			int split = bounds.IndexOf('_');
			if (IsLengthZero(bounds, split))
			{
				return default;
			}
			string strindex = (BigInteger.Parse(bounds.AsSpan(split + 1), NumberStyles.AllowLeadingSign) - one).ToString();
			string childkey = Misc.GetChildKey(name, strindex);
			Task<string> readtsk = optimisticExecutionScope.Read(childkey);
			optimisticExecutionScope.Write(boundsKey, bounds.Substring(0, split + 1) + strindex);
			string result = await readtsk;
			optimisticExecutionScope.Write(childkey, null);
			return new ReadResult(result);
		}
		public async IAsyncEnumerable<string> ToAsyncEnumerable(SafepointController safepointController){
			safepointController.EnsureScopeIs(optimisticExecutionScope);
			string bounds = await optimisticExecutionScope.Read(boundsKey);
			int split = bounds.IndexOf('_');
			BigInteger end = BigInteger.Parse(bounds.AsSpan(split + 1), NumberStyles.AllowLeadingSign);
			for (BigInteger i = BigInteger.Parse(bounds.AsSpan(0, split), NumberStyles.AllowLeadingSign); i < end; ++i){
				await safepointController.SafepointIfNeeded();
				yield return await optimisticExecutionScope.Read(Misc.GetChildKey(name, i.ToString()));
			}
		}
	}
	public readonly struct LargeHashHandle{
		private readonly ConcurrentDictionary<uint, HashHandle> cache;
		private readonly IOptimisticExecutionScope optimisticExecutionScope;
		private readonly BTreeHandle bTreeHandle;
		private readonly string name;
		internal LargeHashHandle(IOptimisticExecutionScope optimisticExecutionScope, BTreeHandle bTreeHandle, string name)
		{
			cache = new ConcurrentDictionary<uint, HashHandle>();
			this.optimisticExecutionScope = optimisticExecutionScope;
			this.bTreeHandle = bTreeHandle;
			this.name = name;
		}
		public Task<string> Read(string key){
			if(key is null){
				throw new ArgumentNullException(nameof(key));
			}
			uint hash = Misc.HashString3(key);
			if(cache.TryGetValue(hash, out HashHandle hashHandle)){
				return hashHandle.Read(key);
			}
			return Read(key, hash);
		}
		private async Task<string> Read(string key, uint hash){
			BigInteger bigInteger = hash;
			if(await bTreeHandle.Contains(bigInteger)){
				return await cache.GetOrAdd(hash, await optimisticExecutionScope.OpenHash(Misc.GetChildKey(name, hash.ToString()))).Read(key);
			}
			return null;
		}
		public async Task Write(string key, string value){
			if (key is null)
			{
				throw new ArgumentNullException(nameof(key));
			}
			uint hash = Misc.HashString3(key);
			BigInteger bigInteger = hash;

			if (value is null){
				if (await bTreeHandle.Contains(bigInteger))
				{
					if(!cache.TryGetValue(hash, out HashHandle hashHandle)){
						hashHandle = cache.GetOrAdd(hash, await optimisticExecutionScope.OpenHash(Misc.GetChildKey(name, hash.ToString())));
					}
					await hashHandle.Write(key, null);
					if ((await hashHandle.Length()).IsZero)
					{
						if (await bTreeHandle.TryRemove(bigInteger))
						{
							return;
						}
						throw new Exception("Unable to deregister empty child hash from b-tree (should not reach here)");
					}
				}
			} else{
				Task<bool> task = bTreeHandle.TryInsert(bigInteger);
				if (cache.TryGetValue(hash, out HashHandle hashHandle))
				{
					await hashHandle.Write(key, value);
					await task;
					return;
				}
				string hashname = Misc.GetChildKey(name, hash.ToString());
				if (await task)
				{
					await optimisticExecutionScope.TryCreateHash(hashname);
				}
				await cache.GetOrAdd(hash, await optimisticExecutionScope.OpenHash(hashname)).Write(key, value);
			}
		}
		public async IAsyncEnumerable<NewKeyValuePair<string, string>> ToAsyncEnumerable(SafepointController safepointController){
			await foreach(BigInteger bigInteger in bTreeHandle.SelectAll(safepointController, false)){
				await foreach(NewKeyValuePair<string, string> keyValuePair in (await optimisticExecutionScope.OpenHash(Misc.GetChildKey(name, bigInteger.ToString()))).ToAsyncEnumerable(safepointController)){
					yield return keyValuePair;
				}
			}
		}
		public Task<bool> NotEmpty(){
			return bTreeHandle.NotEmpty();
		}
	}
	public readonly struct SortedSetHandle{
		private readonly BTreeHandle bTreeHandle;
		private readonly string name1;
		private readonly string dictionary;
		private readonly IOptimisticExecutionScope optimisticExecutionScope;

		internal SortedSetHandle(BTreeHandle bTreeHandle, string name1, string dictionary, IOptimisticExecutionScope optimisticExecutionScope)
		{
			this.bTreeHandle = bTreeHandle;
			this.name1 = name1;
			this.dictionary = dictionary;
			this.optimisticExecutionScope = optimisticExecutionScope;
		}

		private async Task<LargeHashHandle> GetHash(string name){
			name = Misc.GetChildKey(name1, name);
			await optimisticExecutionScope.TryCreateLargeHash(name);
			return await optimisticExecutionScope.OpenLargeHash(name);
		}
		public async Task Write(string key, string value, BigInteger bigInteger){
			if (value is null)
			{
				throw new InvalidOperationException("Use TryDelete instead");
			}
			Task btreeadd = bTreeHandle.TryInsert(bigInteger);
			string prereadkey = Misc.GetChildKey(dictionary, key);
			Task<string> tsk = optimisticExecutionScope.Read(prereadkey);
			string strbigint = bigInteger.ToString();
			Task<LargeHashHandle> getHash1 = GetHash(strbigint);
			string preread = await tsk;
			if (preread is null)
			{
				Task tsk4 = Write2(getHash1, key, value);
				optimisticExecutionScope.Write(prereadkey, strbigint);
				await tsk4;
			}
			else
			{
				LargeHashHandle hash1 = await getHash1;
				if (preread == strbigint)
				{
					
					Task tsk4 = hash1.Write(key, value);
					optimisticExecutionScope.Write(prereadkey, strbigint);
					await tsk4;
				}
				else
				{
					Task<LargeHashHandle> GetHash2 = GetHash(preread);
					Task tsk4 = hash1.Write(key, value);
					LargeHashHandle hash2 = await GetHash2;
					Task tsk3 = hash2.Write(key, null);
					optimisticExecutionScope.Write(prereadkey, strbigint);
					BigInteger bigInteger1 = BigInteger.Parse(preread, NumberStyles.AllowLeadingSign);
					await tsk3;
					Task tsk5 = BTreeRemoveIfNeeded(bigInteger1, hash2);
					await tsk4;
					await tsk5;
				}
			}
			await btreeadd;
		}
		private async Task BTreeRemoveIfNeeded(BigInteger bigInteger, LargeHashHandle hash2){
			if (await hash2.NotEmpty())
			{
				return;
			}
			if (await bTreeHandle.TryRemove(bigInteger))
			{
				return;
			}
			throw new Exception("Unable to remove from b-tree (should not reach here)");
		}
		private static async Task Write2(Task<LargeHashHandle> task, string key, string value){
			await (await task).Write(key, value);
		}
		public async Task<bool> TryDelete(string key)
		{
			string temp = Misc.GetChildKey(dictionary, key);
			Task<string> readtsk = optimisticExecutionScope.Read(temp);
			string read = await readtsk;
			if (read is null)
			{
				return false;
			}
			LargeHashHandle largeHashHandle = await GetHash(read);
			Task tsk = largeHashHandle.Write(key, null);
			optimisticExecutionScope.Write(temp, null);
			BigInteger bigInteger = BigInteger.Parse(read, NumberStyles.AllowLeadingSign);
			await tsk;
			if (await largeHashHandle.NotEmpty())
			{
				return true;
			}
			if(await bTreeHandle.TryRemove(bigInteger)){
				return true;
			}
			throw new Exception("Unable to remove from b-tree (should not reach here)");
		}
		public async IAsyncEnumerable<SortedSetReadResult> Select(SafepointController safepointController, CompareOperator compareOperator, BigInteger bigInteger, bool reverse) {
			await foreach(BigInteger value in bTreeHandle.Select(safepointController, bigInteger, compareOperator, reverse)){
				await foreach(NewKeyValuePair<string, string> newKeyValuePair in (await GetHash(value.ToString())).ToAsyncEnumerable(safepointController)){
					yield return new SortedSetReadResult(newKeyValuePair.key, value, newKeyValuePair.value);
				}
			}
		}
		public async IAsyncEnumerable<SortedSetReadResult> SelectAll(SafepointController safepointController, bool reverse)
		{
			await foreach (BigInteger value in bTreeHandle.SelectAll(safepointController, reverse))
			{
				await foreach (NewKeyValuePair<string, string> newKeyValuePair in (await GetHash(value.ToString())).ToAsyncEnumerable(safepointController))
				{
					yield return new SortedSetReadResult(newKeyValuePair.key, value, newKeyValuePair.value);
				}
			}
		}
	}
	public readonly struct SelectionOrderingRequirement{
		public readonly string rowname;
		public readonly bool descending;

		public SelectionOrderingRequirement(string rowname, bool descending)
		{
			this.rowname = rowname ?? throw new ArgumentNullException(nameof(rowname));
			this.descending = descending;
		}
	}
	public readonly struct TableHandle
	{
		private readonly IReadOnlyDictionary<string, string> primaryRowIndexes;
		private readonly IReadOnlyDictionary<string, SortedSetHandle> bTreeIndexes;
		private readonly IReadOnlyDictionary<string, bool> dataColumns;
		private readonly IOptimisticExecutionScope optimisticExecutionScope;

		public TableHandle(IReadOnlyDictionary<string, string> primaryRowIndexes, IReadOnlyDictionary<string, SortedSetHandle> bTreeIndexes, IReadOnlyDictionary<string, bool> dataColumns, IOptimisticExecutionScope optimisticExecutionScope)
		{
			this.primaryRowIndexes = primaryRowIndexes;
			this.bTreeIndexes = bTreeIndexes;
			this.dataColumns = dataColumns;
			this.optimisticExecutionScope = optimisticExecutionScope;
		}

		public async Task Insert(IReadOnlyDictionary<string, string> stringCols, IReadOnlyDictionary<string, BigInteger> intCols)
		{
			Dictionary<string, string> allCols = new Dictionary<string, string>(stringCols);
			foreach (KeyValuePair<string, BigInteger> keyValuePair1 in intCols)
			{
				allCols.Add(keyValuePair1.Key, keyValuePair1.Value.ToString());
			}
			foreach(string key in primaryRowIndexes.Keys){
				if(stringCols.TryGetValue(key, out string value)){
					if(value is null){
						throw new InvalidOperationException(new StringBuilder("Primary key ").Append(key).Append(" is set to null").ToString());
					}
					continue;
				}
				throw new InvalidOperationException(new StringBuilder("Primary key ").Append(key).Append(" is not set").ToString());
			}
			foreach(string key in dataColumns.Keys){
				if(stringCols.ContainsKey(key)){
					continue;
				}
				throw new InvalidOperationException(new StringBuilder("Data column ").Append(key).Append(" is not set").ToString());
			}
			foreach (string key in stringCols.Keys)
			{
				if (dataColumns.ContainsKey(key))
				{
					continue;
				}
				if (primaryRowIndexes.ContainsKey(key))
				{
					continue;
				}
				throw new InvalidOperationException(new StringBuilder("String column ").Append(key).Append(" does not exist").ToString());
			}
			foreach (string key in intCols.Keys){
				if(bTreeIndexes.ContainsKey(key)){
					continue;
				}
				throw new InvalidOperationException(new StringBuilder("Sorted int column ").Append(key).Append(" does not exist").ToString());
			}
			foreach (string key in bTreeIndexes.Keys)
			{
				if (intCols.ContainsKey(key))
				{
					continue;
				}
				throw new InvalidOperationException(new StringBuilder("Sorted int column ").Append(key).Append(" is not set").ToString());
			}
			string id = YuriTables.Extensions.Random();
			Task[] tasks = new Task[bTreeIndexes.Count];
			int xctr = 0;
			foreach(KeyValuePair<string, SortedSetHandle> sortedSetHandle in bTreeIndexes){
				tasks[xctr++] = sortedSetHandle.Value.Write(id, string.Empty, intCols[sortedSetHandle.Key]);
			}

			optimisticExecutionScope.Write(id, JsonConvert.SerializeObject(allCols));

			foreach (KeyValuePair<string, string> keyValuePair in primaryRowIndexes)
			{
				string key = keyValuePair.Key;
				string read2 = allCols[key];
				if (read2 is null)
				{
					throw new InvalidOperationException("Primary keys cannot be set to null");
				}
				string temp = Misc.GetChildKey(primaryRowIndexes[key], read2);
				if ((await optimisticExecutionScope.Read(temp)) is null)
				{
					optimisticExecutionScope.Write(temp, id);
					continue;
				}
				throw new InvalidOperationException(new StringBuilder("Key ").Append(read2).Append(" of column ").Append(key).Append(" already exists").ToString());
				
			}
			await tasks;
		}
		public async Task<TryGetRowResult> TryGetRowByPrimaryKey(string column, string key){
			string id = await optimisticExecutionScope.Read(Misc.GetChildKey(primaryRowIndexes[column], key));
			if(id is null){
				return default;
			}
			return new TryGetRowResult(Misc.DeserializeObjectWithFastCreate<Dictionary<string, string>>(await optimisticExecutionScope.Read(id)));
		}
		public async IAsyncEnumerable<IReadOnlyDictionary<string, string>> SelectAllOrderedBy(SafepointController safepointController, string column, bool reverse){
			await foreach(SortedSetReadResult sortedSetReadResult in bTreeIndexes[column].SelectAll(safepointController, reverse)){
				Task<string> read = optimisticExecutionScope.Read(sortedSetReadResult.key);
				Dictionary<string, string> row = new Dictionary<string, string>();
				JsonConvert.PopulateObject(await read, row);
				yield return row;
			}
		}
		public async IAsyncEnumerable<IReadOnlyDictionary<string, string>> SelectOrderedBy(SafepointController safepointController, string column, CompareOperator compareOperator, BigInteger bigInteger, bool reverse)
		{
			await foreach (SortedSetReadResult sortedSetReadResult in bTreeIndexes[column].Select(safepointController, compareOperator, bigInteger, reverse))
			{
				Task<string> read = optimisticExecutionScope.Read(sortedSetReadResult.key);
				Dictionary<string, string> row = new Dictionary<string, string>();
				JsonConvert.PopulateObject(await read, row);
				yield return row;
			}
		}
		public async Task UpdateRowByPrimaryKey(string column, string targetColumn, string key, string value){
			if(bTreeIndexes.ContainsKey(targetColumn)){
				throw new InvalidOperationException("We cannot do this with sorted integer values");
			}
			string id = await optimisticExecutionScope.Read(Misc.GetChildKey(primaryRowIndexes[column], key));
			if(id is null){
				throw new InvalidOperationException("This primary key does not exist");
			}
			Task<string> read = optimisticExecutionScope.Read(id);
			Dictionary<string, string> decoded = new Dictionary<string, string>();
			JsonConvert.PopulateObject(await read, decoded);
			string oldValue = decoded[targetColumn];
			if (oldValue == value)
			{
				return;
			}
			async Task Whatever(string hash, IOptimisticExecutionScope optimisticExecutionScope)
			{
				string k1 = Misc.GetChildKey(hash, oldValue);
				Task<string> read1 = optimisticExecutionScope.Read(k1);
				string k2 = Misc.GetChildKey(hash, value);
				Task<string> read2 = optimisticExecutionScope.Read(k2);
				if (id == await read1)
				{
					if ((await read2) is null)
					{
						optimisticExecutionScope.Write(k2, id);
						optimisticExecutionScope.Write(k1, null);
					}
					else
					{
						throw new InvalidOperationException("Duplicate primary key");
					}
				}
				else
				{
					throw new Exception("FATAL Duplicate primary key (should not reach here)");
				}
			}
			Task task;
			if (primaryRowIndexes.TryGetValue(targetColumn, out string hash))
			{
				task = Whatever(hash, optimisticExecutionScope);
			}
			else
			{
				task = null;
			}

			decoded[targetColumn] = value;
			optimisticExecutionScope.Write(id, JsonConvert.SerializeObject(decoded));
			if (task is null)
			{
				return;
			}
			await task;
		}
		public async Task UpdateSortedIntByPrimaryKey(string column, string targetColumn, string key, BigInteger value){
			Task<string> getid = optimisticExecutionScope.Read(Misc.GetChildKey(primaryRowIndexes[column], key));
			if (bTreeIndexes.TryGetValue(targetColumn, out SortedSetHandle sortedSetHandle))
			{
				string id = await getid;
				if (id is null)
				{
					throw new InvalidOperationException("This primary key does not exist");
				}
				await UpdateIntImpl(id, targetColumn, value, sortedSetHandle);
				return;
			}
			throw new InvalidOperationException("This operation can only be performed on sorted int columns");
		}
		public async Task UpdateAllRows(SafepointController safepointController, string targetColumn, string value){
			bool isPrimary;
			if(primaryRowIndexes.TryGetValue(targetColumn, out string primaryHash)){
				isPrimary = true;
			} else{
				if(dataColumns.ContainsKey(targetColumn)){
					isPrimary = false;
				} else{
					throw new InvalidOperationException(bTreeIndexes.ContainsKey(targetColumn) ? "This operation is not valid for int columns" : "This column does not exist");
				}
			}
			SortedSetHandle sortedSetHandle;
			foreach(SortedSetHandle temp in bTreeIndexes.Values){
				sortedSetHandle = temp;
				goto start;
			}
			throw new InvalidOperationException("Enumeration is only supported for tables with b-tree indexes");
		start:
			IAsyncEnumerable<SortedSetReadResult> sortedSetReadResults = sortedSetHandle.SelectAll(safepointController, false);
			if (isPrimary){
				await foreach (SortedSetReadResult sortedSetReadResult in sortedSetReadResults)
				{
					await UpdatePrimaryImpl(sortedSetReadResult.key, targetColumn, value, primaryHash);
				}
			} else{
				Queue<Task> tasks = new Queue<Task>();
				await foreach (SortedSetReadResult sortedSetReadResult in sortedSetReadResults)
				{
					tasks.Enqueue(UpdateNormalImpl(sortedSetReadResult.key, targetColumn, value));
				}
				await tasks.ToArray();
			}
		}
		public async Task UpdateMultipleIntRows(SafepointController safepointController, string column, BigInteger bigInteger, CompareOperator compareOperator, string targetColumn, BigInteger value)
		{
			if (!bTreeIndexes.TryGetValue(targetColumn, out SortedSetHandle sortedSetHandle1))
			{
				throw new InvalidOperationException("This operation is only valid for sorted int rows");
			}
			bool samecol = targetColumn == column;
			SortedSetHandle sortedSetHandle;
			if (samecol){
				sortedSetHandle = sortedSetHandle1;
			} else{
				if (!bTreeIndexes.TryGetValue(column, out sortedSetHandle))
				{
					throw new InvalidOperationException("This operation is only valid for sorted int rows");
				}
			}

			IAsyncEnumerable<SortedSetReadResult> sortedSetReadResults = sortedSetHandle.Select(safepointController, compareOperator, bigInteger, false);
			if (samecol){
				Queue<string> keys = new Queue<string>();
				await foreach (SortedSetReadResult sortedSetReadResult in sortedSetReadResults)
				{
					keys.Enqueue(sortedSetReadResult.key);
				}
				while(keys.TryDequeue(out string key)){
					await UpdateIntImpl(key, targetColumn, value, sortedSetHandle1);
				}
			} else{
				await foreach (SortedSetReadResult sortedSetReadResult in sortedSetReadResults)
				{
					await UpdateIntImpl(sortedSetReadResult.key, targetColumn, value, sortedSetHandle1);
				}
			}
		}
		public async Task UpdateMultipleRows(SafepointController safepointController, string column, BigInteger bigInteger, CompareOperator compareOperator, string targetColumn, string value){
			if(!bTreeIndexes.TryGetValue(column, out SortedSetHandle sortedSetHandle)){
				throw new InvalidOperationException("This operation is only valid for sorted int rows");
			}
			bool isPrimary;
			if (primaryRowIndexes.TryGetValue(targetColumn, out string primaryHash))
			{
				isPrimary = true;
			}
			else
			{
				if (dataColumns.ContainsKey(targetColumn))
				{
					isPrimary = false;
				}
				else
				{
					throw new InvalidOperationException(bTreeIndexes.ContainsKey(targetColumn) ? "This operation is not valid for int columns" : "This column does not exist");
				}
			}
			IAsyncEnumerable<SortedSetReadResult> sortedSetReadResults = sortedSetHandle.Select(safepointController, compareOperator, bigInteger, false);
			if (isPrimary)
			{
				await foreach (SortedSetReadResult sortedSetReadResult in sortedSetReadResults)
				{
					await UpdatePrimaryImpl(sortedSetReadResult.key, targetColumn, value, primaryHash);
				}
			}
			else
			{
				Queue<Task> tasks = new Queue<Task>();
				await foreach (SortedSetReadResult sortedSetReadResult in sortedSetReadResults)
				{
					tasks.Enqueue(UpdateNormalImpl(sortedSetReadResult.key, targetColumn, value));
				}
				await tasks.ToArray();
			}
		}
		private async Task UpdatePrimaryImpl(string id, string targetColumn, string value, string primaryHash)
		{
			Task<string> read = optimisticExecutionScope.Read(id);
			Dictionary<string, string> decoded = new Dictionary<string, string>();
			JsonConvert.PopulateObject(await read, decoded);
			string oldValue = decoded[targetColumn];
			if (oldValue == value)
			{
				return;
			}
			async Task Whatever(IOptimisticExecutionScope optimisticExecutionScope)
			{
				string k1 = Misc.GetChildKey(primaryHash, oldValue);
				Task<string> read1 = optimisticExecutionScope.Read(k1);
				string k2 = Misc.GetChildKey(primaryHash, value);
				Task<string> read2 = optimisticExecutionScope.Read(k2);
				if (id == await read1)
				{
					if ((await read2) is null)
					{
						optimisticExecutionScope.Write(k2, id);
						optimisticExecutionScope.Write(k1, null);
					}
					else
					{
						throw new InvalidOperationException("Duplicate primary key");
					}
				}
				else
				{
					throw new Exception("FATAL Duplicate primary key (should not reach here)");
				}
			}
			Task task = Whatever(optimisticExecutionScope);

			decoded[targetColumn] = value;
			optimisticExecutionScope.Write(id, JsonConvert.SerializeObject(decoded));
			if (task is null)
			{
				return;
			}
			await task;
		}
		private async Task UpdateNormalImpl(string id, string targetColumn, string value)
		{
			Task<string> read = optimisticExecutionScope.Read(id);
			Dictionary<string, string> decoded = new Dictionary<string, string>();
			JsonConvert.PopulateObject(await read, decoded);
			string oldValue = decoded[targetColumn];
			if (oldValue == value)
			{
				return;
			}

			decoded[targetColumn] = value;
			optimisticExecutionScope.Write(id, JsonConvert.SerializeObject(decoded));
		}
		private async Task UpdateIntImpl(string id, string column, BigInteger newValue, SortedSetHandle btree)
		{
			Task<string> read = optimisticExecutionScope.Read(id);
			string strbigint = newValue.ToString();
			Dictionary<string, string> decoded = new Dictionary<string, string>();
			JsonConvert.PopulateObject(await read, decoded);
			string oldval = decoded[column];
			if (oldval == strbigint){
				return;
			}
			Task tsk = btree.Write(id, string.Empty, newValue);
			decoded[column] = strbigint;
			optimisticExecutionScope.Write(id, JsonConvert.SerializeObject(decoded));
			await tsk;
		}
		private static async Task EnsureTrue(Task<bool> task){
			if(await task){
				return;
			}
			throw new Exception("Unable to remove row from index (should not reach here)");
		}
		private async Task DeleteImpl(string id){
			Queue<Task> deleteFromIndexes = new Queue<Task>();
			foreach(SortedSetHandle sortedSetHandle in bTreeIndexes.Values){
				deleteFromIndexes.Enqueue(EnsureTrue(sortedSetHandle.TryDelete(id)));
			}
			if(primaryRowIndexes.Count > 0){
				Task<string> read = optimisticExecutionScope.Read(id);
				Dictionary<string, string> decoded = new Dictionary<string, string>();
				JsonConvert.PopulateObject(await read, decoded);
				foreach(KeyValuePair<string, string> kvp in primaryRowIndexes){
					optimisticExecutionScope.Write(Misc.GetChildKey(kvp.Value, decoded[kvp.Key]), null);
				}
			}
			await deleteFromIndexes.ToArray();
		}
		public async Task DeleteRowByPrimaryKey(string column, string key)
		{
			string id = await optimisticExecutionScope.Read(Misc.GetChildKey(primaryRowIndexes[column], key));
			if (id is null)
			{
				throw new InvalidOperationException("This primary key does not exist");
			}
			await DeleteImpl(id);
		}
	}
	public readonly struct TryGetRowResult{
		public readonly bool found;
		public readonly IReadOnlyDictionary<string, string> theRow;
		public TryGetRowResult(IReadOnlyDictionary<string, string> theRow)
		{
			this.theRow = theRow ?? throw new ArgumentNullException(nameof(theRow));
			found = true;
		}
	}
}
