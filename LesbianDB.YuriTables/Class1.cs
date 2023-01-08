using System;
using LesbianDB.Optimism.Core;
using LesbianDB;
using System.Numerics;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Globalization;
using System.Text;
using System.Collections.Generic;
using System.Threading;
using Newtonsoft.Json;
using System.Diagnostics.CodeAnalysis;
using System.Collections.Concurrent;
using System.Linq;
using Newtonsoft.Json.Converters;
using System.Collections;

namespace LesbianDB.Optimism.YuriTables
{
	/// <summary>
	/// NOTE: YuriTables is NOT production-ready
	/// </summary>
	public static class Extensions
	{
		public static string Random(){
			Span<byte> bytes = stackalloc byte[32];
			RandomNumberGenerator.Fill(bytes);
			return Convert.ToBase64String(bytes, Base64FormattingOptions.None);
		}
		public static async Task ArrayPushStart(this IOptimisticExecutionScope optimisticExecutionScope, string arrayname, string item){
			string arrayname2 = arrayname + '_';
			string read = await optimisticExecutionScope.Read(arrayname2);
			if(read is null){
				optimisticExecutionScope.Write(arrayname2, "0_1");
				optimisticExecutionScope.Write(arrayname + "_0", item);
			} else{
				string[] header = read.Split('_');
				string key = (BigInteger.Parse(header[0], NumberStyles.AllowLeadingSign) - 1).ToString();
				optimisticExecutionScope.Write(arrayname2 + key, item);
				optimisticExecutionScope.Write(arrayname2, new StringBuilder(key).Append('_').Append(header[1]).ToString());
			}
		}
		public static async Task ArrayPushEnd(this IOptimisticExecutionScope optimisticExecutionScope, string arrayname, string item)
		{
			string arrayname2 = arrayname + '_';
			string read = await optimisticExecutionScope.Read(arrayname2);
			if (read is null)
			{
				optimisticExecutionScope.Write(arrayname2, "0_1");
				optimisticExecutionScope.Write(arrayname + "_0", item);
			}
			else
			{
				string[] header = read.Split('_');
				BigInteger key = BigInteger.Parse(header[1], NumberStyles.AllowLeadingSign);
				optimisticExecutionScope.Write(arrayname2 + key, item);
				optimisticExecutionScope.Write(arrayname2, new StringBuilder(header[0]).Append('_').Append((key + 1).ToString()).ToString());
			}
		}
		public static async Task<BigInteger> ArrayGetLength(this IOptimisticExecutionScope optimisticExecutionScope, string arrayname){
			string read = await optimisticExecutionScope.Read(arrayname + '_');
			if(read is null){
				return BigInteger.Zero;
			}
			string[] header = read.Split('_');
			return BigInteger.Parse(header[1], NumberStyles.AllowLeadingSign) - BigInteger.Parse(header[0], NumberStyles.AllowLeadingSign);
		}
		public static async Task<ReadResult> ArrayTryPopFirst(this IOptimisticExecutionScope optimisticExecutionScope, string arrayname)
		{
			string arrayname2 = arrayname + '_';
			string read = await optimisticExecutionScope.Read(arrayname2);
			if (read is null)
			{
				return default;
			}
			string[] header = read.Split('_');
			if (header[0] == header[1])
			{
				return default;
			}
			BigInteger bigInteger = BigInteger.Parse(header[0], NumberStyles.AllowLeadingSign);
			string key = arrayname2 + bigInteger.ToString();

			//Optimistic optimizations constraint: make sure that we are atomically consistent
			string value = (await optimisticExecutionScope.VolatileRead(new string[] { key, arrayname2 }))[key];

			optimisticExecutionScope.Write(key, null);
			optimisticExecutionScope.Write(arrayname2, new StringBuilder((bigInteger + 1).ToString()).Append('_').Append(header[1]).ToString());
			return new ReadResult(value);
		}
		public static async Task<ReadResult> ArrayTryPopLast(this IOptimisticExecutionScope optimisticExecutionScope, string arrayname)
		{
			string arrayname2 = arrayname + '_';
			string read = await optimisticExecutionScope.Read(arrayname2);
			if (read is null)
			{
				return default;
			}
			string[] header = read.Split('_');
			if (header[0] == header[1])
			{
				return default;
			}
			string bigInteger = (BigInteger.Parse(header[1], NumberStyles.AllowLeadingSign) - 1).ToString();
			string key = arrayname2 + bigInteger;

			//Optimistic optimizations constraint: make sure that we are atomically consistent
			IReadOnlyDictionary<string, string> dict = await optimisticExecutionScope.VolatileRead(new string[] { key, arrayname2 });

			optimisticExecutionScope.Write(key, null);

			optimisticExecutionScope.Write(arrayname2, new StringBuilder(header[0]).Append('_').Append(bigInteger).ToString());
			return new ReadResult(dict[key]);
		}
		public static async Task<string> ArrayGetValue(this IOptimisticExecutionScope optimisticExecutionScope, string arrayname, BigInteger index){
			string arrayname2 = arrayname + '_';
			if (index.Sign < 0){
				throw new IndexOutOfRangeException("negative array index");
			}
			string read = await optimisticExecutionScope.Read(arrayname2);
			if (read is null)
			{
				throw new IndexOutOfRangeException();
			}
			string[] header = read.Split('_');
			BigInteger start = BigInteger.Parse(header[0], NumberStyles.AllowLeadingSign);
			BigInteger end = BigInteger.Parse(header[1], NumberStyles.AllowLeadingSign);
			index += start;
			if(end > index){
				string key = arrayname2 + index.ToString();

				//Optimistic optimizations constraint: make sure that we are atomically consistent
				return optimisticExecutionScope is VolatileReadManager ? await optimisticExecutionScope.Read(key) : (await optimisticExecutionScope.VolatileRead(new string[] {key, arrayname2}))[key];
			}
			throw new IndexOutOfRangeException();
		}
		private static readonly BigInteger one = BigInteger.One;
		public static async Task<ReadResult> ArrayTryGetValue(this IOptimisticExecutionScope optimisticExecutionScope, string arrayname, bool last)
		{
			string arrayname2 = arrayname + '_';
			string read = await optimisticExecutionScope.Read(arrayname2);
			if (read is null)
			{
				return default;
			}
			string[] header = read.Split('_');
			string start = header[0];
			string end = header[1];
			if(start == end){
				return default;
			}
			string key = arrayname2 + (last ? (BigInteger.Parse(end, NumberStyles.AllowLeadingSign) - one).ToString() : start);
			return new ReadResult(optimisticExecutionScope is VolatileReadManager ? await optimisticExecutionScope.Read(key) : (await optimisticExecutionScope.VolatileRead(new string[] { key, arrayname2 }))[key]);
		}
		public static async Task ArraySetValue(this IOptimisticExecutionScope optimisticExecutionScope, string arrayname, BigInteger index, string value)
		{
			string arrayname2 = arrayname + '_';
			if (index.Sign < 0)
			{
				throw new IndexOutOfRangeException("negative array index");
			}
			string read = await optimisticExecutionScope.Read(arrayname2);
			if (read is null)
			{
				throw new IndexOutOfRangeException();
			}
			string[] header = read.Split('_');
			BigInteger end = BigInteger.Parse(header[1], NumberStyles.AllowLeadingSign);
			index += BigInteger.Parse(header[0], NumberStyles.AllowLeadingSign);
			if (end > index)
			{
				optimisticExecutionScope.Write(arrayname2 + index.ToString(), value);
				return;
			}
			throw new IndexOutOfRangeException();
		}
		public static void ClearArray(this IOptimisticExecutionScope optimisticExecutionScope, string arrayname){
			optimisticExecutionScope.Write(arrayname + '_', "0_0");
		}
		public static async Task ResizeArray(this IOptimisticExecutionScope optimisticExecutionScope, string arrayname, BigInteger start, BigInteger length){
			string arrayname2 = arrayname + '_';
			if (start.Sign < 0)
			{
				throw new IndexOutOfRangeException("negative array index");
			}
			if(length.Sign < 0){
				throw new IndexOutOfRangeException("negative array size");
			}
			string read = await optimisticExecutionScope.Read(arrayname2);
			if (read is null)
			{
				if (length.IsZero && start.IsZero)
				{
					optimisticExecutionScope.ClearArray(arrayname);
					return;
				}
				throw new IndexOutOfRangeException();
			}
			string[] header = read.Split('_');
			BigInteger start1 = BigInteger.Parse(header[0], NumberStyles.AllowLeadingSign) + start;
			BigInteger end1 = start1 + length;
			if(end1 > BigInteger.Parse(header[1], NumberStyles.AllowLeadingSign)){
				throw new IndexOutOfRangeException();
			}
			optimisticExecutionScope.Write(arrayname2, new StringBuilder(start1.ToString()).Append('_').Append(end1).ToString());
		}
		public static async IAsyncEnumerable<string> ArrayEnumerate(this IOptimisticExecutionScope optimisticExecutionScope, string arrayname){
			StringBuilder stringBuilder = new StringBuilder(arrayname).Append('_');
			string arrayname2 = stringBuilder.ToString();
			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);
			string read = await optimisticExecutionScope.Read(arrayname2);
			if(read is null){
				yield break;
			}
			string[] header = read.Split('_');
			int namelen = arrayname2.Length;
			BigInteger start = BigInteger.Parse(header[0], NumberStyles.AllowLeadingSign);
			BigInteger end = BigInteger.Parse(header[1], NumberStyles.AllowLeadingSign);
			for(BigInteger i = start; i < end; ){
				string strbigint = i.ToString();
				yield return await optimisticExecutionScope.Read(stringBuilder.Append(strbigint).ToString());
				if(++i < end){
					stringBuilder.Remove(namelen, strbigint.Length);
				}
			}
		}
		[JsonObject]
		private sealed class JsonBTreeNode{
			public string[] list;
			public string low;
			public string high;
		}
		private sealed class BTreeBinarySearchComparer : IComparer<string>
		{
			private BTreeBinarySearchComparer(){
				
			}
			public static readonly BTreeBinarySearchComparer instance = new BTreeBinarySearchComparer();
			public int Compare(string x, string y)
			{
				return BigInteger.Compare(BigInteger.Parse(x.AsSpan(0, x.IndexOf('_')), NumberStyles.AllowLeadingSign), BigInteger.Parse(y.AsSpan(0, y.IndexOf('_')), NumberStyles.AllowLeadingSign));
			}
		}
		private static int BTreeCompareValToInt(string val, BigInteger bigInteger){
			return BigInteger.Compare(BigInteger.Parse(val.AsSpan(0, val.IndexOf('_')), NumberStyles.AllowLeadingSign), bigInteger);
		}
		
		private static bool CoinFlip(){
			Span<byte> bytes = stackalloc byte[1];
			RandomNumberGenerator.Fill(bytes);
			return bytes[0] < 128;
		}
		private static bool CheckOperator(BigInteger x, BigInteger y, CompareOperator compareOperator){
			return compareOperator switch
			{
				CompareOperator.LessThan => x < y,
				CompareOperator.LessThanOrEqual => x <= y,
				CompareOperator.GreaterThanOrEqual => x >= y,
				CompareOperator.GreaterThan => x > y,
				CompareOperator.EqualTo => x == y,
				CompareOperator.NotEqualTo => x != y,
				_ => throw new InvalidOperationException(),
			};
		}
		public static async Task HashSet(this IOptimisticExecutionScope optimisticExecutionScope, string hashname, string key, string value){
			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);
			StringBuilder stringBuilder = new StringBuilder(hashname).Append("_keys");
			string arrname = stringBuilder.ToString();
			Task<BigInteger> getarrlen = optimisticExecutionScope.ArrayGetLength(arrname);
			int namelen2 = hashname.Length + 1;
			string key2 = stringBuilder.Remove(namelen2, 4).Append("underlying_").Append(key).ToString();
			string oldval = await optimisticExecutionScope.Read(key2);
			string write;
			if (oldval is null){
				if(value is null){
					return;
				}
				BigInteger index = await getarrlen;
				await optimisticExecutionScope.ArrayPushEnd(arrname, key);
				write = new StringBuilder(index.ToString()).Append('_').Append(value).ToString();
			} else{
				int slice = oldval.IndexOf('_');
				if (value is null){
					if((await getarrlen) == 1){
						if((await optimisticExecutionScope.ArrayTryPopLast(arrname)).exists){
							optimisticExecutionScope.Write(key2, null);
							return;
						}
						throw new Exception("Unexpected zero length keylist (should not reach here)");
					}
					BigInteger index = BigInteger.Parse(oldval.AsSpan(0, slice), NumberStyles.None);
					ReadResult readResult = await optimisticExecutionScope.ArrayTryPopLast(arrname);
					if(readResult.exists){
						if(readResult.value != oldval){
							Task tsk = optimisticExecutionScope.ArraySetValue(arrname, index, readResult.value);
							namelen2 += 11;
							string movekey = stringBuilder.Remove(namelen2, key2.Length - namelen2).Append(readResult.value).ToString();
							Task<string> rereadTask = optimisticExecutionScope.Read(movekey);
							StringBuilder index2 = new StringBuilder(oldval.AsSpan(0, slice).ToString()).Append('_');
							string reread = await rereadTask;
							optimisticExecutionScope.Write(movekey, index2.Append(reread.AsSpan(reread.IndexOf('_') + 1)).ToString());
							await tsk;
						}
					} else{
						throw new Exception("Unexpected zero length keylist (should not reach here)");
					}
					write = null;
				} else{
					slice += 1;
					write = new StringBuilder(oldval, 0, slice, slice + value.Length).Append(value).ToString();
				}
			}
			optimisticExecutionScope.Write(key2, write);
		}
		public static async Task<string> HashGet(this IOptimisticExecutionScope optimisticExecutionScope, string hashname, string key){
			string str = await optimisticExecutionScope.Read(new StringBuilder(hashname).Append("_underlying_").Append(key).ToString());
			if(str is null){
				return str;
			}
			return str[(str.IndexOf('_') + 1)..];
		}
		public static async IAsyncEnumerable<NewKeyValuePair<string, string>> HashEnumerate(this IOptimisticExecutionScope optimisticExecutionScope, string hashname)
		{
			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);
			await foreach(string key in optimisticExecutionScope.ArrayEnumerate(hashname + "_keys")){
				yield return new NewKeyValuePair<string, string>(key, await optimisticExecutionScope.HashGet(hashname, key));
			}
		}

		public static async Task<bool> BTreeTryInsert(this IOptimisticExecutionScope optimisticExecutionScope, string treename, BigInteger bigInteger){
			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);
			string strbigint = bigInteger.ToString();
			JsonBTreeNode jsonBTreeNode = new JsonBTreeNode();
			Queue<string> queue = new Queue<string>();
		start:
			string read = await optimisticExecutionScope.Read(treename);
			
			if(read is null){
				optimisticExecutionScope.Write(treename, JsonConvert.SerializeObject(new JsonBTreeNode()
				{
					list = new string[] { bigInteger.ToString()}
				}));
				return true;
			}
			JsonConvert.PopulateObject(read, jsonBTreeNode);
			string low = jsonBTreeNode.low;
			string high = jsonBTreeNode.high;
			string[] list = jsonBTreeNode.list;
			

			bool notadded = true;
			bool notfirst = low is null;
			bool noloop = true;
			foreach(string str in list){
				if(str == strbigint){
					return false;
				}
				BigInteger bigInteger1 = BigInteger.Parse(str, NumberStyles.AllowLeadingSign);
				if(bigInteger1 > bigInteger && notadded){
					if(notfirst){
						queue.Enqueue(strbigint);
						notadded = false;
					} else{
						queue.Clear();
						treename = low;
						goto start;
					}
				}
				queue.Enqueue(str);
				notfirst = true;
				noloop = false;
			}
			if (noloop) {
				throw new Exception("Unexpected empty b-tree node (should not reach here)");
			}
			if (notadded)
			{
				if (high is null)
				{
					queue.Enqueue(strbigint);
				}
				else
				{
					queue.Clear();
					treename = high;
					goto start;
				}
			}

			list = queue.ToArray();
			int len = list.Length;
			if (read.Length > 256 && len > 0 && (len % 3) == 0){
				int cut = len / 3;
				string lowerkey = Random();
				string upperkey = Random();
				string[] temp = new string[cut];
				list.AsSpan(0, cut).CopyTo(temp.AsSpan());
				jsonBTreeNode.high = null;
				jsonBTreeNode.list = temp;
				optimisticExecutionScope.Write(lowerkey, JsonConvert.SerializeObject(jsonBTreeNode));
				jsonBTreeNode.low = null;
				jsonBTreeNode.high = high;
				list.AsSpan(cut * 2, cut).CopyTo(temp.AsSpan());
				optimisticExecutionScope.Write(upperkey, JsonConvert.SerializeObject(jsonBTreeNode));
				jsonBTreeNode.low = lowerkey;
				jsonBTreeNode.high = upperkey;
				list.AsSpan(cut, cut).CopyTo(temp.AsSpan());
			} else{
				jsonBTreeNode.list = list;
			}
			optimisticExecutionScope.Write(treename, JsonConvert.SerializeObject(jsonBTreeNode));
			return true;
		}
		public static IAsyncEnumerable<BigInteger> BTreeSelect(this IOptimisticExecutionScope optimisticExecutionScope, string treename, CompareOperator compareOperator, bool reverse, BigInteger bigInteger){
			switch(compareOperator){
				case CompareOperator.LessThan:
					return optimisticExecutionScope.BTreeSelect(treename, bigInteger, bigInteger.ToString(), new JsonBTreeNode(), true, reverse);
				case CompareOperator.GreaterThan:
					return optimisticExecutionScope.BTreeSelect(treename, bigInteger, bigInteger.ToString(), new JsonBTreeNode(), false, reverse);
				case CompareOperator.GreaterThanOrEqual:
					bigInteger -= 1;
					return optimisticExecutionScope.BTreeSelect(treename, bigInteger, bigInteger.ToString(), new JsonBTreeNode(), false, reverse);
				case CompareOperator.LessThanOrEqual:
					bigInteger += 1;
					return optimisticExecutionScope.BTreeSelect(treename, bigInteger, bigInteger.ToString(), new JsonBTreeNode(), true, reverse);
				case CompareOperator.EqualTo:
					return optimisticExecutionScope.BTreeSelectEqual(treename, bigInteger);
				case CompareOperator.NotEqualTo:
					return optimisticExecutionScope.BTreeSelectNotEqual(treename, bigInteger, reverse);
				default:
					throw new InvalidOperationException("Invalid b-tree operation");
			}
		}
		public static IAsyncEnumerable<BigInteger> BTreeSelectAll(this IOptimisticExecutionScope optimisticExecutionScope, string treename, bool reverse){
			return BTreeSelectAll(VolatileReadManager.Create(optimisticExecutionScope), treename, reverse, new JsonBTreeNode());
		}
		private static async IAsyncEnumerable<BigInteger> BTreeSelectAll(ISnapshotReadScope volatileReadManager, string treename, bool reverse, JsonBTreeNode jsonBTreeNode){
			string read = await volatileReadManager.Read(treename);
			if (read is null)
			{
				yield break;
			}
			JsonConvert.PopulateObject(read, jsonBTreeNode);
			string[] list = jsonBTreeNode.list;
			string first;
			string last;
			if (reverse)
			{
				first = jsonBTreeNode.high;
				last = jsonBTreeNode.low;
			}
			else
			{
				first = jsonBTreeNode.low;
				last = jsonBTreeNode.high;
			}
			if(first is { }){
				await foreach(BigInteger bigInteger in BTreeSelectAll(volatileReadManager, first, reverse, jsonBTreeNode)){
					yield return bigInteger;
				}
			}
			if(reverse){
				int i = list.Length;
				while (i > 0){
					yield return BigInteger.Parse(list[--i], NumberStyles.AllowLeadingSign);
				}
			} else{
				foreach(string str in list){
					yield return BigInteger.Parse(str, NumberStyles.AllowLeadingSign);
				}
			}
			if (last is { })
			{
				await foreach (BigInteger bigInteger in BTreeSelectAll(volatileReadManager, last, reverse, jsonBTreeNode))
				{
					yield return bigInteger;
				}
			}
		}
		private static async IAsyncEnumerable<BigInteger> BTreeSelectEqual(this IOptimisticExecutionScope optimisticExecutionScope, string treename, BigInteger bigInteger){
			await foreach(BigInteger bigInteger1 in optimisticExecutionScope.BTreeSelect(treename, CompareOperator.GreaterThanOrEqual, false, bigInteger))
			{
				yield return bigInteger1;
				yield break;
			}
		}
		private static async IAsyncEnumerable<BigInteger> BTreeSelectNotEqual(this IOptimisticExecutionScope optimisticExecutionScope, string treename, BigInteger bigInteger, bool reverse){
			string strbigint = bigInteger.ToString();
			JsonBTreeNode jsonBTreeNode = new JsonBTreeNode();
			await foreach (BigInteger bigInteger1 in optimisticExecutionScope.BTreeSelect(treename, bigInteger, strbigint, jsonBTreeNode, !reverse, reverse))
			{
				yield return bigInteger1;
			}
			await foreach (BigInteger bigInteger1 in optimisticExecutionScope.BTreeSelect(treename, bigInteger, strbigint, jsonBTreeNode, reverse, reverse))
			{
				yield return bigInteger1;
			}
		}

		private static async IAsyncEnumerable<BigInteger> BTreeSelect(this IOptimisticExecutionScope optimisticExecutionScope, string treename, BigInteger bigInteger, string strbigint, JsonBTreeNode jsonBTreeNode, bool smaller, bool reverse)
		{
			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);
			string read = await optimisticExecutionScope.Read(treename);
			if(read is null){
				yield break;
			}
			JsonConvert.PopulateObject(read, jsonBTreeNode);
			string[] list = jsonBTreeNode.list;
			int len = list.Length;
			if(len == 0){
				throw new Exception("Unexpected empty b-tree buckets (should not reach here)");
			}
			string firstval = list[0];
			string lastval = list[len - 1];
			BigInteger firstint = BigInteger.Parse(firstval, NumberStyles.AllowLeadingSign);
			BigInteger lastint = len == 1 ? firstint : BigInteger.Parse(lastval, NumberStyles.AllowLeadingSign);

			IEnumerable<string> enumerable;
			string first;
			string last;
			if(reverse){
				enumerable = list.Reverse();
				first = jsonBTreeNode.high;
				last = jsonBTreeNode.low;
			} else{
				enumerable = list;
				first = jsonBTreeNode.low;
				last = jsonBTreeNode.high;
			}
			if(first is { }){
				await foreach(BigInteger bigInteger1 in optimisticExecutionScope.BTreeSelect(first, bigInteger, strbigint, jsonBTreeNode, smaller, reverse)){
					yield return bigInteger1;
				}
			}
			foreach(string str in enumerable){
				BigInteger bigInteger1;
				if(str == firstval){
					bigInteger1 = firstint;
				} else if(str == lastval){
					bigInteger1 = lastint;
				} else if(str == strbigint){
					//optimizations
					if(smaller == reverse){
						continue;
					} else{
						break;
					}
				} else{
					bigInteger1 = BigInteger.Parse(str, NumberStyles.AllowLeadingSign);
				}
				if(smaller ? (bigInteger1 < bigInteger) : (bigInteger1 > bigInteger)){
					yield return bigInteger1;
				}
			}
			if (last is { })
			{
				await foreach (BigInteger bigInteger1 in optimisticExecutionScope.BTreeSelect(last, bigInteger, strbigint, jsonBTreeNode, smaller, reverse))
				{
					yield return bigInteger1;
				}
			}
		}
		public static async Task<bool> ZTryDelete(this IOptimisticExecutionScope optimisticExecutionScope, string name, string key){
			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);
			StringBuilder stringBuilder = new StringBuilder(name).Append("_underlying_").Append(key);
			string temp = stringBuilder.ToString();
			Task<string> readtsk = optimisticExecutionScope.Read(temp);
			int namelen = name.Length + 1;
			string btreename = stringBuilder.Remove(namelen, temp.Length - namelen).Append("btree").ToString();
			string read = await readtsk;
			if(read is null){
				return false;
			}
			Task tsk = optimisticExecutionScope.HashSet(stringBuilder.Remove(namelen, btreename.Length - namelen).Append(read).ToString(), key, null);
			optimisticExecutionScope.Write(temp, null);
			await tsk;
			return true;
		}
		public static async Task ZSet(this IOptimisticExecutionScope optimisticExecutionScope, string name, string key, string value, BigInteger bigInteger){
			if(value is null){
				throw new InvalidOperationException("Use ZTryDelete instead");
			}
			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);
			StringBuilder stringBuilder = new StringBuilder(name);
			int namelen = name.Length + 1;
			string btreename = stringBuilder.Append("_btree").ToString();
			Task btreeadd = optimisticExecutionScope.BTreeTryInsert(btreename, bigInteger);
			string prereadkey = stringBuilder.Remove(namelen, 5).Append("underlying_").Append(key).ToString();
			Task<string> tsk = optimisticExecutionScope.Read(prereadkey);
			string strbigint = bigInteger.ToString();
			string hashname = stringBuilder.Remove(namelen, prereadkey.Length - namelen).Append(strbigint).ToString();
			string preread = await tsk;
			if(preread is null){
				Task tsk4 = optimisticExecutionScope.HashSet(hashname, key, value);
				optimisticExecutionScope.Write(prereadkey, strbigint);
				await tsk4;
			} else{
				if (preread == strbigint)
				{
					Task tsk4 = optimisticExecutionScope.HashSet(hashname, key, value);
					optimisticExecutionScope.Write(prereadkey, strbigint);
					await tsk4;
				} else{
					Task tsk3 = optimisticExecutionScope.HashSet(stringBuilder.Remove(namelen, hashname.Length - namelen).Append(preread).ToString(), key, null);
					Task tsk4 = optimisticExecutionScope.HashSet(hashname, key, value);
					optimisticExecutionScope.Write(prereadkey, strbigint);
					await tsk4;
					await tsk3;
				}
			}
			await btreeadd;
		}
		public static async IAsyncEnumerable<SortedSetReadResult> ZSelect(this IOptimisticExecutionScope optimisticExecutionScope, string name, CompareOperator compareOperator, BigInteger bigInteger, bool reverse){
			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);
			StringBuilder stringBuilder = new StringBuilder(name);
			int namelen = name.Length + 1;
			string treename = stringBuilder.Append("_btree").ToString();
			stringBuilder.Remove(namelen, 5);
			await foreach(BigInteger bigInteger1 in optimisticExecutionScope.BTreeSelect(treename, compareOperator, reverse, bigInteger)){
				string strbigint1 = bigInteger1.ToString();

				await foreach(NewKeyValuePair<string, string> keyValuePair in optimisticExecutionScope.HashEnumerate(stringBuilder.Append(strbigint1).ToString())){
					yield return new SortedSetReadResult(keyValuePair.key, bigInteger1, keyValuePair.value);
				}
				stringBuilder.Remove(namelen, strbigint1.Length);
			}
			yield break;
		}
		public static async IAsyncEnumerable<SortedSetReadResult> ZSelectAll(this IOptimisticExecutionScope optimisticExecutionScope, string name, bool reverse)
		{
			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);
			StringBuilder stringBuilder = new StringBuilder(name);
			int namelen = name.Length + 1;
			string treename = stringBuilder.Append("_btree").ToString();
			stringBuilder.Remove(namelen, 5);
			await foreach (BigInteger bigInteger1 in optimisticExecutionScope.BTreeSelectAll(treename, reverse))
			{
				string strbigint1 = bigInteger1.ToString();

				await foreach (NewKeyValuePair<string, string> keyValuePair in optimisticExecutionScope.HashEnumerate(stringBuilder.Append(strbigint1).ToString()))
				{
					yield return new SortedSetReadResult(keyValuePair.key, bigInteger1, keyValuePair.value);
				}
				stringBuilder.Remove(namelen, strbigint1.Length);
			}
			yield break;
		}
		public static async Task<bool> TryCreateTable(this IOptimisticExecutionScope optimisticExecutionScope, IReadOnlyDictionary<string, ColumnType> keyValuePairs, string name){
			foreach(ColumnType columnType in keyValuePairs.Values){
				if(columnType == ColumnType.SortedInt || columnType == ColumnType.UniqueString){
					goto create;
				}
			}
			throw new InvalidOperationException("Attempted to create write-only table");
		create:
			if((await optimisticExecutionScope.Read(name)) is null){
				optimisticExecutionScope.Write(name, JsonConvert.SerializeObject(keyValuePairs));
				return true;
			}
			return false;
		}
		public static async Task TableInsert(this IOptimisticExecutionScope optimisticExecutionScope, string table, IReadOnlyDictionary<string, string> stringCols, IReadOnlyDictionary<string, BigInteger> intCols){
			Task<string> readtsk = optimisticExecutionScope.Read(table);
			string id = Random();
			int namelen = table.Length + 1;
			StringBuilder stringBuilder = new StringBuilder(table).Append('_');

			Dictionary<string, string> allCols = new Dictionary<string, string>(stringCols);
			foreach(KeyValuePair<string, BigInteger> keyValuePair1 in intCols){
				allCols.Add(keyValuePair1.Key, keyValuePair1.Value.ToString());
			}
			optimisticExecutionScope.Write(id, JsonConvert.SerializeObject(allCols));
			string read = await readtsk;
			if(read is null){
				throw new InvalidOperationException(new StringBuilder("Table ").Append(table).Append(" does not exist").ToString());
			}

			IReadOnlyDictionary<string, ColumnType> tableDescriptor = Misc.DeserializeObjectWithFastCreate<Dictionary<string, ColumnType>>(read);
			foreach (KeyValuePair<string, ColumnType> keyValuePair2 in tableDescriptor){
				string key = keyValuePair2.Key;
				if (keyValuePair2.Value == ColumnType.SortedInt)
				{
					if (intCols.ContainsKey(key))
					{
						continue;
					}
				}
				else
				{
					if (stringCols.ContainsKey(key))
					{
						continue;
					}
				}
				throw new InvalidOperationException(new StringBuilder("Column ").Append(key).Append(" is not set").ToString());
			}
			if(allCols.Count > tableDescriptor.Count){
				throw new InvalidOperationException("More columns set than expected");
			}

			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);

			//Update indexes
			foreach(KeyValuePair<string, ColumnType> keyValuePair in tableDescriptor){
				ColumnType rowType = keyValuePair.Value;
				if(rowType == ColumnType.DataString){
					break;
				}
				string key = keyValuePair.Key;
				string temp = "";
				if(rowType == ColumnType.SortedInt){
					temp = stringBuilder.Append("btree_").Append(key).ToString();
					await optimisticExecutionScope.ZSet(temp, id, string.Empty, intCols[key]);
				} else{
					string read2 = allCols[key];
					if(read2 is null){
						throw new InvalidOperationException("Primary keys cannot be set to null");
					}
					temp = stringBuilder.Append("dictionary_").Append(key.Length).Append('_').Append(key).Append(read2).ToString();
					if ((await optimisticExecutionScope.Read(temp)) is { }){
						throw new InvalidOperationException(new StringBuilder("Key ").Append(read2).Append(" of column ").Append(key).Append(" already exists").ToString());
					}
					optimisticExecutionScope.Write(temp, id);
				}
				stringBuilder.Remove(namelen, temp.Length - namelen);
			}
		}
		public static async Task<Row> TableTrySelectPrimaryKey(this IOptimisticExecutionScope optimisticExecutionScope, string table, string column, string key){
			string read1 = new StringBuilder(table).Append("_dictionary_").Append(column.Length).Append('_').Append(column).Append(key).ToString();
			IReadOnlyDictionary<string, string> read = await optimisticExecutionScope.VolatileRead(new string[] { table, read1 });
			IReadOnlyDictionary<string, ColumnType> schema = Misc.DeserializeObjectWithFastCreate<Dictionary<string, ColumnType>>(read[table]);
			if (schema[column] != ColumnType.UniqueString){
				throw new InvalidOperationException(new StringBuilder("Column ").Append(column).Append(" of table ").Append(table).Append(" is not primary key").ToString());
			}
			string id = read[read1];
			if(id is null){
				return null;
			}
			return new Row(id, table, Misc.DeserializeObjectWithFastCreate<Dictionary<string, string>>((await optimisticExecutionScope.VolatileRead(new string[] { table, read1, id }))[id]), schema);
		}
		public static async IAsyncEnumerable<Row> TableTrySelectSortedRow(this IOptimisticExecutionScope optimisticExecutionScope, string table, string column, CompareOperator compareOperator, BigInteger bigInteger, bool reverse){
			Task<string> readtsk = optimisticExecutionScope.Read(table);
			string read1 = new StringBuilder(table).Append("_btree_").Append(column).ToString();
			string strbigint = bigInteger.ToString();
			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);
			string read = await readtsk;
			if (read is null)
			{
				throw new InvalidOperationException(new StringBuilder("Table ").Append(table).Append(" does not exist").ToString());
			}
			IReadOnlyDictionary<string, ColumnType> schema = Misc.DeserializeObjectWithFastCreate<Dictionary<string, ColumnType>>(read);
			if (schema[column] != ColumnType.SortedInt)
			{
				throw new InvalidOperationException(new StringBuilder("Column ").Append(column).Append(" of table ").Append(table).Append(" is not sorted int").ToString());
			}
			await foreach(SortedSetReadResult sortedSetReadResult in optimisticExecutionScope.ZSelect(read1, compareOperator, bigInteger, reverse)){
				Dictionary<string, string> temp = Misc.DeserializeObjectWithFastCreate<Dictionary<string, string>>(await optimisticExecutionScope.Read(sortedSetReadResult.key));
				if(temp[column] == sortedSetReadResult.bigInteger.ToString()){
					yield return new Row(sortedSetReadResult.key, table, temp, schema);
					continue;
				}
				//The only way we can get here is database corruption
				throw new Exception("Index out of sync with data, database may be corrupted (should not reach here)");
			}
			yield break;
		}
		public static async Task TableUpdateRow(this IOptimisticExecutionScope optimisticExecutionScope, Row row, string column, string value)
		{
			string id = row.id;
			Task<string> readtsk = optimisticExecutionScope.Read(id);
			string table = row.table;
			ColumnType columnType = row.GetColumnType(column);
			if (columnType == ColumnType.SortedInt){
				throw new InvalidOperationException("Sorted int updates are not supported by this method");
			}
			string read = await readtsk;
			if (read is null){
				throw new InvalidOperationException("This row is already deleted");
			}
			Dictionary<string, string> keyValuePairs = Misc.DeserializeObjectWithFastCreate<Dictionary<string, string>>(read);
			foreach (KeyValuePair<string, string> keyValuePair in row)
			{
				if(keyValuePairs[keyValuePair.Key] != keyValuePair.Value){
					throw new Exception("Row desynchronized from database");
				}
			}
			if (keyValuePairs[column] == value)
			{
				return;
			}
			//update indexes
			if (columnType == ColumnType.UniqueString)
			{
				if(value is null){
					throw new InvalidOperationException("Primary keys cannot be set to null");
				}
				StringBuilder stringBuilder = new StringBuilder(table).Append("_dictionary_").Append(column.Length).Append('_').Append(column);
				int namelen = stringBuilder.Length;
				string firstkey = stringBuilder.Append(column.Length).Append('_').Append(column).Append(row[column]).ToString();
				optimisticExecutionScope.Write(firstkey, null);
				optimisticExecutionScope.Write(stringBuilder.Remove(namelen, firstkey.Length - namelen).Append(value).ToString(), id);
			}
			row[column] = value;
			optimisticExecutionScope.Write(id, JsonConvert.SerializeObject(row));
		}
		public static async Task TableUpdateSortedIntRow(this IOptimisticExecutionScope optimisticExecutionScope, string name, Row row, string column, BigInteger bigInteger){
			string id = row.id;
			Task<string> readtsk = optimisticExecutionScope.Read(id);
			string table = row.table;
			if(row.GetColumnType(column) != ColumnType.SortedInt){
				throw new InvalidOperationException(new StringBuilder("Column ").Append(column).Append(" is not sorted int").ToString());
			}
			string strbigint = bigInteger.ToString();
			string btreename = new StringBuilder(table).Append("_btree_").Append(column).ToString();
			string read = await readtsk;
			if (read is null)
			{
				throw new InvalidOperationException("This row is already deleted");
			}
			Dictionary<string, string> keyValuePairs = Misc.DeserializeObjectWithFastCreate<Dictionary<string, string>>(read);
			foreach (KeyValuePair<string, string> keyValuePair in row)
			{
				if (keyValuePairs[keyValuePair.Key] != keyValuePair.Value)
				{
					throw new Exception("Row desynchronized from database");
				}
			}
			if(row[column] == strbigint){
				return;
			}
			Task tsk = optimisticExecutionScope.ZSet(btreename, id, string.Empty, bigInteger);
			row[column] = strbigint;
			optimisticExecutionScope.Write(id, JsonConvert.SerializeObject(row));
			await tsk;
		}
		public static async Task TableDeleteRow(this IOptimisticExecutionScope optimisticExecutionScope, Row row)
		{
			string id = row.id;
			Task<string> readtsk = optimisticExecutionScope.Read(id);
			string table = row.table;
			StringBuilder stringBuilder = new StringBuilder(table).Append('_');
			int namelen = table.Length + 1;
			string read = await readtsk;
			if (read is null)
			{
				throw new InvalidOperationException("This row is already deleted");
			}
			foreach (KeyValuePair<string, string> keyValuePair in Misc.DeserializeObjectWithFastCreate<Dictionary<string, string>>(read))
			{
				if (row[keyValuePair.Key] != keyValuePair.Value)
				{
					throw new Exception("Row desynchronized from database");
				}
			}
			foreach (KeyValuePair<string, string> keyValuePair in row)
			{
				string key = keyValuePair.Key;
				string temp;
				ColumnType columnType = row.GetColumnType(key);
				if (columnType == ColumnType.DataString)
				{
					continue;
				}
				if (columnType == ColumnType.SortedInt)
				{
					temp = stringBuilder.Append("btree_").Append(key).ToString();
					if (!await optimisticExecutionScope.ZTryDelete(temp, id))
					{
						throw new Exception("Index out of sync with data, database may be corrupted (should not reach here)");
					}
				}
				else
				{
					string value = keyValuePair.Value;
					temp = stringBuilder.Append("dictionary_").Append(value.Length).Append('_').Append(value).Append(key).ToString();
					optimisticExecutionScope.Write(temp, null);
				}
				stringBuilder.Remove(namelen, temp.Length - namelen);
			}
			optimisticExecutionScope.Write(id, null);
		}
		public static async IAsyncEnumerable<Row> TableSelectAllOrderedBy(this IOptimisticExecutionScope optimisticExecutionScope, string table, string column, bool reverse)
		{
			Task<string> readtsk = optimisticExecutionScope.Read(table);
			string read1 = new StringBuilder(table).Append("_btree_").Append(column).ToString();
			optimisticExecutionScope = VolatileReadManager.Create(optimisticExecutionScope);
			string read = await readtsk;
			if (read is null)
			{
				throw new InvalidOperationException(new StringBuilder("Table ").Append(table).Append(" does not exist").ToString());
			}
			IReadOnlyDictionary<string, ColumnType> schema = Misc.DeserializeObjectWithFastCreate<Dictionary<string, ColumnType>>(read);
			if (schema[column] != ColumnType.SortedInt)
			{
				throw new InvalidOperationException(new StringBuilder("Column ").Append(column).Append(" of table ").Append(table).Append(" is not sorted int").ToString());
			}
			await foreach (SortedSetReadResult sortedSetReadResult in optimisticExecutionScope.ZSelectAll(read1, reverse))
			{
				Dictionary<string, string> temp = Misc.DeserializeObjectWithFastCreate<Dictionary<string, string>>(await optimisticExecutionScope.Read(sortedSetReadResult.key));
				if (temp[column] == sortedSetReadResult.bigInteger.ToString())
				{
					yield return new Row(sortedSetReadResult.key, table, temp, schema);
					continue;
				}
				//The only way we can get here is database corruption
				throw new Exception("Index out of sync with data, database may be corrupted (should not reach here)");
			}
			yield break;

		}
	}
	[JsonConverter(typeof(StringEnumConverter))]
	public enum ColumnType : byte{
		UniqueString, SortedInt, DataString
	}
	public enum CompareOperator : byte{
		LessThan, LessThanOrEqual, GreaterThanOrEqual, GreaterThan, EqualTo, NotEqualTo
	}
	public sealed class Row : IReadOnlyDictionary<string, string>{
		public readonly string id;
		public readonly string table;
		private readonly Dictionary<string, string> dictionary;
		private readonly int count;
		private readonly IReadOnlyDictionary<string, ColumnType> schema;
		public ColumnType GetColumnType(string column){
			return schema[column];
		}

		internal Row(string id, string table, Dictionary<string, string> dictionary, IReadOnlyDictionary<string, ColumnType> schema)
		{
			this.id = id;
			this.table = table;
			this.dictionary = dictionary;
			this.schema = schema;
			count = dictionary.Count;
		}

		public string this[string key]{
			get => dictionary[key];
			internal set => dictionary[key] = value;
		}

		public IEnumerable<string> Keys => dictionary.Keys;

		public IEnumerable<string> Values => dictionary.Values;

		public int Count => count;

		public bool ContainsKey(string key)
		{
			return dictionary.ContainsKey(key);
		}

		public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
		{
			return dictionary.GetEnumerator();
		}

		public bool TryGetValue(string key, [MaybeNullWhen(false)] out string value)
		{
			return dictionary.TryGetValue(key, out value);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return ((IEnumerable)dictionary).GetEnumerator();
		}
	}
	public readonly struct ReadResult{
		public readonly bool exists;
		public readonly string value;

		public ReadResult(string value)
		{
			this.value = value;
			exists = true;
		}
	}
	public readonly struct BTreeReadResult{
		public readonly string value;
		public readonly BigInteger bigInteger;

		public BTreeReadResult(string value, BigInteger bigInteger)
		{
			this.value = value;
			this.bigInteger = bigInteger;
		}
	}
	public readonly struct SortedSetReadResult{
		public readonly string key;
		public readonly BigInteger bigInteger;
		public readonly string value;

		public SortedSetReadResult(string key, BigInteger bigInteger, string value)
		{
			this.key = key;
			this.bigInteger = bigInteger;
			this.value = value;
		}
	}
	public readonly struct NewKeyValuePair<K, V>{
		public readonly K key;
		public readonly V value;

		public NewKeyValuePair(K key, V value)
		{
			this.key = key;
			this.value = value;
		}
	}
}
