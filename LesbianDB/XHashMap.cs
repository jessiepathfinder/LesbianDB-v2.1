using System;
using System.Collections.Generic;
using System.Text;
using System.Security.Cryptography;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Collections;
using System.Runtime.InteropServices;
using System.Numerics;

namespace LesbianDB
{
	public sealed class Hash256{
		public readonly ulong first;
		public readonly ulong second;
		public readonly ulong third;
		public readonly ulong fourth;
		private static readonly ulong salt;
		static Hash256(){
			Span<ulong> span = stackalloc ulong[8];
			RandomNumberGenerator.Fill(MemoryMarshal.AsBytes(span));
			salt = span[0];
		}
		public Hash256(string str){
			Span<ulong> span = stackalloc ulong[6];
			using(SHA384 sha384 = SHA384.Create()){
				sha384.TryComputeHash(MemoryMarshal.AsBytes(str.AsSpan()), MemoryMarshal.AsBytes(span), out _);
			}
			first = span[0];
			second = span[1];
			third = span[2];
			fourth = span[3];
		}
		public override bool Equals(object obj)
		{
			if(obj is null){
				return false;
			}
			if(obj is Hash256 hash256){
				return hash256.first == first & hash256.second == second & hash256.third == third & hash256.fourth == fourth;
			}
			return false;
		}
		public override int GetHashCode()
		{
			//Should not be a big deal, since we can only get Hash256 by hashing strings
			return (first ^ salt).GetHashCode();
		}
	}
	public sealed class XHashMap<T> : Dictionary<Hash256, T>, IDictionary<string, T>
	{
		public T this[string key] { get => this[new Hash256(key)]; set => this[new Hash256(key)] = value; }

		public bool IsReadOnly => false;

		ICollection<string> IDictionary<string, T>.Keys => throw new NotImplementedException();

		ICollection<T> IDictionary<string, T>.Values => Values;

		public void Add(string key, T value)
		{
			Add(new Hash256(key), value);
		}

		public void Add(KeyValuePair<string, T> item)
		{
			Add(new Hash256(item.Key), item.Value);
		}

		public bool Contains(KeyValuePair<string, T> item)
		{
			return ((IDictionary<Hash256, T>)this).Contains(new KeyValuePair<Hash256, T>(new Hash256(item.Key), item.Value));
		}

		public bool ContainsKey(string key)
		{
			return ContainsKey(new Hash256(key));
		}

		public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex)
		{
			throw new NotImplementedException();
		}

		public bool Remove(string key)
		{
			return Remove(new Hash256(key));
		}

		public bool Remove(KeyValuePair<string, T> item)
		{
			return ((IDictionary<Hash256, T>)this).Remove(new KeyValuePair<Hash256, T>(new Hash256(item.Key), item.Value));
		}

		public bool TryGetValue(string key, out T value)
		{
			return TryGetValue(new Hash256(key), out value);
		}

		IEnumerator<KeyValuePair<string, T>> IEnumerable<KeyValuePair<string, T>>.GetEnumerator()
		{
			throw new NotImplementedException();
		}
	}
	public sealed class ConcurrentXHashMap<T> : ConcurrentDictionary<Hash256, T>, IDictionary<string, T>
	{
		public T this[string key] { get => this[new Hash256(key)]; set => this[new Hash256(key)] = value; }

		public bool IsReadOnly => false;

		ICollection<string> IDictionary<string, T>.Keys => throw new NotImplementedException();

		public void Add(string key, T value)
		{
			if(TryAdd(new Hash256(key), value)){
				return;
			}
			throw new ArgumentException();
		}

		public void Add(KeyValuePair<string, T> item)
		{
			Add(item.Key, item.Value);
		}

		public bool Contains(KeyValuePair<string, T> item)
		{
			throw new NotImplementedException();
		}

		public bool ContainsKey(string key)
		{
			return ContainsKey(new Hash256(key));
		}

		public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex)
		{
			throw new NotImplementedException();
		}

		public bool Remove(string key)
		{
			return TryRemove(new Hash256(key), out _);
		}

		public bool Remove(KeyValuePair<string, T> item)
		{
			throw new NotImplementedException();
		}

		public bool TryGetValue(string key, out T value)
		{
			return TryGetValue(new Hash256(key), out value);
		}

		IEnumerator<KeyValuePair<string, T>> IEnumerable<KeyValuePair<string, T>>.GetEnumerator()
		{
			throw new NotImplementedException();
		}
	}
}
