using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;

namespace LesbianDB
{
	public readonly struct YuriHash
	{
		private readonly ulong[] table;
		public YuriHash(ulong[] table)
		{
			if (table.Length == 128) {
				this.table = table;
			} else {
				throw new ArgumentException("Vector must be 128 units long");
			}
		}
		private YuriHash(ulong[] table, bool n)
		{
			this.table = table;
		}

		public static YuriHash GetRandom(){
			ulong[] vector = new ulong[128];
			RandomNumberGenerator.Fill(MemoryMarshal.AsBytes(vector.AsSpan()));
			return new YuriHash(vector, false);
		}

		public YuriHash(ReadOnlySpan<byte> bytes){
			table = new ulong[128];
			Span<byte> span = MemoryMarshal.AsBytes(table.AsSpan());
			using SHA512 sha512 = SHA512.Create();
			sha512.TryComputeHash(bytes, span.Slice(0, 64), out _);
			sha512.TryComputeHash(span.Slice(0, 64), span.Slice(64, 64), out _);
			for (int i = 0; i < 832; i += 64)
			{
				sha512.TryComputeHash(span.Slice(i, 128), span.Slice(i + 128, 64), out _);
			}
		}

		public ulong Permute(ulong x) {
			ulong sum = 0;
			for (byte i = 0; i < 64; ++i) {
				ulong mask = 1UL << i;
				ulong counterparty = table[(i * 2UL) + ((mask & x) / mask)];
				unchecked {
					sum += counterparty;
				}
			}
			return sum;
		}
	}
	public readonly struct YuriStringHash{
		private readonly YuriHash[] yuriHashes;

		public YuriStringHash(YuriHash x, YuriHash y)
		{
			//NOTE: both of these YuriHashes must be initialized with diffrent random vectors
			yuriHashes = new YuriHash[] { x, y };
		}
		public ulong HashString(ReadOnlySpan<byte> bytes){
			ulong state = 13536186186642527470 + (ulong)bytes.Length;
			foreach(byte x in bytes){
				for(int i = 0; i < 8; ++i){
					int mask = 1 << i;
					state = yuriHashes[(x & mask) / mask].Permute(state);
				}
			}

			//Length Extension Attack protection

			return yuriHashes[0].Permute(state);
		}
	}
}
