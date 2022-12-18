using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace LesbianDB
{
	/// <summary>
	/// A non-thread-safe memory stream backed by a memory pool
	/// </summary>
	public sealed class PooledMemoryStream : Stream
	{
		private readonly ArrayPool<byte> arrayPool;
		private byte[] myBuffer;
		public byte[] GetBuffer(){
			return myBuffer;
		}
		private int position;
		private int size;

		public PooledMemoryStream(ArrayPool<byte> arrayPool)
		{
			this.arrayPool = arrayPool;
			myBuffer = arrayPool.Rent(4096);
		}
		public PooledMemoryStream(ArrayPool<byte> arrayPool, int initialCapacity)
		{
			this.arrayPool = arrayPool;
			myBuffer = arrayPool.Rent(initialCapacity);
		}

		protected override void Dispose(bool disposing)
		{
			if(disposing){
				byte[] tmp = myBuffer;
				if (tmp is { })
				{
					myBuffer = null;
					arrayPool.Return(tmp);
				}
			}
		}

		public override bool CanRead => true;

		public override bool CanSeek => true;

		public override bool CanWrite => true;

		public override long Length => size;

		public override long Position { get => position; set => Seek(0, SeekOrigin.Begin); }

		public override void Flush()
		{
			
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			return Read(buffer.AsSpan(offset, count));
		}
		public override int Read(Span<byte> buffer)
		{
			int readable = size - position;
			if(buffer.Length < readable){
				readable = buffer.Length;
			}
			myBuffer.AsSpan(position, readable).CopyTo(buffer);
			position += readable;
			return readable;

		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			int newpos = (int)offset;
			if(origin == SeekOrigin.Current){
				newpos += position;

			} else if(origin == SeekOrigin.End){
				newpos = size - newpos;
			}
			if (newpos < 0)
			{
				throw new IOException("Seek before start of stream");
			}
			if(newpos > size){
				newpos = size;
			}
			position = newpos;
			return newpos;
		}

		public override void SetLength(long value)
		{
			if(position > value){
				Seek(value, SeekOrigin.Begin);
			}
			size = (int)value;
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			Write(buffer.AsSpan(offset, count));
		}
		public override void Write(ReadOnlySpan<byte> buffer)
		{
			int newsize = position + buffer.Length;
			if (size < newsize){
				EnsureCapacity(newsize);
				size = newsize;
			}
			buffer.CopyTo(myBuffer.AsSpan(position, buffer.Length));
			position = newsize;
		}

		private void EnsureCapacity(int capacity){
			byte[] old = myBuffer;
			if (old.Length < size){
				byte[] tempbuffer = arrayPool.Rent(capacity * 2);
				old.CopyTo(tempbuffer, 0);
				myBuffer = tempbuffer;
				arrayPool.Return(old);
			}
		}
	}
}
