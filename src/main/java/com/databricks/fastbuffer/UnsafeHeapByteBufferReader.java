package com.databricks.fastbuffer;

import java.nio.ByteBuffer;


/**
 * An implementation of the ByteBufferReader using sun.misc.Unsafe. This provides very high
 * throughput read of various primitive types from a HeapByteBuffer, but can potentially
 * crash the JVM if the implementation is faulty.
 */
public class UnsafeHeapByteBufferReader implements ByteBufferReader {

  private long offset;
  private byte[] array;

  public UnsafeHeapByteBufferReader(ByteBuffer buf) {
    if (!buf.hasArray()) {
      throw new UnsupportedOperationException("buf (" + buf + ") must have a backing array");
    }
    offset = Unsafe.BYTE_ARRAY_BASE_OFFSET;
    array = buf.array();
  }

  @Override
  public byte getByte() {
    byte v = Unsafe.UNSAFE.getByte(array, offset);
    offset += 1;
    return v;
  }

  @Override
  public byte[] getBytes(byte[] dst, int len) {
    Unsafe.UNSAFE.copyMemory(array, offset, dst, Unsafe.BYTE_ARRAY_BASE_OFFSET, len);
    return dst;
  }

  @Override
  public short getShort() {
    short v = Unsafe.UNSAFE.getShort(array, offset);
    offset += 2;
    return v;
  }

  @Override
  public int getInt() {
    int v = Unsafe.UNSAFE.getInt(array, offset);
    offset += 4;
    return v;
  }

  @Override
  public long getLong() {
    long v = Unsafe.UNSAFE.getLong(array, offset);
    offset += 8;
    return v;
  }

  @Override
  public float getFloat() {
    float v = Unsafe.UNSAFE.getFloat(array, offset);
    offset += 4;
    return v;
  }

  @Override
  public double getDouble() {
    double v = Unsafe.UNSAFE.getDouble(array, offset);
    offset += 8;
    return v;
  }

  @Override
  public int position() {
    return (int) (offset - Unsafe.BYTE_ARRAY_BASE_OFFSET);
  }

  @Override
  public ByteBufferReader position(int newPosition) {
    offset = Unsafe.BYTE_ARRAY_BASE_OFFSET + newPosition;
    return this;
  }
}
