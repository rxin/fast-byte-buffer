package com.databricks.fastbuffer;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;


/**
 * An implementation of the ByteBufferReader using sun.misc.Unsafe. This provides very high
 * throughput read of various primitive types from a ByteBuffer, but can potentially
 * crash the JVM if the implementation is faulty.
 */
public class UnsafeDirectByteBufferReader implements ByteBufferReader {

  private long baseOffset;
  private long offset;

  public UnsafeDirectByteBufferReader(ByteBuffer buf) {
    baseOffset = getMemoryAddress(buf);
    offset = baseOffset;
  }

  @Override
  public byte getByte() {
    byte v = Unsafe.UNSAFE.getByte(offset);
    offset += 1;
    return v;
  }

  @Override
  public byte[] getBytes(byte[] dst, int len) {
    Unsafe.UNSAFE.copyMemory(null, offset, dst, Unsafe.BYTE_ARRAY_BASE_OFFSET, len);
    return dst;
  }

  @Override
  public short getShort() {
    short v = Unsafe.UNSAFE.getShort(offset);
    offset += 2;
    return v;
  }

  @Override
  public int getInt() {
    int v = Unsafe.UNSAFE.getInt(offset);
    offset += 4;
    return v;
  }

  @Override
  public long getLong() {
    long v = Unsafe.UNSAFE.getLong(offset);
    offset += 8;
    return v;
  }

  @Override
  public float getFloat() {
    float v = Unsafe.UNSAFE.getFloat(offset);
    offset += 4;
    return v;
  }

  @Override
  public double getDouble() {
    double v = Unsafe.UNSAFE.getDouble(offset);
    offset += 8;
    return v;
  }

  @Override
  public int position() {
    return (int) (offset - baseOffset);
  }

  @Override
  public ByteBufferReader position(int newPosition) {
    offset = baseOffset + newPosition;
    return this;
  }

  private static long getMemoryAddress(ByteBuffer buf) throws UnsupportedOperationException {
    long address;
    try {
      Field addressField = java.nio.Buffer.class.getDeclaredField("address");
      addressField.setAccessible(true);
      address = addressField.getLong(buf);
    } catch (Exception e) {
      throw new UnsupportedOperationException(e);
    }
    return address;
  }
}
