package com.databricks.fastbuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * An implementation of the ByteBufferReader using methods directly from ByteBuffer.
 * This is used as the fallback mode in case Unsafe is not supported.
 */
public class JavaByteBufferReader implements ByteBufferReader {

  private ByteBuffer buf;

  public JavaByteBufferReader(ByteBuffer buf) {
    this.buf = buf.duplicate();
    this.buf.order(ByteOrder.nativeOrder());
  }

  @Override
  public byte getByte() {
    return buf.get();
  }

  @Override
  public byte[] getBytes(byte[] dst, int len) {
    buf.get(dst, 0, len);
    return dst;
  }

  @Override
  public short getShort() {
    return buf.getShort();
  }

  @Override
  public int getInt() {
    return buf.getInt();
  }

  @Override
  public long getLong() {
    return buf.getLong();
  }

  @Override
  public float getFloat() {
    return buf.getFloat();
  }

  @Override
  public double getDouble() {
    return buf.getDouble();
  }

  @Override
  public int position() {
    return buf.position();
  }

  @Override
  public ByteBufferReader position(int newPosition) {
    buf.position(newPosition);
    return this;
  }
}
