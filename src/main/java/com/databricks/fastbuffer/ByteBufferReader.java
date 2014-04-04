package com.databricks.fastbuffer;


public interface ByteBufferReader {

  public byte getByte();

  public byte[] getBytes(byte[] dst, int len);

  public short getShort();

  public int getInt();

  public long getLong();

  public float getFloat();

  public double getDouble();

  public int position();

  public ByteBufferReader position(int newPosition);
}
