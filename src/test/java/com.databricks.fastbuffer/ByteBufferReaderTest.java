package com.databricks.fastbuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import org.junit.Test;


public class ByteBufferReaderTest {

  @Test
  public void testJavaByteBufferReader() {
    ByteBuffer buf = ByteBuffer.allocate(dataSize);
    putData(buf);
    ByteBufferReader reader = new JavaByteBufferReader(buf);
    testData(reader);
    testPosition(reader);
  }

  @Test
  public void testUnsafeDirectByteBufferReader() {
    ByteBuffer buf = ByteBuffer.allocateDirect(dataSize);
    putData(buf);
    ByteBufferReader reader = new UnsafeDirectByteBufferReader(buf);
    testData(reader);
    testPosition(reader);
  }

  @Test
  public void testUnsafeHeapByteBufferReader() {
    ByteBuffer buf = ByteBuffer.allocate(dataSize);
    putData(buf);
    ByteBufferReader reader = new UnsafeHeapByteBufferReader(buf);
    testData(reader);
    testPosition(reader);
  }

  private static int dataSize = 4 + 8 + 1 + 8 + 4 + 2 + 3 + 2*2 + 3*4 + 3*8 + 3*4 + 3*8;

  private static void putData(ByteBuffer buf) {
    buf.order(ByteOrder.nativeOrder());
    buf.putInt(655350);
    buf.putLong(10034534500L);
    buf.put((byte) 1);
    buf.putDouble(1.5);
    buf.putFloat((float) 2.5);
    buf.putShort((short) 2);
    buf.put(new byte[] {(byte) 15, (byte) 25, (byte) 35});
    buf.putShort((short) 10);
    buf.putShort((short) 1000);
    buf.putInt(655360);
    buf.putInt(23);
    buf.putInt(134);
    buf.putLong(134134234L);
    buf.putLong(-23454352346L);
    buf.putLong(3245245425L);
    buf.putFloat((float) 1.0);
    buf.putFloat((float) 2.0);
    buf.putFloat((float) 4.0);
    buf.putDouble(1.1);
    buf.putDouble(2.2);
    buf.putDouble(3.3);
    buf.rewind();
  }

  private static void testData(ByteBufferReader reader) {
    assertEquals(reader.getInt(), 655350);
    assertEquals(reader.getLong(), 10034534500L);
    assertEquals(reader.getByte(), (byte) 1);
    assertEquals(reader.getDouble(), 1.5, 0);
    assertEquals(reader.getFloat(), 2.5, 0);
    assertEquals(reader.getShort(), (short) 2);

    assertEquals(reader.getByte(), (byte) 15);
    assertEquals(reader.getByte(), (byte) 25);
    assertEquals(reader.getByte(), (byte) 35);

    assertEquals(reader.getShort(), (short) 10);
    assertEquals(reader.getShort(), (short) 1000);

    assertEquals(reader.getInt(), 655360);
    assertEquals(reader.getInt(), 23);
    assertEquals(reader.getInt(), 134);

    assertEquals(reader.getLong(), 134134234L);
    assertEquals(reader.getLong(), -23454352346L);
    assertEquals(reader.getLong(), 3245245425L);

    assertEquals(reader.getFloat(), (float) 1.0, 0);
    assertEquals(reader.getFloat(), (float) 2.0, 0);
    assertEquals(reader.getFloat(), (float) 4.0, 0);

    assertEquals(reader.getDouble(), 1.1, 0);
    assertEquals(reader.getDouble(), 2.2, 0);
    assertEquals(reader.getDouble(), 3.3, 0);

  }

  private static void testPosition(ByteBufferReader reader) {
    reader.position(4);
    assertEquals(reader.getLong(), 10034534500L);
  }
}
