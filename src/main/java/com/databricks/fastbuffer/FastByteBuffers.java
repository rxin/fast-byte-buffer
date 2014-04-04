package com.databricks.fastbuffer;

import java.nio.ByteBuffer;


public class FastByteBuffers {

  public static ByteBufferReader createReader(ByteBuffer buf) {
    try {
      if (buf.hasArray()) {
        return new UnsafeHeapByteBufferReader(buf);
      } else {
        return new UnsafeDirectByteBufferReader(buf);
      }
    } catch (UnsupportedOperationException e) {
      return new JavaByteBufferReader(buf);
    }
  }
}
