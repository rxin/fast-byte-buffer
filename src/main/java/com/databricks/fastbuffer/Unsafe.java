package com.databricks.fastbuffer;

import java.lang.reflect.Field;


class Unsafe {

  static sun.misc.Unsafe UNSAFE;

  static long BYTE_ARRAY_BASE_OFFSET;

  static {
    try {
      Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      UNSAFE = (sun.misc.Unsafe) unsafeField.get(null);
      BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    } catch (NoSuchFieldException e) {
      UNSAFE = null;
    } catch (IllegalAccessException e) {
      UNSAFE = null;
    }
  }
}
