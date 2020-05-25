/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.util;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

// Inspired from https://fulmicoton.com/posts/bitpacking/
// Encodes multiple integers in a long to get SIMD-like speedups.

/**
 *  Primitive operations to achieve SIMD-like speedups when encoding/decoding positive integers.
 *  If bitsPerValue &le; 8 then we pack 8 ints per long, a total of 16 long
 *  else if bitsPerValue &le; 16 we pack 4 ints per long, a total of 32 long
 *  else we pack 2 ints per long, a total of 64 long
 *
 * @lucene.internal
 */
public class ForPrimitives64 {

  public static final int BLOCK_SIZE = 64;
  private static final int BLOCK_SIZE_LOG2 = 6;

  private static long expandMask32(long mask32) {
    return mask32 | (mask32 << 32);
  }

  private static long expandMask24(long mask24) {
    //return expandMask32(mask24 | (mask24 << 24));
    return expandMask32(mask24 | (mask24 << 24));
  }

  private static long expandMask16(long mask16) {
    return expandMask32(mask16 | (mask16 << 16));
  }

  private static long expandMask8(long mask8) {
    return expandMask16(mask8 | (mask8 << 8));
  }

  private static long mask32(int bitsPerValue) {
    return expandMask32((1L << bitsPerValue) - 1);
  }

  private static long mask24(int bitsPerValue) {
    return expandMask24((1L << bitsPerValue) - 1);
  }

  private static long mask16(int bitsPerValue) {
    return expandMask16((1L << bitsPerValue) - 1);
  }

  private static long mask8(int bitsPerValue) {
    return expandMask8((1L << bitsPerValue) - 1);
  }


  /**
   * Number of bytes required to encode 128 integers of {@code bitsPerValue} bits per value.
   */
  public static int numBytes(int bitsPerValue) throws IOException {
    return bitsPerValue << (BLOCK_SIZE_LOG2 - 3);
  }

  /**
   * The pattern that this shiftLongs method applies is recognized by the C2
   * compiler, which generates SIMD instructions for it in order to shift
   * multiple longs at once.
   */
  private static void shiftLongs(long[] a, int count, long[] b, int bi, int shift, long mask) {
    for (int i = 0; i < count; ++i) {
      b[bi+i] = (a[i] >>> shift) & mask;
    }
  }

  private static final long MASK8_1 = mask8(1);
  private static final long MASK8_2 = mask8(2);
  private static final long MASK8_3 = mask8(3);
  private static final long MASK8_4 = mask8(4);
  private static final long MASK8_5 = mask8(5);
  private static final long MASK8_6 = mask8(6);
  private static final long MASK8_7 = mask8(7);
  private static final long MASK16_1 = mask16(1);
  private static final long MASK16_2 = mask16(2);
  private static final long MASK16_3 = mask16(3);
  private static final long MASK16_4 = mask16(4);
  private static final long MASK16_5 = mask16(5);
  private static final long MASK16_6 = mask16(6);
  private static final long MASK16_7 = mask16(7);
  private static final long MASK16_9 = mask16(9);
  private static final long MASK16_10 = mask16(10);
  private static final long MASK16_11 = mask16(11);
  private static final long MASK16_12 = mask16(12);
  private static final long MASK16_13 = mask16(13);
  private static final long MASK16_14 = mask16(14);
  private static final long MASK16_15 = mask16(15);
  private static final long MASK24_1 = mask24(1);
  private static final long MASK24_23 = mask24(23);
  private static final long MASK32_1 = mask32(1);
  private static final long MASK32_2 = mask32(2);
  private static final long MASK32_3 = mask32(3);
  private static final long MASK32_4 = mask32(4);
  private static final long MASK32_5 = mask32(5);
  private static final long MASK32_6 = mask32(6);
  private static final long MASK32_7 = mask32(7);
  private static final long MASK32_8 = mask32(8);
  private static final long MASK32_9 = mask32(9);
  private static final long MASK32_10 = mask32(10);
  private static final long MASK32_11 = mask32(11);
  private static final long MASK32_12 = mask32(12);
  private static final long MASK32_13 = mask32(13);
  private static final long MASK32_14 = mask32(14);
  private static final long MASK32_15 = mask32(15);
  private static final long MASK32_17 = mask32(17);
  private static final long MASK32_18 = mask32(18);
  private static final long MASK32_19 = mask32(19);
  private static final long MASK32_20 = mask32(20);
  private static final long MASK32_21 = mask32(21);
  private static final long MASK32_22 = mask32(22);
  private static final long MASK32_23 = mask32(23);
  private static final long MASK32_24 = mask32(24);


  /**
   * Encode 128 integers from {@code longs} into {@code out}. The integers must have the highest significant bit
   * at {@code bitsPerValue} or lower. The {@code tml} long[] must have a length of
   * at least {@link ForPrimitives64#BLOCK_SIZE} / 2
   */
  public static void encode(long[] longs, int bitsPerValue, DataOutput out, long[] tmp, int code) throws IOException {
    final int nextPrimitive;
    System.arraycopy(longs, 0, tmp, 0, 64);
    if (bitsPerValue <= 8) {
      nextPrimitive = 8;
      collapse8(tmp, longs);
    } else if (bitsPerValue <= 16) {
      nextPrimitive = 16;
      collapse16(tmp, longs);
    } else if (bitsPerValue <= 24) {
      nextPrimitive = 24;
      collapse24(tmp, longs);
    } else {
      nextPrimitive = 32;
      collapse32(tmp, longs);
    }

    out.writeByte((byte) (code + nextPrimitive));

    for (int i = 0; i < nextPrimitive; ++i) {
      // Java longs are big endian and we want to read little endian longs, so we need to reverse bytes
      long l = Long.reverseBytes(longs[i]);
      out.writeLong(l);
    }
  }

  private static void collapse8(long[] orig, long[] arr) {
    for (int i = 0, j = 0 ;i < 64; i +=8, j++) {
      arr[j] = (orig[i] << 56) | (orig[i+1] << 48) | (orig[i+2] << 40) | (orig[i+3] << 32) | (orig[i+4] << 24) |
          (orig[i+5] << 16) | (orig[i+6] << 8) | orig[i+7];
    }
  }

  private static void collapse16(long[] orig, long[] arr) {
    for (int i = 0, j = 0; i < 64; i += 4, j++) {
      arr[j] = (orig[i] << 48) | (orig[i+1] << 32) | (orig[i+2] << 16) | orig[i+3];
    }
  }

  private static void collapse24(long[] orig, long[] arr) {
    for (int i = 0, j = 0; i < 64; i += 8, j+=3) {
      arr[j] = (orig[i] << 40) | (orig[i+1] << 16) | ((orig[i+2] >>> 16) << 8) | (orig[i+2] >>> 8);
      arr[j+1] = (orig[i+2]<< 56) | (orig[i+3] << 32) | (orig[i+4] << 8) | (arr[i+5] >>> 16);
      arr[j+2] = ((orig[i+5] >>>8) << 56) | ((orig[i+5]) << 48) |(orig[i+6] << 24) | (orig[i+7]);
    }
  }

  private static void collapse32(long[] orig, long[] arr) {
    for (int i = 0, j = 0; i < 64; i+=2, j++) {
      arr[j] = (orig[i] << 32) | orig[i+1];
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 1
   * into {@code long} with packed representation.
   */
  public static void decode1(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 1);
    shiftLongs(tmp, 1, longs, 0, 7, MASK8_1);
    shiftLongs(tmp, 1, longs, 1, 6, MASK8_1);
    shiftLongs(tmp, 1, longs, 2, 5, MASK8_1);
    shiftLongs(tmp, 1, longs, 3, 4, MASK8_1);
    shiftLongs(tmp, 1, longs, 4, 3, MASK8_1);
    shiftLongs(tmp, 1, longs, 5, 2, MASK8_1);
    shiftLongs(tmp, 1, longs, 6, 1, MASK8_1);
    shiftLongs(tmp, 1, longs, 7, 0, MASK8_1);
  }

  /**
   * Decode 128 integers with highest significant bit of 2
   * into {@code long} with packed representation.
   */
  public static void decode2(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 2);
    shiftLongs(tmp, 2, longs, 0, 6, MASK8_2);
    shiftLongs(tmp, 2, longs, 2, 4, MASK8_2);
    shiftLongs(tmp, 2, longs, 4, 2, MASK8_2);
    shiftLongs(tmp, 2, longs, 6, 0, MASK8_2);
  }

  /**
   * Decode 128 integers with highest significant bit of 4
   * into {@code long} with packed representation.
   */
  public static void decode4(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 4);
    shiftLongs(tmp, 4, longs, 0, 4, MASK8_4);
    shiftLongs(tmp, 4, longs, 4, 0, MASK8_4);
  }


  /**
   * Decode 128 integers with highest significant bit of 8
   * into {@code long} with packed representation.
   */
  public static void decode8(DataInput in, long[] longs) throws IOException {
    in.readLELongs(longs, 0, 8);
  }

  /**
   * Decode 128 integers with highest significant bit of 16
   * into {@code long} with packed representation.
   */
  public static void decode16(DataInput in, long[] longs) throws IOException {
    in.readLELongs(longs, 0, 16);
  }

  /**
   * Decode 128 integers with highest significant bit of 24
   * into {@code long} with packed representation.
   */
  public static void decode24(DataInput in, long[] longs) throws IOException {
    in.readLELongs(longs, 0, 24);
  }

  /**
   * Decode 128 integers with highest significant bit of 24
   * into {@code long} with packed representation.
   */
  public static void decode32(DataInput in, long[] longs) throws IOException {
    in.readLELongs(longs, 0, 32);
  }
}
