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
public class ForPrimitives {

  public static final int BLOCK_SIZE = 128;
  private static final int BLOCK_SIZE_LOG2 = 7;

  private static long expandMask32(long mask32) {
    return mask32 | (mask32 << 32);
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
   * at least {@link ForPrimitives#BLOCK_SIZE} / 2
   */
  public static void encode(long[] longs, int bitsPerValue, DataOutput out, long[] tmp) throws IOException {
    final int nextPrimitive;
    final int numLongs;
    if (bitsPerValue <= 8) {
      nextPrimitive = 8;
      numLongs = BLOCK_SIZE / 8;
      collapse8(longs);
    } else if (bitsPerValue <= 16) {
      nextPrimitive = 16;
      numLongs = BLOCK_SIZE / 4;
      collapse16(longs);
    } else {
      nextPrimitive = 32;
      numLongs = BLOCK_SIZE / 2;
      collapse32(longs);
    }

    final int numLongsPerShift = bitsPerValue * 2;
    int idx = 0;
    int shift = nextPrimitive - bitsPerValue;
    for (int i = 0; i < numLongsPerShift; ++i) {
      tmp[i] = longs[idx++] << shift;
    }
    for (shift = shift - bitsPerValue; shift >= 0; shift -= bitsPerValue) {
      for (int i = 0; i < numLongsPerShift; ++i) {
        tmp[i] |= longs[idx++] << shift;
      }
    }

    final int remainingBitsPerLong = shift + bitsPerValue;
    final long maskRemainingBitsPerLong;
    if (nextPrimitive == 8) {
      maskRemainingBitsPerLong = mask8(remainingBitsPerLong);
    } else if (nextPrimitive == 16) {
      maskRemainingBitsPerLong = mask16(remainingBitsPerLong);
    } else {
      maskRemainingBitsPerLong = mask32(remainingBitsPerLong);
    }

    int tmpIdx = 0;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < numLongs) {
      if (remainingBitsPerValue > remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        tmp[tmpIdx++] |= (longs[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;
        if (nextPrimitive == 8) {
          mask1 = mask8(remainingBitsPerValue);
          mask2 = mask8(remainingBitsPerLong - remainingBitsPerValue);
        } else if (nextPrimitive == 16) {
          mask1 = mask16(remainingBitsPerValue);
          mask2 = mask16(remainingBitsPerLong - remainingBitsPerValue);
        } else {
          mask1 = mask32(remainingBitsPerValue);
          mask2 = mask32(remainingBitsPerLong - remainingBitsPerValue);
        }
        tmp[tmpIdx] |= (longs[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        tmp[tmpIdx++] |= (longs[idx] >>> remainingBitsPerValue) & mask2;
      }
    }

    for (int i = 0; i < numLongsPerShift; ++i) {
      // Java longs are big endian and we want to read little endian longs, so we need to reverse bytes
      long l = Long.reverseBytes(tmp[i]);
      out.writeLong(l);
    }
  }

  private static void collapse8(long[] arr) {
    for (int i = 0; i < 16; ++i) {
      arr[i] = (arr[i] << 56) | (arr[16+i] << 48) | (arr[32+i] << 40) | (arr[48+i] << 32) | (arr[64+i] << 24) | (arr[80+i] << 16) | (arr[96+i] << 8) | arr[112+i];
    }
  }

  private static void collapse16(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      arr[i] = (arr[i] << 48) | (arr[32+i] << 32) | (arr[64+i] << 16) | arr[96+i];
    }
  }

  private static void collapse32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      arr[i] = (arr[i] << 32) | arr[64+i];
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 1
   * into {@code long} with packed representation.
   */
  public static void decode1(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 2);
    shiftLongs(tmp, 2, longs, 0, 7, MASK8_1);
    shiftLongs(tmp, 2, longs, 2, 6, MASK8_1);
    shiftLongs(tmp, 2, longs, 4, 5, MASK8_1);
    shiftLongs(tmp, 2, longs, 6, 4, MASK8_1);
    shiftLongs(tmp, 2, longs, 8, 3, MASK8_1);
    shiftLongs(tmp, 2, longs, 10, 2, MASK8_1);
    shiftLongs(tmp, 2, longs, 12, 1, MASK8_1);
    shiftLongs(tmp, 2, longs, 14, 0, MASK8_1);
  }

  /**
   * Decode 128 integers with highest significant bit of 2
   * into {@code long} with packed representation.
   */
  public static void decode2(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 4);
    shiftLongs(tmp, 4, longs, 0, 6, MASK8_2);
    shiftLongs(tmp, 4, longs, 4, 4, MASK8_2);
    shiftLongs(tmp, 4, longs, 8, 2, MASK8_2);
    shiftLongs(tmp, 4, longs, 12, 0, MASK8_2);
  }

  /**
   * Decode 128 integers with highest significant bit of 3
   * into {@code long} with packed representation.
   */
  public static void decode3(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 6);
    shiftLongs(tmp, 6, longs, 0, 5, MASK8_3);
    shiftLongs(tmp, 6, longs, 6, 2, MASK8_3);
    for (int iter = 0, tmpIdx = 0, longsIdx = 12; iter < 2; ++iter, tmpIdx += 3, longsIdx += 2) {
      long l0 = (tmp[tmpIdx+0] & MASK8_2) << 1;
      l0 |= (tmp[tmpIdx+1] >>> 1) & MASK8_1;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASK8_1) << 2;
      l1 |= (tmp[tmpIdx+2] & MASK8_2) << 0;
      longs[longsIdx+1] = l1;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 4
   * into {@code long} with packed representation.
   */
  public static void decode4(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 8);
    shiftLongs(tmp, 8, longs, 0, 4, MASK8_4);
    shiftLongs(tmp, 8, longs, 8, 0, MASK8_4);
  }

  /**
   * Decode 128 integers with highest significant bit of 5
   * into {@code long} with packed representation.
   */
  public static void decode5(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 10);
    shiftLongs(tmp, 10, longs, 0, 3, MASK8_5);
    for (int iter = 0, tmpIdx = 0, longsIdx = 10; iter < 2; ++iter, tmpIdx += 5, longsIdx += 3) {
      long l0 = (tmp[tmpIdx+0] & MASK8_3) << 2;
      l0 |= (tmp[tmpIdx+1] >>> 1) & MASK8_2;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASK8_1) << 4;
      l1 |= (tmp[tmpIdx+2] & MASK8_3) << 1;
      l1 |= (tmp[tmpIdx+3] >>> 2) & MASK8_1;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+3] & MASK8_2) << 3;
      l2 |= (tmp[tmpIdx+4] & MASK8_3) << 0;
      longs[longsIdx+2] = l2;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 6
   * into {@code long} with packed representation.
   */
  public static void decode6(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 12);
    shiftLongs(tmp, 12, longs, 0, 2, MASK8_6);
    for (int iter = 0, tmpIdx = 0, longsIdx = 12; iter < 4; ++iter, tmpIdx += 3, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASK8_2) << 4;
      l0 |= (tmp[tmpIdx+1] & MASK8_2) << 2;
      l0 |= (tmp[tmpIdx+2] & MASK8_2) << 0;
      longs[longsIdx+0] = l0;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 7
   * into {@code long} with packed representation.
   */
  public static void decode7(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 14);
    shiftLongs(tmp, 14, longs, 0, 1, MASK8_7);
    for (int iter = 0, tmpIdx = 0, longsIdx = 14; iter < 2; ++iter, tmpIdx += 7, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASK8_1) << 6;
      l0 |= (tmp[tmpIdx+1] & MASK8_1) << 5;
      l0 |= (tmp[tmpIdx+2] & MASK8_1) << 4;
      l0 |= (tmp[tmpIdx+3] & MASK8_1) << 3;
      l0 |= (tmp[tmpIdx+4] & MASK8_1) << 2;
      l0 |= (tmp[tmpIdx+5] & MASK8_1) << 1;
      l0 |= (tmp[tmpIdx+6] & MASK8_1) << 0;
      longs[longsIdx+0] = l0;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 8
   * into {@code long} with packed representation.
   */
  public static void decode8(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(longs, 0, 16);
  }

  /**
   * Decode 128 integers with highest significant bit of 9
   * into {@code long} with packed representation.
   */
  public static void decode9(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 18);
    shiftLongs(tmp, 18, longs, 0, 7, MASK16_9);
    for (int iter = 0, tmpIdx = 0, longsIdx = 18; iter < 2; ++iter, tmpIdx += 9, longsIdx += 7) {
      long l0 = (tmp[tmpIdx+0] & MASK16_7) << 2;
      l0 |= (tmp[tmpIdx+1] >>> 5) & MASK16_2;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASK16_5) << 4;
      l1 |= (tmp[tmpIdx+2] >>> 3) & MASK16_4;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+2] & MASK16_3) << 6;
      l2 |= (tmp[tmpIdx+3] >>> 1) & MASK16_6;
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+3] & MASK16_1) << 8;
      l3 |= (tmp[tmpIdx+4] & MASK16_7) << 1;
      l3 |= (tmp[tmpIdx+5] >>> 6) & MASK16_1;
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+5] & MASK16_6) << 3;
      l4 |= (tmp[tmpIdx+6] >>> 4) & MASK16_3;
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+6] & MASK16_4) << 5;
      l5 |= (tmp[tmpIdx+7] >>> 2) & MASK16_5;
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+7] & MASK16_2) << 7;
      l6 |= (tmp[tmpIdx+8] & MASK16_7) << 0;
      longs[longsIdx+6] = l6;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 10
   * into {@code long} with packed representation.
   */
  public static void decode10(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 20);
    shiftLongs(tmp, 20, longs, 0, 6, MASK16_10);
    for (int iter = 0, tmpIdx = 0, longsIdx = 20; iter < 4; ++iter, tmpIdx += 5, longsIdx += 3) {
      long l0 = (tmp[tmpIdx+0] & MASK16_6) << 4;
      l0 |= (tmp[tmpIdx+1] >>> 2) & MASK16_4;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASK16_2) << 8;
      l1 |= (tmp[tmpIdx+2] & MASK16_6) << 2;
      l1 |= (tmp[tmpIdx+3] >>> 4) & MASK16_2;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+3] & MASK16_4) << 6;
      l2 |= (tmp[tmpIdx+4] & MASK16_6) << 0;
      longs[longsIdx+2] = l2;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 11
   * into {@code long} with packed representation.
   */
  public static void decode11(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 22);
    shiftLongs(tmp, 22, longs, 0, 5, MASK16_11);
    for (int iter = 0, tmpIdx = 0, longsIdx = 22; iter < 2; ++iter, tmpIdx += 11, longsIdx += 5) {
      long l0 = (tmp[tmpIdx+0] & MASK16_5) << 6;
      l0 |= (tmp[tmpIdx+1] & MASK16_5) << 1;
      l0 |= (tmp[tmpIdx+2] >>> 4) & MASK16_1;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+2] & MASK16_4) << 7;
      l1 |= (tmp[tmpIdx+3] & MASK16_5) << 2;
      l1 |= (tmp[tmpIdx+4] >>> 3) & MASK16_2;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+4] & MASK16_3) << 8;
      l2 |= (tmp[tmpIdx+5] & MASK16_5) << 3;
      l2 |= (tmp[tmpIdx+6] >>> 2) & MASK16_3;
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+6] & MASK16_2) << 9;
      l3 |= (tmp[tmpIdx+7] & MASK16_5) << 4;
      l3 |= (tmp[tmpIdx+8] >>> 1) & MASK16_4;
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+8] & MASK16_1) << 10;
      l4 |= (tmp[tmpIdx+9] & MASK16_5) << 5;
      l4 |= (tmp[tmpIdx+10] & MASK16_5) << 0;
      longs[longsIdx+4] = l4;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 12
   * into {@code long} with packed representation.
   */
  public static void decode12(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 24);
    shiftLongs(tmp, 24, longs, 0, 4, MASK16_12);
    for (int iter = 0, tmpIdx = 0, longsIdx = 24; iter < 8; ++iter, tmpIdx += 3, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASK16_4) << 8;
      l0 |= (tmp[tmpIdx+1] & MASK16_4) << 4;
      l0 |= (tmp[tmpIdx+2] & MASK16_4) << 0;
      longs[longsIdx+0] = l0;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 13
   * into {@code long} with packed representation.
   */
  public static void decode13(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 26);
    shiftLongs(tmp, 26, longs, 0, 3, MASK16_13);
    for (int iter = 0, tmpIdx = 0, longsIdx = 26; iter < 2; ++iter, tmpIdx += 13, longsIdx += 3) {
      long l0 = (tmp[tmpIdx+0] & MASK16_3) << 10;
      l0 |= (tmp[tmpIdx+1] & MASK16_3) << 7;
      l0 |= (tmp[tmpIdx+2] & MASK16_3) << 4;
      l0 |= (tmp[tmpIdx+3] & MASK16_3) << 1;
      l0 |= (tmp[tmpIdx+4] >>> 2) & MASK16_1;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+4] & MASK16_2) << 11;
      l1 |= (tmp[tmpIdx+5] & MASK16_3) << 8;
      l1 |= (tmp[tmpIdx+6] & MASK16_3) << 5;
      l1 |= (tmp[tmpIdx+7] & MASK16_3) << 2;
      l1 |= (tmp[tmpIdx+8] >>> 1) & MASK16_2;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+8] & MASK16_1) << 12;
      l2 |= (tmp[tmpIdx+9] & MASK16_3) << 9;
      l2 |= (tmp[tmpIdx+10] & MASK16_3) << 6;
      l2 |= (tmp[tmpIdx+11] & MASK16_3) << 3;
      l2 |= (tmp[tmpIdx+12] & MASK16_3) << 0;
      longs[longsIdx+2] = l2;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 14
   * into {@code long} with packed representation.
   */
  public static void decode14(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 28);
    shiftLongs(tmp, 28, longs, 0, 2, MASK16_14);
    for (int iter = 0, tmpIdx = 0, longsIdx = 28; iter < 4; ++iter, tmpIdx += 7, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASK16_2) << 12;
      l0 |= (tmp[tmpIdx+1] & MASK16_2) << 10;
      l0 |= (tmp[tmpIdx+2] & MASK16_2) << 8;
      l0 |= (tmp[tmpIdx+3] & MASK16_2) << 6;
      l0 |= (tmp[tmpIdx+4] & MASK16_2) << 4;
      l0 |= (tmp[tmpIdx+5] & MASK16_2) << 2;
      l0 |= (tmp[tmpIdx+6] & MASK16_2) << 0;
      longs[longsIdx+0] = l0;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 15
   * into {@code long} with packed representation.
   */
  public static void decode15(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 30);
    shiftLongs(tmp, 30, longs, 0, 1, MASK16_15);
    for (int iter = 0, tmpIdx = 0, longsIdx = 30; iter < 2; ++iter, tmpIdx += 15, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASK16_1) << 14;
      l0 |= (tmp[tmpIdx+1] & MASK16_1) << 13;
      l0 |= (tmp[tmpIdx+2] & MASK16_1) << 12;
      l0 |= (tmp[tmpIdx+3] & MASK16_1) << 11;
      l0 |= (tmp[tmpIdx+4] & MASK16_1) << 10;
      l0 |= (tmp[tmpIdx+5] & MASK16_1) << 9;
      l0 |= (tmp[tmpIdx+6] & MASK16_1) << 8;
      l0 |= (tmp[tmpIdx+7] & MASK16_1) << 7;
      l0 |= (tmp[tmpIdx+8] & MASK16_1) << 6;
      l0 |= (tmp[tmpIdx+9] & MASK16_1) << 5;
      l0 |= (tmp[tmpIdx+10] & MASK16_1) << 4;
      l0 |= (tmp[tmpIdx+11] & MASK16_1) << 3;
      l0 |= (tmp[tmpIdx+12] & MASK16_1) << 2;
      l0 |= (tmp[tmpIdx+13] & MASK16_1) << 1;
      l0 |= (tmp[tmpIdx+14] & MASK16_1) << 0;
      longs[longsIdx+0] = l0;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 16
   * into {@code long} with packed representation.
   */
  public static void decode16(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(longs, 0, 32);
  }

  /**
   * Decode 128 integers with highest significant bit of 17
   * into {@code long} with packed representation.
   */
  public static void decode17(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 34);
    shiftLongs(tmp, 34, longs, 0, 15, MASK32_17);
    for (int iter = 0, tmpIdx = 0, longsIdx = 34; iter < 2; ++iter, tmpIdx += 17, longsIdx += 15) {
      long l0 = (tmp[tmpIdx+0] & MASK32_15) << 2;
      l0 |= (tmp[tmpIdx+1] >>> 13) & MASK32_2;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASK32_13) << 4;
      l1 |= (tmp[tmpIdx+2] >>> 11) & MASK32_4;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+2] & MASK32_11) << 6;
      l2 |= (tmp[tmpIdx+3] >>> 9) & MASK32_6;
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+3] & MASK32_9) << 8;
      l3 |= (tmp[tmpIdx+4] >>> 7) & MASK32_8;
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+4] & MASK32_7) << 10;
      l4 |= (tmp[tmpIdx+5] >>> 5) & MASK32_10;
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+5] & MASK32_5) << 12;
      l5 |= (tmp[tmpIdx+6] >>> 3) & MASK32_12;
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+6] & MASK32_3) << 14;
      l6 |= (tmp[tmpIdx+7] >>> 1) & MASK32_14;
      longs[longsIdx+6] = l6;
      long l7 = (tmp[tmpIdx+7] & MASK32_1) << 16;
      l7 |= (tmp[tmpIdx+8] & MASK32_15) << 1;
      l7 |= (tmp[tmpIdx+9] >>> 14) & MASK32_1;
      longs[longsIdx+7] = l7;
      long l8 = (tmp[tmpIdx+9] & MASK32_14) << 3;
      l8 |= (tmp[tmpIdx+10] >>> 12) & MASK32_3;
      longs[longsIdx+8] = l8;
      long l9 = (tmp[tmpIdx+10] & MASK32_12) << 5;
      l9 |= (tmp[tmpIdx+11] >>> 10) & MASK32_5;
      longs[longsIdx+9] = l9;
      long l10 = (tmp[tmpIdx+11] & MASK32_10) << 7;
      l10 |= (tmp[tmpIdx+12] >>> 8) & MASK32_7;
      longs[longsIdx+10] = l10;
      long l11 = (tmp[tmpIdx+12] & MASK32_8) << 9;
      l11 |= (tmp[tmpIdx+13] >>> 6) & MASK32_9;
      longs[longsIdx+11] = l11;
      long l12 = (tmp[tmpIdx+13] & MASK32_6) << 11;
      l12 |= (tmp[tmpIdx+14] >>> 4) & MASK32_11;
      longs[longsIdx+12] = l12;
      long l13 = (tmp[tmpIdx+14] & MASK32_4) << 13;
      l13 |= (tmp[tmpIdx+15] >>> 2) & MASK32_13;
      longs[longsIdx+13] = l13;
      long l14 = (tmp[tmpIdx+15] & MASK32_2) << 15;
      l14 |= (tmp[tmpIdx+16] & MASK32_15) << 0;
      longs[longsIdx+14] = l14;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 18
   * into {@code long} with packed representation.
   */
  public static void decode18(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 36);
    shiftLongs(tmp, 36, longs, 0, 14, MASK32_18);
    for (int iter = 0, tmpIdx = 0, longsIdx = 36; iter < 4; ++iter, tmpIdx += 9, longsIdx += 7) {
      long l0 = (tmp[tmpIdx+0] & MASK32_14) << 4;
      l0 |= (tmp[tmpIdx+1] >>> 10) & MASK32_4;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASK32_10) << 8;
      l1 |= (tmp[tmpIdx+2] >>> 6) & MASK32_8;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+2] & MASK32_6) << 12;
      l2 |= (tmp[tmpIdx+3] >>> 2) & MASK32_12;
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+3] & MASK32_2) << 16;
      l3 |= (tmp[tmpIdx+4] & MASK32_14) << 2;
      l3 |= (tmp[tmpIdx+5] >>> 12) & MASK32_2;
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+5] & MASK32_12) << 6;
      l4 |= (tmp[tmpIdx+6] >>> 8) & MASK32_6;
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+6] & MASK32_8) << 10;
      l5 |= (tmp[tmpIdx+7] >>> 4) & MASK32_10;
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+7] & MASK32_4) << 14;
      l6 |= (tmp[tmpIdx+8] & MASK32_14) << 0;
      longs[longsIdx+6] = l6;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 19
   * into {@code long} with packed representation.
   */
  public static void decode19(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 38);
    shiftLongs(tmp, 38, longs, 0, 13, MASK32_19);
    for (int iter = 0, tmpIdx = 0, longsIdx = 38; iter < 2; ++iter, tmpIdx += 19, longsIdx += 13) {
      long l0 = (tmp[tmpIdx+0] & MASK32_13) << 6;
      l0 |= (tmp[tmpIdx+1] >>> 7) & MASK32_6;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASK32_7) << 12;
      l1 |= (tmp[tmpIdx+2] >>> 1) & MASK32_12;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+2] & MASK32_1) << 18;
      l2 |= (tmp[tmpIdx+3] & MASK32_13) << 5;
      l2 |= (tmp[tmpIdx+4] >>> 8) & MASK32_5;
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+4] & MASK32_8) << 11;
      l3 |= (tmp[tmpIdx+5] >>> 2) & MASK32_11;
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+5] & MASK32_2) << 17;
      l4 |= (tmp[tmpIdx+6] & MASK32_13) << 4;
      l4 |= (tmp[tmpIdx+7] >>> 9) & MASK32_4;
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+7] & MASK32_9) << 10;
      l5 |= (tmp[tmpIdx+8] >>> 3) & MASK32_10;
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+8] & MASK32_3) << 16;
      l6 |= (tmp[tmpIdx+9] & MASK32_13) << 3;
      l6 |= (tmp[tmpIdx+10] >>> 10) & MASK32_3;
      longs[longsIdx+6] = l6;
      long l7 = (tmp[tmpIdx+10] & MASK32_10) << 9;
      l7 |= (tmp[tmpIdx+11] >>> 4) & MASK32_9;
      longs[longsIdx+7] = l7;
      long l8 = (tmp[tmpIdx+11] & MASK32_4) << 15;
      l8 |= (tmp[tmpIdx+12] & MASK32_13) << 2;
      l8 |= (tmp[tmpIdx+13] >>> 11) & MASK32_2;
      longs[longsIdx+8] = l8;
      long l9 = (tmp[tmpIdx+13] & MASK32_11) << 8;
      l9 |= (tmp[tmpIdx+14] >>> 5) & MASK32_8;
      longs[longsIdx+9] = l9;
      long l10 = (tmp[tmpIdx+14] & MASK32_5) << 14;
      l10 |= (tmp[tmpIdx+15] & MASK32_13) << 1;
      l10 |= (tmp[tmpIdx+16] >>> 12) & MASK32_1;
      longs[longsIdx+10] = l10;
      long l11 = (tmp[tmpIdx+16] & MASK32_12) << 7;
      l11 |= (tmp[tmpIdx+17] >>> 6) & MASK32_7;
      longs[longsIdx+11] = l11;
      long l12 = (tmp[tmpIdx+17] & MASK32_6) << 13;
      l12 |= (tmp[tmpIdx+18] & MASK32_13) << 0;
      longs[longsIdx+12] = l12;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 20
   * into {@code long} with packed representation.
   */
  public static void decode20(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 40);
    shiftLongs(tmp, 40, longs, 0, 12, MASK32_20);
    for (int iter = 0, tmpIdx = 0, longsIdx = 40; iter < 8; ++iter, tmpIdx += 5, longsIdx += 3) {
      long l0 = (tmp[tmpIdx+0] & MASK32_12) << 8;
      l0 |= (tmp[tmpIdx+1] >>> 4) & MASK32_8;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASK32_4) << 16;
      l1 |= (tmp[tmpIdx+2] & MASK32_12) << 4;
      l1 |= (tmp[tmpIdx+3] >>> 8) & MASK32_4;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+3] & MASK32_8) << 12;
      l2 |= (tmp[tmpIdx+4] & MASK32_12) << 0;
      longs[longsIdx+2] = l2;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 21
   * into {@code long} with packed representation.
   */
  public static void decode21(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 42);
    shiftLongs(tmp, 42, longs, 0, 11, MASK32_21);
    for (int iter = 0, tmpIdx = 0, longsIdx = 42; iter < 2; ++iter, tmpIdx += 21, longsIdx += 11) {
      long l0 = (tmp[tmpIdx+0] & MASK32_11) << 10;
      l0 |= (tmp[tmpIdx+1] >>> 1) & MASK32_10;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASK32_1) << 20;
      l1 |= (tmp[tmpIdx+2] & MASK32_11) << 9;
      l1 |= (tmp[tmpIdx+3] >>> 2) & MASK32_9;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+3] & MASK32_2) << 19;
      l2 |= (tmp[tmpIdx+4] & MASK32_11) << 8;
      l2 |= (tmp[tmpIdx+5] >>> 3) & MASK32_8;
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+5] & MASK32_3) << 18;
      l3 |= (tmp[tmpIdx+6] & MASK32_11) << 7;
      l3 |= (tmp[tmpIdx+7] >>> 4) & MASK32_7;
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+7] & MASK32_4) << 17;
      l4 |= (tmp[tmpIdx+8] & MASK32_11) << 6;
      l4 |= (tmp[tmpIdx+9] >>> 5) & MASK32_6;
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+9] & MASK32_5) << 16;
      l5 |= (tmp[tmpIdx+10] & MASK32_11) << 5;
      l5 |= (tmp[tmpIdx+11] >>> 6) & MASK32_5;
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+11] & MASK32_6) << 15;
      l6 |= (tmp[tmpIdx+12] & MASK32_11) << 4;
      l6 |= (tmp[tmpIdx+13] >>> 7) & MASK32_4;
      longs[longsIdx+6] = l6;
      long l7 = (tmp[tmpIdx+13] & MASK32_7) << 14;
      l7 |= (tmp[tmpIdx+14] & MASK32_11) << 3;
      l7 |= (tmp[tmpIdx+15] >>> 8) & MASK32_3;
      longs[longsIdx+7] = l7;
      long l8 = (tmp[tmpIdx+15] & MASK32_8) << 13;
      l8 |= (tmp[tmpIdx+16] & MASK32_11) << 2;
      l8 |= (tmp[tmpIdx+17] >>> 9) & MASK32_2;
      longs[longsIdx+8] = l8;
      long l9 = (tmp[tmpIdx+17] & MASK32_9) << 12;
      l9 |= (tmp[tmpIdx+18] & MASK32_11) << 1;
      l9 |= (tmp[tmpIdx+19] >>> 10) & MASK32_1;
      longs[longsIdx+9] = l9;
      long l10 = (tmp[tmpIdx+19] & MASK32_10) << 11;
      l10 |= (tmp[tmpIdx+20] & MASK32_11) << 0;
      longs[longsIdx+10] = l10;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 22
   * into {@code long} with packed representation.
   */
  public static void decode22(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 44);
    shiftLongs(tmp, 44, longs, 0, 10, MASK32_22);
    for (int iter = 0, tmpIdx = 0, longsIdx = 44; iter < 4; ++iter, tmpIdx += 11, longsIdx += 5) {
      long l0 = (tmp[tmpIdx+0] & MASK32_10) << 12;
      l0 |= (tmp[tmpIdx+1] & MASK32_10) << 2;
      l0 |= (tmp[tmpIdx+2] >>> 8) & MASK32_2;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+2] & MASK32_8) << 14;
      l1 |= (tmp[tmpIdx+3] & MASK32_10) << 4;
      l1 |= (tmp[tmpIdx+4] >>> 6) & MASK32_4;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+4] & MASK32_6) << 16;
      l2 |= (tmp[tmpIdx+5] & MASK32_10) << 6;
      l2 |= (tmp[tmpIdx+6] >>> 4) & MASK32_6;
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+6] & MASK32_4) << 18;
      l3 |= (tmp[tmpIdx+7] & MASK32_10) << 8;
      l3 |= (tmp[tmpIdx+8] >>> 2) & MASK32_8;
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+8] & MASK32_2) << 20;
      l4 |= (tmp[tmpIdx+9] & MASK32_10) << 10;
      l4 |= (tmp[tmpIdx+10] & MASK32_10) << 0;
      longs[longsIdx+4] = l4;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 23
   * into {@code long} with packed representation.
   */
  public static void decode23(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 46);
    shiftLongs(tmp, 46, longs, 0, 9, MASK32_23);
    for (int iter = 0, tmpIdx = 0, longsIdx = 46; iter < 2; ++iter, tmpIdx += 23, longsIdx += 9) {
      long l0 = (tmp[tmpIdx+0] & MASK32_9) << 14;
      l0 |= (tmp[tmpIdx+1] & MASK32_9) << 5;
      l0 |= (tmp[tmpIdx+2] >>> 4) & MASK32_5;
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+2] & MASK32_4) << 19;
      l1 |= (tmp[tmpIdx+3] & MASK32_9) << 10;
      l1 |= (tmp[tmpIdx+4] & MASK32_9) << 1;
      l1 |= (tmp[tmpIdx+5] >>> 8) & MASK32_1;
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+5] & MASK32_8) << 15;
      l2 |= (tmp[tmpIdx+6] & MASK32_9) << 6;
      l2 |= (tmp[tmpIdx+7] >>> 3) & MASK32_6;
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+7] & MASK32_3) << 20;
      l3 |= (tmp[tmpIdx+8] & MASK32_9) << 11;
      l3 |= (tmp[tmpIdx+9] & MASK32_9) << 2;
      l3 |= (tmp[tmpIdx+10] >>> 7) & MASK32_2;
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+10] & MASK32_7) << 16;
      l4 |= (tmp[tmpIdx+11] & MASK32_9) << 7;
      l4 |= (tmp[tmpIdx+12] >>> 2) & MASK32_7;
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+12] & MASK32_2) << 21;
      l5 |= (tmp[tmpIdx+13] & MASK32_9) << 12;
      l5 |= (tmp[tmpIdx+14] & MASK32_9) << 3;
      l5 |= (tmp[tmpIdx+15] >>> 6) & MASK32_3;
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+15] & MASK32_6) << 17;
      l6 |= (tmp[tmpIdx+16] & MASK32_9) << 8;
      l6 |= (tmp[tmpIdx+17] >>> 1) & MASK32_8;
      longs[longsIdx+6] = l6;
      long l7 = (tmp[tmpIdx+17] & MASK32_1) << 22;
      l7 |= (tmp[tmpIdx+18] & MASK32_9) << 13;
      l7 |= (tmp[tmpIdx+19] & MASK32_9) << 4;
      l7 |= (tmp[tmpIdx+20] >>> 5) & MASK32_4;
      longs[longsIdx+7] = l7;
      long l8 = (tmp[tmpIdx+20] & MASK32_5) << 18;
      l8 |= (tmp[tmpIdx+21] & MASK32_9) << 9;
      l8 |= (tmp[tmpIdx+22] & MASK32_9) << 0;
      longs[longsIdx+8] = l8;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of 24
   * into {@code long} with packed representation.
   */
  public static void decode24(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 48);
    shiftLongs(tmp, 48, longs, 0, 8, MASK32_24);
    for (int iter = 0, tmpIdx = 0, longsIdx = 48; iter < 16; ++iter, tmpIdx += 3, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASK32_8) << 16;
      l0 |= (tmp[tmpIdx+1] & MASK32_8) << 8;
      l0 |= (tmp[tmpIdx+2] & MASK32_8) << 0;
      longs[longsIdx+0] = l0;
    }
  }

  /**
   * Decode 128 integers with highest significant bit of {@code bitsPerValue}
   * into {@code long} with packed representation.
   */
  public static void decodeSlow(int bitsPerValue, DataInput in, long[] tmp, long[] longs) throws IOException {
    final int numLongs = bitsPerValue << 1;
    in.readLELongs(tmp, 0, numLongs);
    final long mask = mask32(bitsPerValue);
    int longsIdx = 0;
    int shift = 32 - bitsPerValue;
    for (; shift >= 0; shift -= bitsPerValue) {
      shiftLongs(tmp, numLongs, longs, longsIdx, shift, mask);
      longsIdx += numLongs;
    }
    final int remainingBitsPerLong = shift + bitsPerValue;
    final long mask32RemainingBitsPerLong = mask32(remainingBitsPerLong);
    int tmpIdx = 0;
    int remainingBits = remainingBitsPerLong;
    for (; longsIdx < BLOCK_SIZE / 2; ++longsIdx) {
      int b = bitsPerValue - remainingBits;
      long l = (tmp[tmpIdx++] & mask32(remainingBits)) << b;
      while (b >= remainingBitsPerLong) {
        b -= remainingBitsPerLong;
        l |= (tmp[tmpIdx++] & mask32RemainingBitsPerLong) << b;
      }
      if (b > 0) {
        l |= (tmp[tmpIdx] >>> (remainingBitsPerLong-b)) & mask32(b);
        remainingBits = remainingBitsPerLong - b;
      } else {
        remainingBits = remainingBitsPerLong;
      }
      longs[longsIdx] = l;
    }
  }
}
