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

// This file has been automatically generated, DO NOT EDIT

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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.SIMDIntegerEncoder;
import org.apache.lucene.util.packed.PackedInts;

final class SIMDDocIdsWriter {

  private final long[] tmp1 = new long[SIMDIntegerEncoder.BLOCK_SIZE];
  private final long[] tmp2 = new long[SIMDIntegerEncoder.BLOCK_SIZE];

  /** sole cxtor */
  SIMDDocIdsWriter() {
  }

  /**
   * Encode 128 integers from {@code ints} into {@code out}.
   */
  void encode(int[] ints, int start, DataOutput out) throws IOException {
    boolean sorted = true;
    for (int i = 1; i < SIMDIntegerEncoder.BLOCK_SIZE; ++i) {
      if (ints[start + i - 1] > ints[start + i]) {
        sorted = false;
        break;
      }
    }
    if (sorted) {
      if (ints[start] == ints[start + SIMDIntegerEncoder.BLOCK_SIZE - 1]) {
        out.writeByte((byte) 0);
        out.writeVInt(ints[start]);
      } else {
        int base  = ints[start];
        tmp1[0] = 0;
        long max = 0;
        for (int i = 1; i < SIMDIntegerEncoder.BLOCK_SIZE; ++i) {
          int doc = ints[start + i] - ints[start + i - 1];
          max |= Integer.toUnsignedLong(doc);
          tmp1[i] = doc;
        }
        int bpv = PackedInts.bitsRequired(max);
        out.writeByte((byte) (32 + bpv));
        out.writeVInt(base);
        encode(tmp1, bpv, out, tmp2);
      }
    } else {
      long max = 0;
      for (int j = 0; j < SIMDIntegerEncoder.BLOCK_SIZE; ++j) {
        max |= Integer.toUnsignedLong(ints[start + j]);
        tmp1[j] = ints[start + j];
      }
      int bpv = PackedInts.bitsRequired(max);
      out.writeByte((byte) bpv);
      encode(tmp1, bpv, out, tmp2);
    }
  }

  /**
   * Decode 128 integers into {@code ints}.
   */
  void decode(DataInput in, int[] ints, int offset) throws IOException {
    final int bitsPerValue = in.readByte();
    decode(bitsPerValue, in, ints, offset, tmp1, tmp2);
  }

  /**
   * Decode 128 integers and calls the method
   * {@link org.apache.lucene.index.PointValues.IntersectVisitor#visit(int)}.
   */
  void decode(DataInput in, PointValues.IntersectVisitor visitor) throws IOException {
    final int bitsPerValue = in.readByte();
    decode(bitsPerValue, in, visitor, tmp1, tmp2);
  }

  private static void encode(long[] longs, int bitsPerValue, DataOutput out, long[] tmp) throws IOException {
    // re-order values for easier decoding
    if (bitsPerValue <= 8) {
      for (int i = 0, j = 0; i < 16; ++i, j += 8) {
        tmp[i] = longs[j];
        tmp[16+i] = longs[j+1];
        tmp[32+i] = longs[j+2];
        tmp[48+i] = longs[j+3];
        tmp[64+i] = longs[j+4];
        tmp[80+i] = longs[j+5];
        tmp[96+i] = longs[j+6];
        tmp[112+i] = longs[j+7];
      }
    } else if (bitsPerValue <= 16) {
      for (int i = 0, j = 0; i < 32; ++i, j += 4) {
        tmp[i] = longs[j];
        tmp[32+i] = longs[j+1];
        tmp[64+i] = longs[j+2];
        tmp[96+i] = longs[j+3];
      }
    } else {
      for (int i = 0, j = 0; i < 64; ++i, j += 2) {
        tmp[i] = longs[j];
        tmp[64+i] = longs[j+1];
      }
    }
    SIMDIntegerEncoder.encode(tmp, bitsPerValue, out, longs);
  }

  /**
   * Decode 128 integers into {@code longs}.
   */
  private static void decode(int bitsPerValue, DataInput in, int[] ints, int offset, long[] longs, long[] tmp) throws IOException {
    final int base;
    switch (bitsPerValue) {
      case 0:
        base = in.readVInt();
        Arrays.fill(ints, offset, offset + SIMDIntegerEncoder.BLOCK_SIZE, base);
        break;
      case 1:
        SIMDIntegerEncoder.decode1(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 2:
        SIMDIntegerEncoder.decode2(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 3:
        SIMDIntegerEncoder.decode3(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 4:
        SIMDIntegerEncoder.decode4(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 5:
        SIMDIntegerEncoder.decode5(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 6:
        SIMDIntegerEncoder.decode6(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 7:
        SIMDIntegerEncoder.decode7(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 8:
        SIMDIntegerEncoder.decode8(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 9:
        SIMDIntegerEncoder.decode9(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 10:
        SIMDIntegerEncoder.decode10(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 11:
        SIMDIntegerEncoder.decode11(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 12:
        SIMDIntegerEncoder.decode12(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 13:
        SIMDIntegerEncoder.decode13(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 14:
        SIMDIntegerEncoder.decode14(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 15:
        SIMDIntegerEncoder.decode15(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 16:
        SIMDIntegerEncoder.decode16(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 17:
        SIMDIntegerEncoder.decode17(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 18:
        SIMDIntegerEncoder.decode18(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 19:
        SIMDIntegerEncoder.decode19(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 20:
        SIMDIntegerEncoder.decode20(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 21:
        SIMDIntegerEncoder.decode21(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 22:
        SIMDIntegerEncoder.decode22(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 23:
        SIMDIntegerEncoder.decode23(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 24:
        SIMDIntegerEncoder.decode24(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 25:
      case 26:
      case 27:
      case 28:
      case 29:
      case 30:
      case 31:
      case 32:
        SIMDIntegerEncoder.decodeSlow(bitsPerValue, in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 33:
        base = in.readVInt();
        SIMDIntegerEncoder.decode1(in, tmp, longs);
        expand8Delta(longs, ints, offset, base);
        break;
      case 34:
        base = in.readVInt();
        SIMDIntegerEncoder.decode2(in, tmp, longs);
        expand8Delta(longs, ints, offset, base);
        break;
      case 35:
        base = in.readVInt();
        SIMDIntegerEncoder.decode3(in, tmp, longs);
        expand8Delta(longs, ints, offset, base);
        break;
      case 36:
        base = in.readVInt();
        SIMDIntegerEncoder.decode4(in, tmp, longs);
        expand8Delta(longs, ints, offset, base);
        break;
      case 37:
        base = in.readVInt();
        SIMDIntegerEncoder.decode5(in, tmp, longs);
        expand8Delta(longs, ints, offset, base);
        break;
      case 38:
        base = in.readVInt();
        SIMDIntegerEncoder.decode6(in, tmp, longs);
        expand8Delta(longs, ints, offset, base);
        break;
      case 39:
        base = in.readVInt();
        SIMDIntegerEncoder.decode7(in, tmp, longs);
        expand8Delta(longs, ints, offset, base);
        break;
      case 40:
        base = in.readVInt();
        SIMDIntegerEncoder.decode8(in, tmp, longs);
        expand8Delta(longs, ints, offset, base);
        break;
      case 41:
        base = in.readVInt();
        SIMDIntegerEncoder.decode9(in, tmp, longs);
        expand16Delta(longs, ints, offset, base);
        break;
      case 42:
        base = in.readVInt();
        SIMDIntegerEncoder.decode10(in, tmp, longs);
        expand16Delta(longs, ints, offset, base);
        break;
      case 43:
        base = in.readVInt();
        SIMDIntegerEncoder.decode11(in, tmp, longs);
        expand16Delta(longs, ints, offset, base);
        break;
      case 44:
        base = in.readVInt();
        SIMDIntegerEncoder.decode12(in, tmp, longs);
        expand16Delta(longs, ints, offset, base);
        break;
      case 45:
        base = in.readVInt();
        SIMDIntegerEncoder.decode13(in, tmp, longs);
        expand16Delta(longs, ints, offset, base);
        break;
      case 46:
        base = in.readVInt();
        SIMDIntegerEncoder.decode14(in, tmp, longs);
        expand16Delta(longs, ints, offset, base);
        break;
      case 47:
        base = in.readVInt();
        SIMDIntegerEncoder.decode15(in, tmp, longs);
        expand16Delta(longs, ints, offset, base);
        break;
      case 48:
        base = in.readVInt();
        SIMDIntegerEncoder.decode16(in, tmp, longs);
        expand16Delta(longs, ints, offset, base);
        break;
      case 49:
        base = in.readVInt();
        SIMDIntegerEncoder.decode17(in, tmp, longs);
        expand32Delta(longs, ints, offset, base);
        break;
      case 50:
        base = in.readVInt();
        SIMDIntegerEncoder.decode18(in, tmp, longs);
        expand32Delta(longs, ints, offset, base);
        break;
      case 51:
        base = in.readVInt();
        SIMDIntegerEncoder.decode19(in, tmp, longs);
        expand32Delta(longs, ints, offset, base);
        break;
      case 52:
        base = in.readVInt();
        SIMDIntegerEncoder.decode20(in, tmp, longs);
        expand32Delta(longs, ints, offset, base);
        break;
      case 53:
        base = in.readVInt();
        SIMDIntegerEncoder.decode21(in, tmp, longs);
        expand32Delta(longs, ints, offset, base);
        break;
      case 54:
        base = in.readVInt();
        SIMDIntegerEncoder.decode22(in, tmp, longs);
        expand32Delta(longs, ints, offset, base);
        break;
      case 55:
        base = in.readVInt();
        SIMDIntegerEncoder.decode23(in, tmp, longs);
        expand32Delta(longs, ints, offset, base);
        break;
      case 56:
        base = in.readVInt();
        SIMDIntegerEncoder.decode24(in, tmp, longs);
        expand32Delta(longs, ints, offset, base);
        break;
      default:
        base = in.readVInt();
        SIMDIntegerEncoder.decodeSlow(bitsPerValue - 32, in, tmp, longs);
        expand32Delta(longs, ints, offset, base);
        break;
    }
  }

  private static void decode(int bitsPerValue, DataInput in, PointValues.IntersectVisitor visitor, long[] longs, long[] tmp) throws IOException {
    final int base;
    switch (bitsPerValue) {
      case 0:
        base = in.readVInt();
        for (int i = 0; i < SIMDIntegerEncoder.BLOCK_SIZE; i++) {
          visitor.visit(base);
        }
        break;
      case 1:
        SIMDIntegerEncoder.decode1(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 2:
        SIMDIntegerEncoder.decode2(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 3:
        SIMDIntegerEncoder.decode3(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 4:
        SIMDIntegerEncoder.decode4(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 5:
        SIMDIntegerEncoder.decode5(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 6:
        SIMDIntegerEncoder.decode6(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 7:
        SIMDIntegerEncoder.decode7(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 8:
        SIMDIntegerEncoder.decode8(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 9:
        SIMDIntegerEncoder.decode9(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 10:
        SIMDIntegerEncoder.decode10(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 11:
        SIMDIntegerEncoder.decode11(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 12:
        SIMDIntegerEncoder.decode12(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 13:
        SIMDIntegerEncoder.decode13(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 14:
        SIMDIntegerEncoder.decode14(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 15:
        SIMDIntegerEncoder.decode15(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 16:
        SIMDIntegerEncoder.decode16(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 17:
        SIMDIntegerEncoder.decode17(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 18:
        SIMDIntegerEncoder.decode18(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 19:
        SIMDIntegerEncoder.decode19(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 20:
        SIMDIntegerEncoder.decode20(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 21:
        SIMDIntegerEncoder.decode21(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 22:
        SIMDIntegerEncoder.decode22(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 23:
        SIMDIntegerEncoder.decode23(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 24:
        SIMDIntegerEncoder.decode24(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 25:
      case 26:
      case 27:
      case 28:
      case 29:
      case 30:
      case 31:
      case 32:
        SIMDIntegerEncoder.decodeSlow(bitsPerValue, in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 33:
        base = in.readVInt();
        SIMDIntegerEncoder.decode1(in, tmp, longs);
        expand8Delta(longs, visitor, base);
        break;
      case 34:
        base = in.readVInt();
        SIMDIntegerEncoder.decode2(in, tmp, longs);
        expand8Delta(longs, visitor, base);
        break;
      case 35:
        base = in.readVInt();
        SIMDIntegerEncoder.decode3(in, tmp, longs);
        expand8Delta(longs, visitor, base);
        break;
      case 36:
        base = in.readVInt();
        SIMDIntegerEncoder.decode4(in, tmp, longs);
        expand8Delta(longs, visitor, base);
        break;
      case 37:
        base = in.readVInt();
        SIMDIntegerEncoder.decode5(in, tmp, longs);
        expand8Delta(longs, visitor, base);
        break;
      case 38:
        base = in.readVInt();
        SIMDIntegerEncoder.decode6(in, tmp, longs);
        expand8Delta(longs, visitor, base);
        break;
      case 39:
        base = in.readVInt();
        SIMDIntegerEncoder.decode7(in, tmp, longs);
        expand8Delta(longs, visitor, base);
        break;
      case 40:
        base = in.readVInt();
        SIMDIntegerEncoder.decode8(in, tmp, longs);
        expand8Delta(longs, visitor, base);
        break;
      case 41:
        base = in.readVInt();
        SIMDIntegerEncoder.decode9(in, tmp, longs);
        expand16Delta(longs, visitor, base);
        break;
      case 42:
        base = in.readVInt();
        SIMDIntegerEncoder.decode10(in, tmp, longs);
        expand16Delta(longs, visitor, base);
        break;
      case 43:
        base = in.readVInt();
        SIMDIntegerEncoder.decode11(in, tmp, longs);
        expand16Delta(longs, visitor, base);
        break;
      case 44:
        base = in.readVInt();
        SIMDIntegerEncoder.decode12(in, tmp, longs);
        expand16Delta(longs, visitor, base);
        break;
      case 45:
        base = in.readVInt();
        SIMDIntegerEncoder.decode13(in, tmp, longs);
        expand16Delta(longs, visitor, base);
        break;
      case 46:
        base = in.readVInt();
        SIMDIntegerEncoder.decode14(in, tmp, longs);
        expand16Delta(longs, visitor, base);
        break;
      case 47:
        base = in.readVInt();
        SIMDIntegerEncoder.decode15(in, tmp, longs);
        expand16Delta(longs, visitor, base);
        break;
      case 48:
        base = in.readVInt();
        SIMDIntegerEncoder.decode16(in, tmp, longs);
        expand16Delta(longs, visitor, base);
        break;
      case 49:
        base = in.readVInt();
        SIMDIntegerEncoder.decode17(in, tmp, longs);
        expand32Delta(longs, visitor, base);
        break;
      case 50:
        base = in.readVInt();
        SIMDIntegerEncoder.decode18(in, tmp, longs);
        expand32Delta(longs, visitor, base);
        break;
      case 51:
        base = in.readVInt();
        SIMDIntegerEncoder.decode19(in, tmp, longs);
        expand32Delta(longs, visitor, base);
        break;
      case 52:
        base = in.readVInt();
        SIMDIntegerEncoder.decode20(in, tmp, longs);
        expand32Delta(longs, visitor, base);
        break;
      case 53:
        base = in.readVInt();
        SIMDIntegerEncoder.decode21(in, tmp, longs);
        expand32Delta(longs, visitor, base);
        break;
      case 54:
        base = in.readVInt();
        SIMDIntegerEncoder.decode22(in, tmp, longs);
        expand32Delta(longs, visitor, base);
        break;
      case 55:
        base = in.readVInt();
        SIMDIntegerEncoder.decode23(in, tmp, longs);
        expand32Delta(longs, visitor, base);
        break;
      case 56:
        base = in.readVInt();
        SIMDIntegerEncoder.decode24(in, tmp, longs);
        expand32Delta(longs, visitor, base);
        break;
      default:
        base = in.readVInt();
        SIMDIntegerEncoder.decodeSlow(bitsPerValue - 32, in, tmp, longs);
        expand32Delta(longs, visitor, base);
        break;
    }
  }

  private static void expand8(long[] arr, int[] ints, int offset) {
    for (int i = 0, j = offset; i < 16; ++i, j += 8) {
      long l = arr[i];
      ints[j]   = (int) ((l >>> 56) & 0xFFL);
      ints[j+1] = (int) ((l >>> 48) & 0xFFL);
      ints[j+2] = (int) ((l >>> 40) & 0xFFL);
      ints[j+3] = (int) ((l >>> 32) & 0xFFL);
      ints[j+4] = (int) ((l >>> 24) & 0xFFL);
      ints[j+5] = (int) ((l >>> 16) & 0xFFL);
      ints[j+6] = (int) ((l >>> 8) & 0xFFL);
      ints[j+7] = (int) (l & 0xFFL);
    }
  }

  private static void expand8Delta(long[] arr, int[] ints, int offset, int base) {
    for (int i = 0, j = offset; i < 16; ++i, j += 8) {
      long l = arr[i];
      ints[j]   = base += (int) ((l >>> 56) & 0xFFL);
      ints[j+1] = base +=  (int) ((l >>> 48) & 0xFFL);
      ints[j+2] = base +=  (int) ((l >>> 40) & 0xFFL);
      ints[j+3] = base +=  (int) ((l >>> 32) & 0xFFL);
      ints[j+4] = base +=  (int) ((l >>> 24) & 0xFFL);
      ints[j+5] = base +=  (int) ((l >>> 16) & 0xFFL);
      ints[j+6] = base +=  (int) ((l >>> 8) & 0xFFL);
      ints[j+7] = base +=  (int) (l & 0xFFL);
    }
  }

  private static void expand8(long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      visitor.visit((int) ((l >>> 56) & 0xFFL));
      visitor.visit((int) ((l >>> 48) & 0xFFL));
      visitor.visit((int) ((l >>> 40) & 0xFFL));
      visitor.visit((int) ((l >>> 32) & 0xFFL));
      visitor.visit((int) ((l >>> 24) & 0xFFL));
      visitor.visit((int) ((l >>> 16) & 0xFFL));
      visitor.visit((int) ((l >>> 8) & 0xFFL));
      visitor.visit((int) (l & 0xFFL));
    }
  }

  private static void expand8Delta(long[] arr, PointValues.IntersectVisitor visitor, int base) throws IOException {
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      visitor.visit(base += (int) ((l >>> 56) & 0xFFL));
      visitor.visit(base += (int) ((l >>> 48) & 0xFFL));
      visitor.visit(base += (int) ((l >>> 40) & 0xFFL));
      visitor.visit(base += (int) ((l >>> 32) & 0xFFL));
      visitor.visit(base += (int) ((l >>> 24) & 0xFFL));
      visitor.visit(base += (int) ((l >>> 16) & 0xFFL));
      visitor.visit(base += (int) ((l >>> 8) & 0xFFL));
      visitor.visit(base += (int) (l & 0xFFL));
    }
  }

  private static void expand16(long[] arr, int[] ints, int offset) {
    for (int i = 0, j = offset; i < 32; ++i, j += 4) {
      long l = arr[i];
      ints[j] = (int) ((l >>> 48) & 0xFFFFL);
      ints[j+1] = (int) ((l >>> 32) & 0xFFFFL);
      ints[j+2] = (int) ((l >>> 16) & 0xFFFFL);
      ints[j+3] = (int) (l & 0xFFFFL);
    }
  }

  private static void expand16Delta(long[] arr, int[] ints, int offset, int base) {
    for (int i = 0, j = offset; i < 32; ++i, j += 4) {
      long l = arr[i];
      ints[j] = base += (int) ((l >>> 48) & 0xFFFFL);
      ints[j+1] =  base += (int) ((l >>> 32) & 0xFFFFL);
      ints[j+2] =  base += (int) ((l >>> 16) & 0xFFFFL);
      ints[j+3] =  base += (int) (l & 0xFFFFL);
    }
  }

  private static void expand16(long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      visitor.visit((int) ((l >>> 48) & 0xFFFFL));
      visitor.visit((int) ((l >>> 32) & 0xFFFFL));
      visitor.visit((int) ((l >>> 16) & 0xFFFFL));
      visitor.visit((int) (l & 0xFFFFL));
    }
  }

  private static void expand16Delta(long[] arr, PointValues.IntersectVisitor visitor, int base) throws IOException {
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      visitor.visit(base += (int) ((l >>> 48) & 0xFFFFL));
      visitor.visit(base += (int) ((l >>> 32) & 0xFFFFL));
      visitor.visit(base += (int) ((l >>> 16) & 0xFFFFL));
      visitor.visit(base += (int) (l & 0xFFFFL));
    }
  }

  private static void expand32(long[] arr, int[] ints, int offset) {
    for (int i = 0, j = offset; i < 64; i++, j+=2) {
      long l = arr[i];
      ints[j] = (int) (l >>> 32);
      ints[j+1] = (int) (l & 0xFFFFFFFFL);
    }
  }

  private static void expand32Delta(long[] arr, int[] ints, int offset, int base) {
    for (int i = 0, j = offset; i < 64; i++, j+=2) {
      long l = arr[i];
      ints[j] = base += (int) (l >>> 32);
      ints[j+1] = base += (int) (l & 0xFFFFFFFFL);
    }
  }

  private static void expand32(long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      visitor.visit((int) (l >>> 32));
      visitor.visit((int) (l & 0xFFFFFFFFL));
    }
  }

  private static void expand32Delta(long[] arr, PointValues.IntersectVisitor visitor, int base) throws IOException {
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      visitor.visit(base += (int) (l >>> 32));
      visitor.visit(base += (int) (l & 0xFFFFFFFFL));
    }
  }
}
