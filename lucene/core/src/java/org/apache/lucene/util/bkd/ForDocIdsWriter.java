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
import org.apache.lucene.util.ForPrimitives64;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.util.ForPrimitives64.BLOCK_SIZE;

final class ForDocIdsWriter {

  private final long[] tmp1 = new long[BLOCK_SIZE];
  private final long[] tmp2 = new long[BLOCK_SIZE];

  /** sole cxtor */
  ForDocIdsWriter() {
  }

  /**
   * Encode 128 integers from {@code ints} into {@code out}.
   */
  void encode(int[] ints, int start, DataOutput out) throws IOException {
    boolean sorted = true;
    for (int i = 1; i < BLOCK_SIZE; ++i) {
      if (ints[start + i - 1] > ints[start + i]) {
        sorted = false;
        break;
      }
    }
    if (sorted) {
      if (ints[start] == ints[start + BLOCK_SIZE - 1]) {
        // all equal, it might happen for multi-value points
        out.writeByte((byte) 0);
        out.writeVInt(ints[start]);
      } else {
        int base  = ints[start];
        tmp1[0] = 0;
        tmp2[0] = ints[start];
        long max = ints[start];
        long maxDelta = 0;
        for (int i = 1; i < BLOCK_SIZE; ++i) {
          final int delta = ints[start + i] - ints[start + i - 1];
          maxDelta |= Integer.toUnsignedLong(delta);
          max |= Integer.toUnsignedLong(ints[start + i]);
          tmp1[i] = delta;
          tmp2[i] = ints[start + i];
        }
        final int bpvDelta = PackedInts.bitsRequired(maxDelta);
        if (bpvDelta == 1 && allEqualOne(tmp1, 1, BLOCK_SIZE)) {
          // special case for consecutive integers
          out.writeByte(Byte.MAX_VALUE);
          out.writeVInt(base);
        } else {
          final int bpv = PackedInts.bitsRequired(max);
          if (bpvDelta < bpv) {
            // for delta encoding we add 32 to bpv
            out.writeByte((byte) (32 + bpvDelta));
            encode(tmp1, bpvDelta, out, tmp2);
            out.writeVInt(base);
          } else {
            // standard encoding, no benefit from delta encoding
            out.writeByte((byte) bpv);
            encode(tmp2, bpv, out, tmp1);
          }
        }
      }
    } else {
      long max = 0;
      int maxVal = Integer.MIN_VALUE;
      int minVal = Integer.MAX_VALUE;
      for (int j = 0; j < BLOCK_SIZE; ++j) {
        max |= Integer.toUnsignedLong(ints[start + j]);
        tmp1[j] = ints[start + j];
        minVal = Math.min(minVal, ints[start + j]);
        maxVal = Math.max(maxVal, ints[start + j]);
      }
      final int bpv = PackedInts.bitsRequired(max);
      final int bvpDiff = PackedInts.bitsRequired(Integer.toUnsignedLong(maxVal - minVal));
      if (bpv > bvpDiff) {
        // for base encoding we add 64 to bvp
        out.writeByte((byte) (64 + bvpDiff));
        for (int i = 0; i < BLOCK_SIZE; ++i) {
          tmp1[i] = ints[start + i] - minVal;
        }
        encode(tmp1, bvpDiff, out, tmp2);
        out.writeVInt(minVal);
      } else {
        // standard encoding
        out.writeByte((byte) bpv);
        encode(tmp1, bpv, out, tmp2);
      }
    }
  }

  private static boolean allEqualOne(long[] longs, int start, int end) {
    for (int i = start; i < end; ++i) {
      if (longs[i] != 1) {
        return false;
      }
    }
    return true;
  }

  /**
   * Decode 128 integers into {@code ints}.
   */
  void decode(DataInput in, int[] ints, int offset) throws IOException {
    final int bitsPerValue = in.readByte();
    decode(bitsPerValue, in, ints, offset, tmp1, tmp2);
  }

  /**
   * Decode 128 integers and feed the result directly to
   * {@link org.apache.lucene.index.PointValues.IntersectVisitor#visit(int)}.
   */
  void decode(DataInput in, PointValues.IntersectVisitor visitor) throws IOException {
    final int code = in.readByte();
    decode(code, in, visitor, tmp1, tmp2);
  }

  private static void encode(long[] longs, int bitsPerValue, DataOutput out, long[] tmp) throws IOException {
    // re-order values for easier decoding
    if (bitsPerValue <= 8) {
      for (int i = 0, j = 0; i < 8; ++i, j += 8) {
        tmp[i] = longs[j];
        tmp[8+i] = longs[j+1];
        tmp[16+i] = longs[j+2];
        tmp[24+i] = longs[j+3];
        tmp[32+i] = longs[j+4];
        tmp[40+i] = longs[j+5];
        tmp[48+i] = longs[j+6];
        tmp[56+i] = longs[j+7];
      }
    } else if (bitsPerValue <= 16) {
      for (int i = 0, j = 0; i < 16; ++i, j += 4) {
        tmp[i] = longs[j];
        tmp[16+i] = longs[j+1];
        tmp[32+i] = longs[j+2];
        tmp[48+i] = longs[j+3];
      }
    } else {
      for (int i = 0, j = 0; i < 32; ++i, j += 2) {
        tmp[i] = longs[j];
        tmp[32+i] = longs[j+1];
      }
    }
    ForPrimitives64.encode(tmp, bitsPerValue, out, longs);
  }

  /**
   * Decode 128 integers into {@code longs}.
   */
  private static void decode(int code, DataInput in, int[] ints, int offset, long[] longs, long[] tmp) throws IOException {
    //System.out.println(code);
    switch (code) {
      case 0:
        final int base = in.readVInt();
        Arrays.fill(ints, offset, offset + BLOCK_SIZE, base);
        break;
      case 1:
        ForPrimitives64.decode1(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 2:
        ForPrimitives64.decode2(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 3:
        ForPrimitives64.decode3(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 4:
        ForPrimitives64.decode4(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 5:
        ForPrimitives64.decode5(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 6:
        ForPrimitives64.decode6(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 7:
        ForPrimitives64.decode7(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 8:
        ForPrimitives64.decode8(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 9:
        ForPrimitives64.decode9(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 10:
        ForPrimitives64.decode10(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 11:
        ForPrimitives64.decode11(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 12:
        ForPrimitives64.decode12(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 13:
        ForPrimitives64.decode13(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 14:
        ForPrimitives64.decode14(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 15:
        ForPrimitives64.decode15(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 16:
        ForPrimitives64.decode16(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 17:
        ForPrimitives64.decode17(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 18:
        ForPrimitives64.decode18(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 19:
        ForPrimitives64.decode19(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 20:
        ForPrimitives64.decode20(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 21:
        ForPrimitives64.decode21(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 22:
        ForPrimitives64.decode22(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 23:
        ForPrimitives64.decode23(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 24:
        ForPrimitives64.decode24(in, tmp, longs);
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
        ForPrimitives64.decodeSlow(code, in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 33:
        ForPrimitives64.decode1(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 34:
        ForPrimitives64.decode2(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 35:
        ForPrimitives64.decode3(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 36:
        ForPrimitives64.decode4(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 37:
        ForPrimitives64.decode5(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 38:
        ForPrimitives64.decode6(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 39:
        ForPrimitives64.decode7(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 40:
        ForPrimitives64.decode8(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 41:
        ForPrimitives64.decode9(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 42:
        ForPrimitives64.decode10(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 43:
        ForPrimitives64.decode11(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 44:
        ForPrimitives64.decode12(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 45:
        ForPrimitives64.decode13(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 46:
        ForPrimitives64.decode14(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 47:
        ForPrimitives64.decode15(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 48:
        ForPrimitives64.decode16(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 49:
        ForPrimitives64.decode17(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 50:
        ForPrimitives64.decode18(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 51:
        ForPrimitives64.decode19(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 52:
        ForPrimitives64.decode20(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 53:
        ForPrimitives64.decode21(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 54:
        ForPrimitives64.decode22(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 55:
        ForPrimitives64.decode23(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 56:
        ForPrimitives64.decode24(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 57:
      case 58:
      case 59:
      case 60:
      case 61:
      case 62:
      case 63:
      case 64:
        ForPrimitives64.decodeSlow(code - 32, in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 65:
        ForPrimitives64.decode1(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 66:
        ForPrimitives64.decode2(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 67:
        ForPrimitives64.decode3(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 68:
        ForPrimitives64.decode4(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 69:
        ForPrimitives64.decode5(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 70:
        ForPrimitives64.decode6(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 71:
        ForPrimitives64.decode7(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 72:
        ForPrimitives64.decode8(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 73:
        ForPrimitives64.decode9(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 74:
        ForPrimitives64.decode10(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 75:
        ForPrimitives64.decode11(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 76:
        ForPrimitives64.decode12(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 77:
        ForPrimitives64.decode13(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 78:
        ForPrimitives64.decode14(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 79:
        ForPrimitives64.decode15(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 80:
        ForPrimitives64.decode16(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 81:
        ForPrimitives64.decode17(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 82:
        ForPrimitives64.decode18(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 83:
        ForPrimitives64.decode19(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 84:
        ForPrimitives64.decode20(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 85:
        ForPrimitives64.decode21(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 86:
        ForPrimitives64.decode22(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 87:
        ForPrimitives64.decode23(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 88:
        ForPrimitives64.decode24(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 89:
      case 90:
      case 91:
      case 92:
      case 93:
      case 94:
      case 95:
      case 96:
        ForPrimitives64.decodeSlow(code - 64, in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case Byte.MAX_VALUE:
        consecutiveIntegers(in, ints, offset);
        break;
      default:
        throw new IllegalArgumentException("Invalid code: " + code);
    }
  }

  private static void decode(int code, DataInput in, PointValues.IntersectVisitor visitor, long[] longs, long[] tmp) throws IOException {
    switch (code) {
      case 0:
        final int base = in.readVInt();
        for (int i = 0; i < BLOCK_SIZE; i++) {
          visitor.visit(base);
        }
        break;
      case 1:
        ForPrimitives64.decode1(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 2:
        ForPrimitives64.decode2(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 3:
        ForPrimitives64.decode3(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 4:
        ForPrimitives64.decode4(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 5:
        ForPrimitives64.decode5(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 6:
        ForPrimitives64.decode6(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 7:
        ForPrimitives64.decode7(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 8:
        ForPrimitives64.decode8(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 9:
        ForPrimitives64.decode9(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 10:
        ForPrimitives64.decode10(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 11:
        ForPrimitives64.decode11(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 12:
        ForPrimitives64.decode12(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 13:
        ForPrimitives64.decode13(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 14:
        ForPrimitives64.decode14(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 15:
        ForPrimitives64.decode15(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 16:
        ForPrimitives64.decode16(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 17:
        ForPrimitives64.decode17(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 18:
        ForPrimitives64.decode18(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 19:
        ForPrimitives64.decode19(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 20:
        ForPrimitives64.decode20(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 21:
        ForPrimitives64.decode21(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 22:
        ForPrimitives64.decode22(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 23:
        ForPrimitives64.decode23(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 24:
        ForPrimitives64.decode24(in, tmp, longs);
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
        ForPrimitives64.decodeSlow(code, in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 33:
        ForPrimitives64.decode1(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 34:
        ForPrimitives64.decode2(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 35:
        ForPrimitives64.decode3(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 36:
        ForPrimitives64.decode4(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 37:
        ForPrimitives64.decode5(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 38:
        ForPrimitives64.decode6(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 39:
        ForPrimitives64.decode7(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 40:
        ForPrimitives64.decode8(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 41:
        ForPrimitives64.decode9(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 42:
        ForPrimitives64.decode10(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 43:
        ForPrimitives64.decode11(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 44:
        ForPrimitives64.decode12(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 45:
        ForPrimitives64.decode13(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 46:
        ForPrimitives64.decode14(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 47:
        ForPrimitives64.decode15(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 48:
        ForPrimitives64.decode16(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 49:
        ForPrimitives64.decode17(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 50:
        ForPrimitives64.decode18(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 51:
        ForPrimitives64.decode19(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 52:
        ForPrimitives64.decode20(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 53:
        ForPrimitives64.decode21(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 54:
        ForPrimitives64.decode22(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 55:
        ForPrimitives64.decode23(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 56:
        ForPrimitives64.decode24(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 57:
      case 58:
      case 59:
      case 60:
      case 61:
      case 62:
      case 63:
      case 64:
        ForPrimitives64.decodeSlow(code - 32, in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 65:
        ForPrimitives64.decode1(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 66:
        ForPrimitives64.decode2(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 67:
        ForPrimitives64.decode3(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 68:
        ForPrimitives64.decode4(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 69:
        ForPrimitives64.decode5(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 70:
        ForPrimitives64.decode6(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 71:
        ForPrimitives64.decode7(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 72:
        ForPrimitives64.decode8(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 73:
        ForPrimitives64.decode9(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 74:
        ForPrimitives64.decode10(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 75:
        ForPrimitives64.decode11(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 76:
        ForPrimitives64.decode12(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 77:
        ForPrimitives64.decode13(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 78:
        ForPrimitives64.decode14(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 79:
        ForPrimitives64.decode15(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 80:
        ForPrimitives64.decode16(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 81:
        ForPrimitives64.decode17(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 82:
        ForPrimitives64.decode18(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 83:
        ForPrimitives64.decode19(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 84:
        ForPrimitives64.decode20(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 85:
        ForPrimitives64.decode21(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 86:
        ForPrimitives64.decode22(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 87:
        ForPrimitives64.decode23(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 88:
        ForPrimitives64.decode24(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 89:
      case 90:
      case 91:
      case 92:
      case 93:
      case 94:
      case 95:
      case 96:
        ForPrimitives64.decodeSlow(code - 64, in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case Byte.MAX_VALUE:
        consecutiveIntegers(in, visitor);
        break;
      default:
        throw new IllegalArgumentException("Invalid code: " + code);
    }
  }

  private static void expand8(long[] arr, int[] ints, int offset) {
    for (int i = 0, j = offset; i < 8; ++i, j += 8) {
      long l = arr[i];
      ints[j]   = (int) ((l >>> 56) & 0xFF);
      ints[j+1] = (int) ((l >>> 48) & 0xFF);
      ints[j+2] = (int) ((l >>> 40) & 0xFF);
      ints[j+3] = (int) ((l >>> 32) & 0xFF);
      ints[j+4] = (int) ((l >>> 24) & 0xFF);
      ints[j+5] = (int) ((l >>> 16) & 0xFF);
      ints[j+6] = (int) ((l >>> 8) & 0xFF);
      ints[j+7] = (int) (l & 0xFF);
    }
  }

  private static void expand8Delta(DataInput in, long[] arr, int[] ints, int offset) throws IOException{
    int base = in.readVInt();
    for (int i = 0, j = offset; i < 8; ++i, j += 8) {
      long l = arr[i];
      ints[j]   = base += (int) ((l >>> 56) & 0xFF);
      ints[j+1] = base += (int) ((l >>> 48) & 0xFF);
      ints[j+2] = base += (int) ((l >>> 40) & 0xFF);
      ints[j+3] = base += (int) ((l >>> 32) & 0xFF);
      ints[j+4] = base += (int) ((l >>> 24) & 0xFF);
      ints[j+5] = base += (int) ((l >>> 16) & 0xFF);
      ints[j+6] = base += (int) ((l >>> 8) & 0xFF);
      ints[j+7] = base += (int) (l & 0xFF);
    }
  }

  private static void expand8Base(DataInput in, long[] arr, int[] ints, int offset) throws IOException{
    final int base = in.readVInt();
    for (int i = 0, j = offset; i < 8; ++i, j += 8) {
      long l = arr[i];
      ints[j]   = base + (int) ((l >>> 56) & 0xFF);
      ints[j+1] = base + (int) ((l >>> 48) & 0xFF);
      ints[j+2] = base + (int) ((l >>> 40) & 0xFF);
      ints[j+3] = base + (int) ((l >>> 32) & 0xFF);
      ints[j+4] = base + (int) ((l >>> 24) & 0xFF);
      ints[j+5] = base + (int) ((l >>> 16) & 0xFF);
      ints[j+6] = base + (int) ((l >>> 8) & 0xFF);
      ints[j+7] = base + (int) (l & 0xFF);
    }
  }

  private static void expand8(long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < 8; ++i) {
      long l = arr[i];
      visitor.visit((int) ((l >>> 56) & 0xFF));
      visitor.visit((int) ((l >>> 48) & 0xFF));
      visitor.visit((int) ((l >>> 40) & 0xFF));
      visitor.visit((int) ((l >>> 32) & 0xFF));
      visitor.visit((int) ((l >>> 24) & 0xFF));
      visitor.visit((int) ((l >>> 16) & 0xFF));
      visitor.visit((int) ((l >>> 8) & 0xFF));
      visitor.visit((int) (l & 0xFF));
    }
  }

  private static void expand8Delta(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    int base = in.readVInt();
    for (int i = 0; i < 8; ++i) {
      long l = arr[i];
      visitor.visit(base += (int) ((l >>> 56) & 0xFF));
      visitor.visit(base += (int) ((l >>> 48) & 0xFF));
      visitor.visit(base += (int) ((l >>> 40) & 0xFF));
      visitor.visit(base += (int) ((l >>> 32) & 0xFF));
      visitor.visit(base += (int) ((l >>> 24) & 0xFF));
      visitor.visit(base += (int) ((l >>> 16) & 0xFF));
      visitor.visit(base += (int) ((l >>> 8) & 0xFF));
      visitor.visit(base += (int) (l & 0xFF));
    }
  }

  private static void expand8Base(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    final int base = in.readVInt();
    for (int i = 0; i < 8; ++i) {
      long l = arr[i];
      visitor.visit(base + (int) ((l >>> 56) & 0xFF));
      visitor.visit(base + (int) ((l >>> 48) & 0xFF));
      visitor.visit(base + (int) ((l >>> 40) & 0xFF));
      visitor.visit(base + (int) ((l >>> 32) & 0xFF));
      visitor.visit(base + (int) ((l >>> 24) & 0xFF));
      visitor.visit(base + (int) ((l >>> 16) & 0xFF));
      visitor.visit(base + (int) ((l >>> 8) & 0xFF));
      visitor.visit(base + (int) (l & 0xFF));
    }
  }

  private static void expand16(long[] arr, int[] ints, int offset) {
    for (int i = 0, j = offset; i < 16; ++i, j += 4) {
      long l = arr[i];
      ints[j]   = (int) ((l >>> 48) & 0xFFFF);
      ints[j+1] = (int) ((l >>> 32) & 0xFFFF);
      ints[j+2] = (int) ((l >>> 16) & 0xFFFF);
      ints[j+3] = (int) (l & 0xFFFF);
    }
  }

  private static void expand16Delta(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    int base = in.readVInt();
    for (int i = 0, j = offset; i < 16; ++i, j += 4) {
      long l = arr[i];
      ints[j]   = base += (int) ((l >>> 48) & 0xFFFF);
      ints[j+1] = base += (int) ((l >>> 32) & 0xFFFF);
      ints[j+2] = base += (int) ((l >>> 16) & 0xFFFF);
      ints[j+3] = base += (int) (l & 0xFFFF);
    }
  }

  private static void expand16Base(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    final int base = in.readVInt();
    for (int i = 0, j = offset; i < 16; ++i, j += 4) {
      long l = arr[i];
      ints[j]   = base + (int) ((l >>> 48) & 0xFFFF);
      ints[j+1] = base + (int) ((l >>> 32) & 0xFFFF);
      ints[j+2] = base + (int) ((l >>> 16) & 0xFFFF);
      ints[j+3] = base + (int) (l & 0xFFFF);
    }
  }

  private static void expand16(long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      visitor.visit((int) ((l >>> 48) & 0xFFFF));
      visitor.visit((int) ((l >>> 32) & 0xFFFF));
      visitor.visit((int) ((l >>> 16) & 0xFFFF));
      visitor.visit((int) (l & 0xFFFF));
    }
  }

  private static void expand16Delta(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    int base = in.readVInt();
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      visitor.visit(base += (int) ((l >>> 48) & 0xFFFF));
      visitor.visit(base += (int) ((l >>> 32) & 0xFFFF));
      visitor.visit(base += (int) ((l >>> 16) & 0xFFFF));
      visitor.visit(base += (int) (l & 0xFFFF));
    }
  }

  private static void expand16Base(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    final int base = in.readVInt();
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      visitor.visit(base + (int) ((l >>> 48) & 0xFFFF));
      visitor.visit(base + (int) ((l >>> 32) & 0xFFFF));
      visitor.visit(base + (int) ((l >>> 16) & 0xFFFF));
      visitor.visit(base + (int) (l & 0xFFFF));
    }
  }

  private static void expand32(long[] arr, int[] ints, int offset) {
    for (int i = 0, j = offset; i < 32; i++, j+=2) {
      long l = arr[i];
      ints[j] = (int) (l >>> 32);
      ints[j+1] = (int) l;
    }
  }

  private static void expand32Delta(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    int base = in.readVInt();
//    for (int i = 0, j = offset; i < 32; i++, j+=2) {
//      long l = arr[i];
//      ints[j]   = base += (int) (l >>> 32);
//      ints[j+1] = base += (int) l;
//    }

    ints[offset]   = base + (int) (arr[0] >>> 32);
    ints[offset+1] = ints[offset] + (int) arr[0];
    ints[offset+2] = ints[offset+1] + (int) (arr[1] >>> 32);
    ints[offset+3] = ints[offset+2] + (int) arr[1];
    ints[offset+4] = ints[offset+3] + (int) (arr[2] >>> 32);
    ints[offset+5] = ints[offset+4] + (int) arr[2];
    ints[offset+6]   = ints[offset+5] + (int) (arr[3] >>> 32);
    ints[offset+7] = ints[offset+6] + (int) arr[3];
    ints[offset+8]   = ints[offset+7] + (int) (arr[4] >>> 32);
    ints[offset+9] = ints[offset+8] + (int) arr[4];
    ints[offset+10]   = ints[offset+9] + (int) (arr[5] >>> 32);
    ints[offset+11] = ints[offset+10] + (int) arr[5];
    ints[offset+12]   = ints[offset+11] + (int) (arr[6] >>> 32);
    ints[offset+13] = ints[offset+12] + (int) arr[6];
    ints[offset+14]   = ints[offset+13] + (int) (arr[7] >>> 32);
    ints[offset+15] = ints[offset+14] + (int) arr[7];
    ints[offset+16]   = ints[offset+15] + (int) (arr[8] >>> 32);
    ints[offset+17] = ints[offset+16] + (int) arr[8];
    ints[offset+18]   = ints[offset+17] + (int) (arr[9] >>> 32);
    ints[offset+19] = ints[offset+18] + (int) arr[9];
    ints[offset+20]   = ints[offset+19] + (int) (arr[10] >>> 32);
    ints[offset+21] = ints[offset+20] + (int) arr[10];
    ints[offset+22]   = ints[offset+21] + (int) (arr[11] >>> 32);
    ints[offset+23] = ints[offset+22] + (int) arr[11];
    ints[offset+24]   = ints[offset+23] + (int) (arr[12] >>> 32);
    ints[offset+25] = ints[offset+24] + (int) arr[12];
    ints[offset+26]   = ints[offset+25] + (int) (arr[13] >>> 32);
    ints[offset+27] = ints[offset+26] + (int) arr[13];
    ints[offset+28]   = ints[offset+27] + (int) (arr[14] >>> 32);
    ints[offset+29] = ints[offset+28] + (int) arr[14];
    ints[offset+30]   = ints[offset+29] + (int) (arr[15] >>> 32);
    ints[offset+31] = ints[offset+30] + (int) arr[15];
    ints[offset+32]   = ints[offset+31] + (int) (arr[16] >>> 32);
    ints[offset+33] = ints[offset+32] + (int) arr[16];
    ints[offset+34]   = ints[offset+33] + (int) (arr[17] >>> 32);
    ints[offset+35] = ints[offset+34] + (int) arr[17];
    ints[offset+36]   = ints[offset+35] + (int) (arr[18] >>> 32);
    ints[offset+37] = ints[offset+36] + (int) arr[18];
    ints[offset+38]   = ints[offset+37] + (int) (arr[19] >>> 32);
    ints[offset+39] = ints[offset+38] + (int) arr[19];
    ints[offset+40]   = ints[offset+39] + (int) (arr[20] >>> 32);
    ints[offset+41] = ints[offset+40] + (int) arr[20];
    ints[offset+42]   = ints[offset+41] + (int) (arr[21] >>> 32);
    ints[offset+43] = ints[offset+42] + (int) arr[21];
    ints[offset+44]   = ints[offset+43] + (int) (arr[22] >>> 32);
    ints[offset+45] = ints[offset+44] + (int) arr[22];
    ints[offset+46]   = ints[offset+45] + (int) (arr[23] >>> 32);
    ints[offset+47] = ints[offset+46] + (int) arr[23];
    ints[offset+48]   = ints[offset+47] + (int) (arr[24] >>> 32);
    ints[offset+49] = ints[offset+48] + (int) arr[24];
    ints[offset+50]   = ints[offset+49] + (int) (arr[25] >>> 32);
    ints[offset+51] = ints[offset+50] + (int) arr[25];
    ints[offset+52]   = ints[offset+51] + (int) (arr[26] >>> 32);
    ints[offset+53] = ints[offset+52] + (int) arr[26];
    ints[offset+54]   = ints[offset+53] + (int) (arr[27] >>> 32);
    ints[offset+55] = ints[offset+54] + (int) arr[27];
    ints[offset+56]   = ints[offset+55] + (int) (arr[28] >>> 32);
    ints[offset+57] = ints[offset+56] + (int) arr[28];
    ints[offset+58]   = ints[offset+57] + (int) (arr[29] >>> 32);
    ints[offset+59] = ints[offset+58] + (int) arr[29];
    ints[offset+60]   = ints[offset+59] + (int) (arr[30] >>> 32);
    ints[offset+61] = ints[offset+60] + (int) arr[30];
    ints[offset+62]   = ints[offset+61] + (int) (arr[31] >>> 32);
    ints[offset+63] = ints[offset+62] + (int) arr[31];
  }

  private static void expand32Base(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    final int base = in.readVInt();
    for (int i = 0, j = offset; i < 32; i++, j+=2) {
      long l = arr[i];
      ints[j]   = base + (int) (l >>> 32);
      ints[j+1] = base + (int) l;
    }
  }

  private static void expand32(long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      visitor.visit((int) (l >>> 32));
      visitor.visit((int) l);
    }
  }

  private static void expand32Delta(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    int base = in.readVInt();
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      visitor.visit(base += (int) (l >>> 32));
      visitor.visit(base += (int) l);
    }
  }

  private static void expand32Base(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    final int base = in.readVInt();
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      visitor.visit(base + (int) (l >>> 32));
      visitor.visit(base + (int) l);
    }
  }

  private static void consecutiveIntegers(DataInput in, int[] ints, int offset) throws IOException {
    int base = in.readVInt();
    for (int i = 0, j = offset; i < BLOCK_SIZE; i++, j++) {
      ints[j] = base + i;
    }
  }

  private static void consecutiveIntegers(DataInput in, PointValues.IntersectVisitor visitor) throws IOException {
    int base = in.readVInt();
    for (int i = 0; i < BLOCK_SIZE; ++i) {
     visitor.visit(base + i);
    }
  }
}
