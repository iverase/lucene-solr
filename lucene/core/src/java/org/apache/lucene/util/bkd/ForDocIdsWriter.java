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
          if (primitive(bpvDelta) < primitive(bpv)) {
            // for delta encoding we add 32 to bpv
            encode(tmp1, bpvDelta, out, tmp2, 32);
            out.writeVInt(base);
          } else {
            // standard encoding, no benefit from delta encoding
            encode(tmp2, bpv, out, tmp1, 0);
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
      final int bpv = primitive(PackedInts.bitsRequired(max));
      final int bvpDiff = primitive(PackedInts.bitsRequired(Integer.toUnsignedLong(maxVal - minVal)));
      if (bvpDiff < bpv) {
        // for base encoding we add 64 to bvp
        for (int i = 0; i < BLOCK_SIZE; ++i) {
          tmp1[i] = ints[start + i] - minVal;
        }
        encode(tmp1, bvpDiff, out, tmp2, 64);
        out.writeVInt(minVal);
      } else {
        // standard encoding
        encode(tmp1, bpv, out, tmp2, 0);
      }
    }
  }

  private int primitive(int x) {
    if (x <= 8) {
      return 8;
    } else if (x <= 16) {
      return 16;
    } else if(x <= 24) {
      return 24;
    } else {
      return 32;
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

  private static void encode(long[] longs, int bitsPerValue, DataOutput out, long[] tmp, int code) throws IOException {
    ForPrimitives64.encode(longs, bitsPerValue, out, tmp, code);
  }

  /**
   * Decode 128 integers into {@code longs}.
   */
  private static void decode(int code, DataInput in, int[] ints, int offset, long[] longs, long[] tmp) throws IOException {
   // System.out.println(code);
    switch (code) {
      case 0:
        final int base = in.readVInt();
        Arrays.fill(ints, offset, offset + BLOCK_SIZE, base);
        break;
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
      case 6:
      case 7:
      case 8:
        ForPrimitives64.decode8(in, longs);
        expand8(longs, ints, offset);
        break;
      case 9:
      case 10:
      case 11:
      case 12:
      case 13:
      case 14:
      case 15:
      case 16:
        ForPrimitives64.decode16(in, longs);
        expand16(longs, ints, offset);
        break;
      case 17:
      case 18:
      case 19:
      case 20:
      case 21:
      case 22:
      case 23:
      case 24:
        ForPrimitives64.decode24(in, longs);
        expand24(longs, ints, offset);
        break;
      case 25:
      case 26:
      case 27:
      case 28:
      case 29:
      case 30:
      case 31:
      case 32:
        ForPrimitives64.decode32(in, longs);
        expand32(longs, ints, offset);
        break;
      case 33:
      case 34:
      case 35:
      case 36:
      case 37:
      case 38:
      case 39:
      case 40:
        ForPrimitives64.decode8(in, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 41:
      case 42:
      case 43:
      case 44:
      case 45:
      case 46:
      case 47:
      case 48:
        ForPrimitives64.decode16(in, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 49:
      case 50:
      case 51:
      case 52:
      case 53:
      case 54:
      case 55:
      case 56:
        ForPrimitives64.decode24(in, longs);
        expand24Delta(in, longs, ints, offset);
        break;
      case 57:
      case 58:
      case 59:
      case 60:
      case 61:
      case 62:
      case 63:
      case 64:
        ForPrimitives64.decode32(in, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 65:
      case 66:
      case 67:
      case 68:
      case 69:
      case 70:
      case 71:
      case 72:
        ForPrimitives64.decode8(in, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 73:
      case 74:
      case 75:
      case 76:
      case 77:
      case 78:
      case 79:
      case 80:
        ForPrimitives64.decode16(in, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 81:
      case 82:
      case 83:
      case 84:
      case 85:
      case 86:
      case 87:
      case 88:
        ForPrimitives64.decode24(in, longs);
        expand24Base(in, longs, ints, offset);
        break;
      case 89:
      case 90:
      case 91:
      case 92:
      case 93:
      case 94:
      case 95:
      case 96:
        ForPrimitives64.decode32(in, longs);
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
      case 2:
      case 3:
      case 4:
      case 5:
      case 6:
      case 7:
      case 8:
        ForPrimitives64.decode8(in, longs);
        expand8(longs, visitor);
        break;
      case 9:
      case 10:
      case 11:
      case 12:
      case 13:
      case 14:
      case 15:
      case 16:
        ForPrimitives64.decode16(in, longs);
        expand16(longs, visitor);
        break;
      case 17:
      case 18:
      case 19:
      case 20:
      case 21:
      case 22:
      case 23:
      case 24:
        ForPrimitives64.decode24(in, longs);
        expand24(longs, visitor);
        break;
      case 25:
      case 26:
      case 27:
      case 28:
      case 29:
      case 30:
      case 31:
      case 32:
        ForPrimitives64.decode32(in, longs);
        expand32(longs, visitor);
        break;
      case 33:
      case 34:
      case 35:
      case 36:
      case 37:
      case 38:
      case 39:
      case 40:
        ForPrimitives64.decode8(in, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 41:
      case 42:
      case 43:
      case 44:
      case 45:
      case 46:
      case 47:
      case 48:
        ForPrimitives64.decode16(in, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 49:
      case 50:
      case 51:
      case 52:
      case 53:
      case 54:
      case 55:
      case 56:
        ForPrimitives64.decode24(in, longs);
        expand24Delta(in, longs, visitor);
        break;
      case 57:
      case 58:
      case 59:
      case 60:
      case 61:
      case 62:
      case 63:
      case 64:
        ForPrimitives64.decode32(in, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 65:
      case 66:
      case 67:
      case 68:
      case 69:
      case 70:
      case 71:
      case 72:
        ForPrimitives64.decode8(in, longs);
        expand8Base(in, longs, visitor);
        break;
      case 73:
      case 74:
      case 75:
      case 76:
      case 77:
      case 78:
      case 79:
      case 80:
        ForPrimitives64.decode16(in, longs);
        expand16Base(in, longs, visitor);
        break;
      case 81:
      case 82:
      case 83:
      case 84:
      case 85:
      case 86:
      case 87:
      case 88:
        ForPrimitives64.decode24(in, longs);
        expand24Base(in, longs, visitor);
        break;
      case 89:
      case 90:
      case 91:
      case 92:
      case 93:
      case 94:
      case 95:
      case 96:
        ForPrimitives64.decode32(in, longs);
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
      ints[j]   = base += (int) ((arr[i] >>> 48) & 0xFFFF);
      ints[j+1] = base += (int) ((arr[i] >>> 32) & 0xFFFF);
      ints[j+2] = base += (int) ((arr[i] >>> 16) & 0xFFFF);
      ints[j+3] = base += (int) (arr[i] & 0xFFFF);
    }
  }

  private static void expand16Base(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    final int base = in.readVInt();
    for (int i = 0, j = offset; i < 16; ++i, j += 4) {
      ints[j]   = base + (int) ((arr[i] >>> 48) & 0xFFFF);
      ints[j+1] = base + (int) ((arr[i] >>> 32) & 0xFFFF);
      ints[j+2] = base + (int) ((arr[i] >>> 16) & 0xFFFF);
      ints[j+3] = base + (int) (arr[i] & 0xFFFF);
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

  private static void expand24(long[] arr, int[] ints, int offset) {
    for (int i = 0, j = offset; i < 24; i+= 3, j += 8) {
      long l1 = arr[i];
      long l2 = arr[i+1];
      long l3 = arr[i+2];
      ints[j] =  (int) (l1 >>> 40);
      ints[j+1] = (int) (l1 >>> 16) & 0xffffff;
      ints[j+2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
      ints[j+3] = (int) (l2 >>> 32) & 0xffffff;
      ints[j+4] = (int) (l2 >>> 8) & 0xffffff;
      ints[j+5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
      ints[j+6] = (int) (l3 >>> 24) & 0xffffff;
      ints[j+7] = (int) l3 & 0xffffff;
    }
  }

  private static void expand24Delta(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    int base = in.readVInt();
    for (int i = 0, j = offset; i < 24; i += 3, j += 8) {
      long l1 = arr[i];
      long l2 = arr[i+1];
      long l3 = arr[i+2];
      ints[j] =   base += (int) (l1 >>> 40);
      ints[j+1] = base += (int) (l1 >>> 16) & 0xffffff;
      ints[j+2] = base += (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
      ints[j+3] = base += (int) (l2 >>> 32) & 0xffffff;
      ints[j+4] = base += (int) (l2 >>> 8) & 0xffffff;
      ints[j+5] = base += (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
      ints[j+6] = base += (int) (l3 >>> 24) & 0xffffff;
      ints[j+7] = base += (int) l3 & 0xffffff;
    }
  }

  private static void expand24Base(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    final int base = in.readVInt();
    for (int i = 0, j = offset; i < 24; i += 3, j += 8) {
      long l1 = arr[i];
      long l2 = arr[i+1];
      long l3 = arr[i+2];
      ints[j] =   base + ((int) (l1 >>> 40));
      ints[j+1] = base + ((int) (l1 >>> 16) & 0xffffff);
      ints[j+2] = base + ((int) (((l1 & 0xffff) << 8) | (l2 >>> 56)));
      ints[j+3] = base + ((int) (l2 >>> 32) & 0xffffff);
      ints[j+4] = base + ((int) (l2 >>> 8) & 0xffffff);
      ints[j+5] = base + ((int) (((l2 & 0xff) << 16) | (l3 >>> 48)));
      ints[j+6] = base + ((int) (l3 >>> 24) & 0xffffff);
      ints[j+7] = base + ((int) l3 & 0xffffff);
    }
  }

  private static void expand24(long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < 24; i+= 3) {
      long l1 = arr[i];
      long l2 = arr[i+1];
      long l3 = arr[i+2];
      visitor.visit((int) (l1 >>> 40));
      visitor.visit((int) (l1 >>> 16) & 0xffffff);
      visitor.visit((int) (((l1 & 0xffff) << 8) | (l2 >>> 56)));
      visitor.visit((int) (l2 >>> 32) & 0xffffff);
      visitor.visit((int) (l2 >>> 8) & 0xffffff);
      visitor.visit((int) (((l2 & 0xff) << 16) | (l3 >>> 48)));
      visitor.visit((int) (l3 >>> 24) & 0xffffff);
      visitor.visit((int) l3 & 0xffffff);
    }
  }

  private static void expand24Delta(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    int base = in.readVInt();
    for (int i = 0; i < 24; i+= 3) {
      long l1 = arr[i];
      long l2 = arr[i+1];
      long l3 = arr[i+2];
      visitor.visit(base += (int) (l1 >>> 40));
      visitor.visit(base += (int) (l1 >>> 16) & 0xffffff);
      visitor.visit(base += (int) (((l1 & 0xffff) << 8) | (l2 >>> 56)));
      visitor.visit(base += (int) (l2 >>> 32) & 0xffffff);
      visitor.visit(base += (int) (l2 >>> 8) & 0xffffff);
      visitor.visit(base += (int) (((l2 & 0xff) << 16) | (l3 >>> 48)));
      visitor.visit(base += (int) (l3 >>> 24) & 0xffffff);
      visitor.visit(base += (int) l3 & 0xffffff);
    }
  }

  private static void expand24Base(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    final int base = in.readVInt();
    for (int i = 0; i < 24; i+= 3) {
      long l1 = arr[i];
      long l2 = arr[i+1];
      long l3 = arr[i+2];
      visitor.visit(base + ((int) (l1 >>> 40)));
      visitor.visit(base + ((int) (l1 >>> 16) & 0xffffff));
      visitor.visit(base + ((int) (((l1 & 0xffff) << 8) | (l2 >>> 56))));
      visitor.visit(base + ((int) (l2 >>> 32) & 0xffffff));
      visitor.visit(base + ((int) (l2 >>> 8) & 0xffffff));
      visitor.visit(base + ((int) (((l2 & 0xff) << 16) | (l3 >>> 48))));
      visitor.visit(base + ((int) (l3 >>> 24) & 0xffffff));
      visitor.visit(base + ((int) l3 & 0xffffff));
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
    for (int i = 0, j = offset; i < 32; i++, j+=2) {
      ints[j]   = base += (int) (arr[i] >>> 32);
      ints[j+1] = base += (int) arr[i];
    }
  }

  private static void expand32Base(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    final int base = in.readVInt();
    for (int i = 0, j = offset; i < 32; i++, j+=2) {
      ints[j]   = base + (int) (arr[i] >>> 32);
      ints[j+1] = base + (int) arr[i];
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
