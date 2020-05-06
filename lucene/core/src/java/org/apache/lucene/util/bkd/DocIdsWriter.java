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

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

class DocIdsWriter {

  private static final byte SORTED = (byte) 0;
  private static final byte EQUAL = (byte) 1;
  private static final byte INT24 = (byte) 24;
  private static final byte INT32 = (byte) 32;
  private static final byte SIMD = (byte) 64;

  private DocIdsWriter() {}

  static void writeDocIds(int[] docIds, int start, int count, DataOutput out, long[] tmp) throws IOException {
    assert tmp.length >= ForUtilCheck.BLOCK_SIZE / 2;
    // docs can be sorted either when all docs in a block have the same value
    // or when a segment is sorted
    boolean sorted = true;
    for (int i = 1; i < count; ++i) {
      if (docIds[start + i - 1] > docIds[start + i]) {
        sorted = false;
        break;
      }
    }
    if (sorted) {
      if (count > 0 && docIds[start] == docIds[start + count - 1]) {
        out.writeByte(EQUAL);
        out.writeVInt(docIds[start]);
      } else {
        out.writeByte(SORTED);
        int previous = 0;
        for (int i = 0; i < count; ++i) {
          int doc = docIds[start + i];
          out.writeVInt(doc - previous);
          previous = doc;
        }
      }
    } else {
      if (count % ForUtilCheck.BLOCK_SIZE == 0) {
        out.writeByte(SIMD);
        writeSIMD(docIds, start, count, out, tmp);
        return;
      }
      long max = 0;
      for (int i = 0; i < count; ++i) {
        max |= Integer.toUnsignedLong(docIds[start + i]);
      }
      if (max <= 0xffffff) {
        out.writeByte(INT24);
        for (int i = 0; i < count; ++i) {
          out.writeShort((short) (docIds[start + i] >>> 8));
          out.writeByte((byte) docIds[start + i]);
        }
      } else {
        out.writeByte(INT32);
        for (int i = 0; i < count; ++i) {
          out.writeInt(docIds[start + i]);
        }
      }
    }
  }

  private static void writeSIMD(int[] docIds, int start, int count, DataOutput out, long[] tmp) throws IOException {
    long[] source = new long[ForUtilCheck.BLOCK_SIZE];
    final int iterations = count / ForUtilCheck.BLOCK_SIZE;
    for (int i = 0; i < iterations; ++i) {
      long max = 0;
      for (int j = 0; j < ForUtilCheck.BLOCK_SIZE; ++j) {
        max |= Integer.toUnsignedLong(docIds[start + i * ForUtilCheck.BLOCK_SIZE + j]);
        source[j] = docIds[start + i * ForUtilCheck.BLOCK_SIZE +j];
      }
      int bpv = PackedInts.bitsRequired(max);
      out.writeByte((byte) bpv);
      ForUtilCheck.encode(source, bpv, out, tmp);
    }
  }

  /** Read {@code count} integers into {@code docIDs}. */
  static void readInts(IndexInput in, int count, int[] docIDs, long[] tmp1, long[] tmp2) throws IOException {
    assert tmp1.length >= ForUtilCheck.BLOCK_SIZE / 2;
    assert tmp2.length >= ForUtilCheck.BLOCK_SIZE / 2;
    final byte bpv = in.readByte();
   // long start = System.nanoTime();
    switch (bpv) {
      case SORTED:
        readDeltaVInts(in, count, docIDs);
        break;
      case INT32:
        readInts32(in, count, docIDs);
        break;
      case INT24:
        readInts24(in, count, docIDs);
        break;
      case SIMD:
        readSIMD(in, count, docIDs, tmp1, tmp2);
        break;
      case EQUAL:
        readEqual(in, count, docIDs);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
    //long end = System.nanoTime();
    //System.out.println("type: " + bpv + " time: " + (end - start));
  }

  private static void readSIMD(IndexInput in, int count, int[] docIDs, long[] tmp1, long[] tmp2) throws IOException {
    final int iterations = count / ForUtilCheck.BLOCK_SIZE;
    for (int i = 0; i < iterations; ++i) {
      final int bitsPerValue = in.readByte();
      ForUtilCheck.decode(bitsPerValue, in, tmp1, tmp2, docIDs, i * ForUtilCheck.BLOCK_SIZE);
    }
  }

  private static void readEqual(IndexInput in, int count, int[] docIDs) throws IOException {
    int doc = in.readVInt();
    Arrays.fill(docIDs, 0, count, doc);
  }

  private static void readDeltaVInts(IndexInput in, int count, int[] docIDs) throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      docIDs[i] = doc;
    }
  }

  private static void readInts32(IndexInput in, int count, int[] docIDs) throws IOException {
    for (int i = 0; i < count; i++) {
      docIDs[i] = in.readInt();
    }
  }

  private static void readInts24(IndexInput in, int count, int[] docIDs) throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      docIDs[i] =  (int) (l1 >>> 40);
      docIDs[i+1] = (int) (l1 >>> 16) & 0xffffff;
      docIDs[i+2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
      docIDs[i+3] = (int) (l2 >>> 32) & 0xffffff;
      docIDs[i+4] = (int) (l2 >>> 8) & 0xffffff;
      docIDs[i+5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
      docIDs[i+6] = (int) (l3 >>> 24) & 0xffffff;
      docIDs[i+7] = (int) l3 & 0xffffff;
    }
    for (; i < count; ++i) {
      docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
    }
  }

  /** Read {@code count} integers and feed the result directly to {@link IntersectVisitor#visit(int)}. */
  static void readInts(IndexInput in, int count, IntersectVisitor visitor, long[] tmp1, long[] tmp2) throws IOException {
    final byte bpv = in.readByte();
    switch (bpv) {
      case SORTED:
        readDeltaVInts(in, count, visitor);
        break;
      case INT32:
        readInts32(in, count, visitor);
        break;
      case INT24:
        readInts24(in, count, visitor);
        break;
      case SIMD:
        readSIMD(in, count, visitor, tmp1, tmp2);
        break;
      case EQUAL:
        readEqual(in, count, visitor);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private static void readSIMD(IndexInput in, int count, IntersectVisitor visitor, long[] tmp1, long[] tmp2) throws IOException {
    final int iterations = count / ForUtilCheck.BLOCK_SIZE;
    for (int i = 0; i < iterations; ++i) {
      final int bitsPerValue = in.readByte();
      ForUtilCheck.decode(bitsPerValue, in, tmp1, tmp2, visitor);
    }
  }

  private static void readEqual(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int doc = in.readVInt();
    for (int i = 0; i < count; i++) {
      visitor.visit(doc);
    }
  }

  private static void readDeltaVInts(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      visitor.visit(doc);
    }
  }

  private static void readInts32(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count; i++) {
      visitor.visit(in.readInt());
    }
  }

  private static void readInts24(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      visitor.visit((int) (l1 >>> 40));
      visitor.visit((int) (l1 >>> 16) & 0xffffff);
      visitor.visit((int) (((l1 & 0xffff) << 8) | (l2 >>> 56)));
      visitor.visit((int) (l2 >>> 32) & 0xffffff);
      visitor.visit((int) (l2 >>> 8) & 0xffffff);
      visitor.visit((int) (((l2 & 0xff) << 16) | (l3 >>> 48)));
      visitor.visit((int) (l3 >>> 24) & 0xffffff);
      visitor.visit((int) l3 & 0xffffff);
    }
    for (; i < count; ++i) {
      visitor.visit((Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte()));
    }
  }
}
