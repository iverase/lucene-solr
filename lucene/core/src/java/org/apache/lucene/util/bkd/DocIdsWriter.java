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

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

class DocIdsWriter {

  private static final byte SORTED = (byte) 0;
  private static final byte INT8 = (byte) 8;
  private static final byte INT16 = (byte) 16;
  private static final byte INT24 = (byte) 24;
  private static final byte INT32 = (byte) 32;

  private DocIdsWriter() {}

  static void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
    if (count == 0) {
      writeDeltaVInts(docIds, start, count, out);
      return;
    }
    // docs can be sorted either when all docs in a block have the same value
    // or when a segment is sorted
    boolean sorted = true;
    long max = Integer.toUnsignedLong(docIds[start]);
    for (int i = 1; i < count; ++i) {
      if (sorted && docIds[start + i - 1] > docIds[start + i]) {
        sorted = false;
      }
      max |= Integer.toUnsignedLong(docIds[start + i]);
    }
    if (max <= 0xff) {
      writeInts8(docIds, start, count, out);
    } else if (sorted) {
      writeDeltaVInts(docIds, start, count, out);
    } else if (max <= 0xffff) {
      writeInts16(docIds, start, count, out);
    } else if (max <= 0xffffff) {
      writeInts24(docIds, start, count, out);
    } else {
      writeInts32(docIds, start, count, out);
    }
  }

  private static void writeDeltaVInts(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(SORTED);
    int previous = 0;
    for (int i = 0; i < count; ++i) {
      int doc = docIds[start + i];
      out.writeVInt(doc - previous);
      previous = doc;
    }
  }

  private static void writeInts32(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(INT32);
    for (int i = 0; i < count; ++i) {
      out.writeInt(docIds[start + i]);
    }
  }

  private static void writeInts24(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(INT24);
    for (int i = 0; i < count; ++i) {
      out.writeShort((short) (docIds[start + i] >>> 8));
      out.writeByte((byte) docIds[start + i]);
    }
  }

  private static void writeInts16(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(INT16);
    for (int i = 0; i < count; ++i) {
      out.writeShort((short) (docIds[start + i]));
    }
  }

  private static void writeInts8(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(INT8);
    for (int i = 0; i < count; ++i) {
      out.writeByte((byte) (docIds[start + i]));
    }
  }

  /** Read {@code count} integers into {@code docIDs}. */
  static void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
    final byte bpv = in.readByte();
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
      case INT16:
        readInts16(in, count, docIDs);
        break;
      case INT8:
        readInts8(in, count, docIDs);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private static void readDeltaVInts(IndexInput in, int count, int[] docIDs) throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      docIDs[i] = doc;
    }
  }

  private static void readInts32(IndexInput in, int count, int[] docIDs) throws IOException {
    int i;
    int offset = count - 1;
    for (i = 0; i < offset; ) {
      long l = in.readLong();
      docIDs[i++] =  (int) (l >>> 32);
      docIDs[i++] = (int) l;
    }
    for (; i < count; ++i) {
      docIDs[i] = in.readInt();
    }
  }

  private static void readInts24(IndexInput in, int count, int[] docIDs) throws IOException {
    int i;
    int offset = count - 7;
    for (i = 0; i < offset; ) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      docIDs[i++] =  (int) (l1 >>> 40);
      docIDs[i++] = (int) (l1 >>> 16) & 0xffffff;
      docIDs[i++] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
      docIDs[i++] = (int) (l2 >>> 32) & 0xffffff;
      docIDs[i++] = (int) (l2 >>> 8) & 0xffffff;
      docIDs[i++] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
      docIDs[i++] = (int) (l3 >>> 24) & 0xffffff;
      docIDs[i++] = (int) l3 & 0xffffff;
    }
    for (; i < count; ++i) {
      docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
    }
  }

  private static void readInts16(IndexInput in, int count, int[] docIDs) throws IOException {
    int i;
    int offset = count - 3;
    for (i = 0; i < offset; ) {
      long l = in.readLong();
      docIDs[i++] = (int) (l >>> 48);
      docIDs[i++] = (int) (l >>> 32) & 0xffff;
      docIDs[i++] = (int) (l >>> 16) & 0xffff;
      docIDs[i++] = (int) l & 0xffff;
    }
    offset += 2;
    for (; i < offset; ) {
      long l = in.readInt();
      docIDs[i++] = (int) (l >>> 16) & 0xffff;
      docIDs[i++] = (int) l & 0xffff;
    }
    for (; i < count; ++i) {
      docIDs[i] = Short.toUnsignedInt(in.readShort());
    }
  }

  private static void readInts8(IndexInput in, int count, int[] docIDs) throws IOException {
    int i;
    int offset = count - 7;
    for (i = 0; i < offset; ) {
      long l = in.readLong();
      docIDs[i++] =  (int) (l >>> 56);
      docIDs[i++] = (int) (l >>> 48) & 0xff;
      docIDs[i++] = (int) (l >>> 40) & 0xff;
      docIDs[i++] = (int) (l >>> 32) & 0xff;
      docIDs[i++] = (int) (l >>> 24) & 0xff;
      docIDs[i++] = (int) (l >>> 16) & 0xff;
      docIDs[i++] = (int) (l >>> 8) & 0xff;
      docIDs[i++] = (int) (l & 0xff);
    }
    offset += 4;
    for (; i < offset; ) {
      long l = in.readInt();
      docIDs[i++] = (int) (l >>> 24) & 0xff;
      docIDs[i++] = (int) (l >>> 16) & 0xff;
      docIDs[i++] = (int) (l >>> 8) & 0xff;
      docIDs[i++] = (int) (l & 0xff);
    }
    for (; i < count; ++i) {
      docIDs[i] = Byte.toUnsignedInt(in.readByte());
    }
  }

  /** Read {@code count} integers and feed the result directly to {@link IntersectVisitor#visit(int)}. */
  static void readInts(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
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
      case INT16:
        readInts16(in, count, visitor);
        break;
      case INT8:
        readInts8(in, count, visitor);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
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
    int i;
    for (i = 0; i < count - 1; i += 2) {
      long l = in.readLong();
      visitor.visit((int) (l >>> 32));
      visitor.visit((int) l);
    }
    for (; i < count; ++i) {
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

  private static void readInts16(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int i;
    for (i = 0; i < count - 3; i += 4) {
      long l = in.readLong();
      visitor.visit((int) (l >>> 48));
      visitor.visit((int) (l >>> 32) & 0xffff);
      visitor.visit((int) (l >>> 16) & 0xffff);
      visitor.visit((int) l & 0xffff);
    }
    for (; i < count - 1; i += 2) {
      long l = in.readInt();
      visitor.visit((int) (l >>> 16) & 0xffff);
      visitor.visit((int) l & 0xffff);
    }
    for (; i < count; ++i) {
      visitor.visit(Short.toUnsignedInt(in.readShort()));
    }
  }

  private static void readInts8(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      long l = in.readLong();
      visitor.visit((int) (l >>> 56));
      visitor.visit((int) (l >>> 48) & 0xff);
      visitor.visit((int) (l >>> 40) & 0xff);
      visitor.visit((int) (l >>> 32) & 0xff);
      visitor.visit((int) (l >>> 24) & 0xff);
      visitor.visit((int) (l >>> 16) & 0xff);
      visitor.visit((int) (l >>> 8) & 0xff);
      visitor.visit((int) (l & 0xff));
    }
    for (; i < count - 3; i += 4) {
      long l = in.readInt();
      visitor.visit((int) (l >>> 24) & 0xff);
      visitor.visit((int) (l >>> 16) & 0xff);
      visitor.visit((int) (l >>> 8) & 0xff);
      visitor.visit((int) (l & 0xff));
    }
    for (; i < count; ++i) {
      visitor.visit(Byte.toUnsignedInt(in.readByte()));
    }
  }
}
