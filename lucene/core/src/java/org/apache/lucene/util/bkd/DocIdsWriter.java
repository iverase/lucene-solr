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

class DocIdsWriter {

  private static final byte SORTED = (byte) 0;
  private static final byte RUNLENSORTED = (byte) 1;
  private static final byte INT8 = (byte) 8;
  private static final byte RUNLEN8 = (byte) 9;
  private static final byte INT16 = (byte) 16;
  private static final byte RUNLEN16 = (byte) 17;
  private static final byte INT24 = (byte) 24;
  private static final byte RUNLEN24 = (byte) 25;
  private static final byte INT32 = (byte) 32;
  private static final byte RUNLEN32 = (byte) 33;

  private DocIdsWriter() {}

  static void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
    if (count == 0) {
      out.writeVInt(0);
      out.writeByte(SORTED);
      return;
    }
    // docs can be sorted either when all docs in a block have the same value
    // or when a segment is sorted
    boolean sorted = true;
    int docId = docIds[start];
    long max = Integer.toUnsignedLong(docId);
    int runLenDocs = 1;
    int prevIndex = 0;
    for (int i = 1; i < count; ++i) {
      if (sorted && docId > docIds[start + i]) {
        sorted = false;
      }
      if (docId != docIds[start + i] || (i - prevIndex == 0xff)) {
        docId = docIds[start + i];
        max |= Integer.toUnsignedLong(docId);
        runLenDocs++;
        prevIndex = i;
      }
    }
    if (max <= 0xff) {
      if (runLenDocs < count / 2) {
        out.writeVInt(runLenDocs);
        writeRunLen8(docIds, start, count, out, runLenDocs);
      } else {
        out.writeVInt(count);
        writeInts8(docIds, start, count, out);
      }
    } else if (sorted) {
      if (runLenDocs < count / 2) {
        out.writeVInt(runLenDocs);
        writeRunLenDeltaVInts(docIds, start, count, out);
      } else {
        out.writeVInt(count);
        writeDeltaVInts(docIds, start, count, out);
      }
    } else if (max <= 0xffff) {
      if (runLenDocs < count / 2) {
        out.writeVInt(runLenDocs);
        writeRunLen16(docIds, start, count, out, runLenDocs);
      } else {
        out.writeVInt(count);
        writeInts16(docIds, start, count, out);
      }
    } else if (max <= 0xffffff) {
      if (runLenDocs < count / 2) {
        out.writeVInt(count);
        writeRunLen24(docIds, start, count, out, runLenDocs);
      } else {
        out.writeVInt(count);
        writeInts24(docIds, start, count, out);
      }
    } else {
      if (runLenDocs < count / 2) {
        out.writeVInt(count);
        writeRunLen32(docIds, start, count, out, runLenDocs);
      } else {
        out.writeVInt(count);
        writeInts32(docIds, start, count, out);
      }
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

  private static void writeRunLenDeltaVInts(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(RUNLENSORTED);
    int prevIndex = 0;
    int previous = 0;
    int doc = docIds[start];
    for (int i = 1; i < count; ++i) {
      if (docIds[start + i] != doc || (i - prevIndex == 0xff)) {
        out.writeByte((byte) (i - prevIndex));
        out.writeVInt(doc - previous);
        previous = doc;
        doc = docIds[start + i];
        prevIndex = i;
      }
    }
    out.writeByte((byte) (count - prevIndex));
    out.writeVInt(doc - previous);
  }

  private static void writeInts32(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(INT32);
    for (int i = 0; i < count; ++i) {
      out.writeInt(docIds[start + i]);
    }
  }

  private static void writeRunLen32(int[] docIds, int start, int count, DataOutput out, int runLenDocs) throws IOException {
    out.writeByte(RUNLEN32);
    int prevIndex = 0;
    int doc = docIds[start];
    for (int i = 1; i < count; ++i) {
      if (docIds[start + i] != doc || (i - prevIndex == 0xff)) {
        out.writeByte((byte) (i - prevIndex));
        out.writeInt(doc);
        doc = docIds[start + i];
        prevIndex = i;
      }
    }
    out.writeByte((byte) (count - prevIndex));
    out.writeInt(doc);
  }

  private static void writeInts24(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(INT24);
    for (int i = 0; i < count; ++i) {
      out.writeShort((short) (docIds[start + i] >>> 8));
      out.writeByte((byte) docIds[start + i]);
    }
  }

  private static void writeRunLen24(int[] docIds, int start, int count, DataOutput out, int runLenDocs) throws IOException {
    out.writeByte(RUNLEN24);
    int prevIndex = 0;
    int doc = docIds[start];
    for (int i = 1; i < count; ++i) {
      if (docIds[start + i] != doc || (i - prevIndex == 0xff)) {
        out.writeByte((byte) (i - prevIndex));
        out.writeShort((short) (doc >>> 8));
        out.writeByte((byte) doc);
        doc = docIds[start + i];
        prevIndex = i;
      }
    }
    out.writeByte((byte) (count - prevIndex));
    out.writeShort((short) (doc >>> 8));
    out.writeByte((byte) doc);
  }

  private static void writeInts16(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(INT16);
    for (int i = 0; i < count; ++i) {
      out.writeShort((short) (docIds[start + i]));
    }
  }

  private static void writeRunLen16(int[] docIds, int start, int count, DataOutput out, int runLenDocs) throws IOException {
    out.writeByte(RUNLEN16);
    int prevIndex = 0;
    int doc = docIds[start];
    for (int i = 1; i < count; ++i) {
      if (docIds[start + i] != doc || (i - prevIndex == 0xff)) {
        out.writeByte((byte) (i - prevIndex));
        out.writeShort((short) doc);
        doc = docIds[start + i];
        prevIndex = i;
      }
    }
    out.writeByte((byte) (count - prevIndex));
    out.writeShort((short) doc);
  }

  private static void writeInts8(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(INT8);
    for (int i = 0; i < count; ++i) {
      out.writeByte((byte) (docIds[start + i]));
    }
  }

  private static void writeRunLen8(int[] docIds, int start, int count, DataOutput out, int runLenDocs) throws IOException {
    out.writeByte(RUNLEN8);
    int prevIndex = 0;
    int doc = docIds[start];
    for (int i = 1; i < count; ++i) {
      if (docIds[start + i] != doc || (i - prevIndex == 0xff)) {
        out.writeByte((byte) (i - prevIndex));
        out.writeByte((byte) doc);
        doc = docIds[start + i];
        prevIndex = i;
      }
    }
    out.writeByte((byte) (count - prevIndex));
    out.writeByte((byte) doc);
  }

  /** Read {@code count} integers into {@code docIDs}. */
  static int readInts(IndexInput in, int[] docIDs) throws IOException {
    final int count = in.readVInt();
    final int bpv = in.readByte();
    switch (bpv) {
      case SORTED:
        return readDeltaVInts(in, count, docIDs);
      case RUNLENSORTED:
        return readRunLenDeltaVInts(in, count, docIDs);
      case INT32:
        return readInts32(in, count, docIDs);
      case RUNLEN32:
        return readRunLen32(in, count, docIDs);
      case INT24:
        return readInts24(in, count, docIDs);
      case RUNLEN24:
        return readRunLen24(in, count, docIDs);
      case INT16:
        return readInts16(in, count, docIDs);
      case RUNLEN16:
        return readRunLen16(in, count, docIDs);
      case INT8:
        return readInts8(in, count, docIDs);
      case RUNLEN8:
        return readRunLen8(in, count, docIDs);
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private static int readDeltaVInts(IndexInput in, int count, int[] docIDs) throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      docIDs[i] = doc;
    }
    return count;
  }

  private static int readRunLenDeltaVInts(IndexInput in, int count, int[] docIDs) throws IOException {
    int doc = 0;
    int index = 0;
    for (int i = 0; i < count; i++) {
      Arrays.fill(docIDs, index, index += Byte.toUnsignedInt(in.readByte()), doc += in.readVInt());
    }
    return index;
  }

  private static int readInts32(IndexInput in, int count, int[] docIDs) throws IOException {
    for (int i = 0; i < count; ++i) {
      docIDs[i] = in.readInt();
    }
    return count;
  }

  private static int readRunLen32(IndexInput in, int count, int[] docIDs) throws IOException {
    for (int index = 0; index < count; ) {
      Arrays.fill(docIDs, index, index += Byte.toUnsignedInt(in.readByte()), in.readInt());
    }
    return count;

  }

  private static int readInts24(IndexInput in, int count, int[] docIDs) throws IOException {
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
    return count;
  }

  private static int readRunLen24(IndexInput in, int count, int[] docIDs) throws IOException {
    for (int index = 0; index < count; ) {
      int l = in.readInt();
      Arrays.fill(docIDs, index,  index += (l >>> 24), l & 0xffffff);
    }
    return count;
  }

  private static int readInts16(IndexInput in, int count, int[] docIDs) throws IOException {
    for (int i = 0; i < count; ++i) {
      docIDs[i] = Short.toUnsignedInt(in.readShort());
    }
    return count;
  }

  private static int readRunLen16(IndexInput in, int count, int[] docIDs) throws IOException {
    int index = 0;
    for (int i = 0; i < count; i++) {
      Arrays.fill(docIDs, index, index += Byte.toUnsignedInt(in.readByte()), Short.toUnsignedInt(in.readShort()));
    }
    return index;
  }

  private static int readInts8(IndexInput in, int count, int[] docIDs) throws IOException {
    for (int i = 0; i < count; ++i) {
      docIDs[i] = Byte.toUnsignedInt(in.readByte());
    }
    return count;
  }

  private static int readRunLen8(IndexInput in, int count, int[] docIDs) throws IOException {
    int index = 0;
    for (int i = 0; i < count; ++i) {
      Arrays.fill(docIDs, index, index += Byte.toUnsignedInt(in.readByte()), Byte.toUnsignedInt(in.readByte()));
    }
    return index;
  }


  /** Read {@code count} integers and feed the result directly to {@link IntersectVisitor#visit(int)}. */
  static void readInts(IndexInput in, IntersectVisitor visitor) throws IOException {
    final int count = in.readVInt();
    final int bpv = in.readByte();
    switch (bpv) {
      case SORTED:
        readDeltaVInts(in, count, visitor);
        break;
      case RUNLENSORTED:
        readRunLenDeltaVInts(in, count, visitor);
        break;
      case INT32:
        readInts32(in, count, visitor);
        break;
      case RUNLEN32:
        readRunLen32(in, count, visitor);
        break;
      case INT24:
        readInts24(in, count, visitor);
        break;
      case RUNLEN24:
        readRunLen24(in, count, visitor);
        break;
      case INT16:
        readInts16(in, count, visitor);
        break;
      case RUNLEN16:
        readRunLen16(in, count, visitor);
        break;
      case INT8:
        readInts8(in, count, visitor);
        break;
      case RUNLEN8:
        readRunLen8(in, count, visitor);
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

  private static void readRunLenDeltaVInts(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      visit(Byte.toUnsignedInt(in.readByte()), doc += in.readVInt(), visitor);
    }
  }

  private static void readInts32(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count; ++i) {
      visitor.visit(in.readInt());
    }
  }

  private static void readRunLen32(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count;) {
      int runLen =  Byte.toUnsignedInt(in.readByte());
      visit(runLen, in.readInt(), visitor);
      i += runLen;
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

  private static void readRunLen24(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count; ) {
      int l = in.readInt();
      int runLen = (l >>> 24);
      visit( runLen, l & 0xffffff, visitor);
      i += runLen;
    }
  }

  private static void readInts16(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count; ++i) {
      visitor.visit(Short.toUnsignedInt(in.readShort()));
    }
  }

  private static void readRunLen16(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count; ++i) {
      visit(Byte.toUnsignedInt(in.readByte()), Short.toUnsignedInt(in.readShort()), visitor);
    }
  }

  private static void readInts8(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count; ++i) {
      visitor.visit(Byte.toUnsignedInt(in.readByte()));
    }
  }

  private static void readRunLen8(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count; ++i) {
      visit(Byte.toUnsignedInt(in.readByte()), Byte.toUnsignedInt(in.readByte()), visitor);
    }
  }

  private static void visit(int len, int doc, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < len; i++) {
      visitor.visit(doc);
    }
  }
}
