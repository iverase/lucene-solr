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

class DocIdsWriter2 {

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

  private DocIdsWriter2() {}

  static void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
    if (count == 0) {
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
    if (sorted) {
      if (runLenDocs < count / 2) {
        writeRunLenDeltaVInts(docIds, start, count, out);
      } else {
        writeDeltaVInts(docIds, start, count, out);
      }
    } else {
      if (max <= 0xff) {
        if (runLenDocs < count / 3) {
          writeRunLen8(docIds, start, count, out, runLenDocs);
        } else {
          writeInts8(docIds, start, count, out);
        }
      } else if (max <= 0xffff) {
        if (runLenDocs < count / 3) {
          writeRunLen16(docIds, start, count, out, runLenDocs);
        } else {
          writeInts16(docIds, start, count, out);
        }
      } else
      if (max <= 0xffffff) {
        if (runLenDocs < count / 3) {
          writeRunLen24(docIds, start, count, out, runLenDocs);
        } else {
          writeInts24(docIds, start, count, out);
        }
      } else {
        if (runLenDocs < count / 3) {
          writeRunLen32(docIds, start, count, out, runLenDocs);
        } else {
          writeInts32(docIds, start, count, out);
        }
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
      if (docIds[start + i] != doc) {
        out.writeVInt(i - prevIndex);
        out.writeInt(doc - previous);
        previous = doc;
        doc = docIds[start + i];
        prevIndex = i;
      }
    }
    out.writeVInt(count - prevIndex);
    out.writeInt(doc - previous);
  }

  private static void writeInts32(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(INT32);
    for (int i = 0; i < count; ++i) {
      out.writeInt(docIds[start + i]);
    }
  }

  private static void writeRunLen32(int[] docIds, int start, int count, DataOutput out, int runLenDocs) throws IOException {
    out.writeByte(RUNLEN32);
    out.writeVInt(runLenDocs);
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
    out.writeVInt(runLenDocs);
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
    out.writeVInt(runLenDocs);
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
    out.writeVInt(runLenDocs);
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
  static void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case SORTED:
        readDeltaVInts(in, count, docIDs);
        break;
      case RUNLENSORTED:
        readRunLenDeltaVInts(in, count, docIDs);
        break;
      case INT32:
        readInts32(in, count, docIDs);
        break;
      case RUNLEN32:
        readRunLen32(in, count, docIDs);
        break;
      case INT24:
        readInts24(in, count, docIDs);
        break;
      case RUNLEN24:
        readRunLen24(in, count, docIDs);
        break;
      case INT16:
        readInts16(in, count, docIDs);
        break;
      case RUNLEN16:
        readRunLen16(in, count, docIDs);
        break;
      case INT8:
        readInts8(in, count, docIDs);
        break;
      case RUNLEN8:
        readRunLen8(in, count, docIDs);
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

  private static void readRunLenDeltaVInts(IndexInput in, int count, int[] docIDs) throws IOException {
    int doc = 0;
    for (int i = 0; i < count; ) {
      int runLen = in.readVInt();
      doc += in.readInt();
      for (int j = 0; j < runLen; j++) {
        docIDs[i++] = doc;
      }
    }
  }

  private static void readInts32(IndexInput in, int count, int[] docIDs) throws IOException {
    int i;
    for (i = 0; i < count - 1; i += 2) {
      long l = in.readLong();
      docIDs[i] =  (int) (l >>> 32);
      docIDs[i+1] = (int) l;
    }
    for (; i < count; ++i) {
      docIDs[i] = in.readInt();
    }
  }

  private static void readRunLen32(IndexInput in, int count, int[] docIDs) throws IOException {
    final int runLenDocs = in.readVInt();
    int i;
    int index = 0;
    int runLen = 0;
    for (i = 0; i < runLenDocs - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      long l4 = in.readLong();
      long l5 = in.readLong();
      runLen  += (int) (l1 >>> 56);
      int doc = (int) (l1 >>> 24);
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l1 >>> 16) & 0xff;
      doc =  (int) (((l1 & 0xffff) << 16) | (l2 >>> 48));
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l2 >>> 40) & 0xff;
      doc =  (int) (l2 >>> 8);
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) l2 & 0xff;
      doc =  (int) (l3 >>> 32);
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l3 >>> 24) & 0xff;
      doc =  (int) (((l3 & 0xffffff) << 8) | (l4 >>> 56));
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l4 >>> 48) & 0xff;
      doc =  (int) (l4 >>> 16);
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l4 >>> 8) & 0xff;
      doc =  (int) (((l4 & 0xff) << 24) | (l5 >>> 40));
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l5 >>> 32) & 0xff;
      doc = (int) l5;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
    }
    for (; i < runLenDocs; ++i) {
      runLen += Byte.toUnsignedInt(in.readByte());
      int doc = in.readInt();
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
    }
    assert index == count;
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

  private static void readRunLen24(IndexInput in, int count, int[] docIDs) throws IOException {
    final int runLenDocs = in.readVInt();
    int i;
    int index = 0;
    int runLen = 0;
    for (i = 0; i < runLenDocs - 1; i += 2) {
      long l = in.readLong();
      runLen += (int) (l >>> 56);
      int doc = (int) (l >>> 32) & 0xffffff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l >>> 24) & 0xff;
      doc = (int) l & 0xffffff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
    }
    for (; i < runLenDocs; ++i) {
      int l = in.readInt();
      runLen +=  (l >>> 24);
      int doc = l & 0xffffff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
    }
    assert index == count;
  }

  private static void readInts16(IndexInput in, int count, int[] docIDs) throws IOException {
    int i;
    for (i = 0; i < count - 3; i += 4) {
      long l = in.readLong();
      docIDs[i] = (int) (l >>> 48);
      docIDs[i+1] = (int) (l >>> 32) & 0xffff;
      docIDs[i+2] = (int) (l >>> 16) & 0xffff;
      docIDs[i+3] = (int) l & 0xffff;
    }
    for (; i < count - 1; i += 2) {
      long l = in.readInt();
      docIDs[i] = (int) (l >>> 16) & 0xffff;
      docIDs[i+1] = (int) l & 0xffff;
    }
    for (; i < count; ++i) {
      docIDs[i] = Short.toUnsignedInt(in.readShort());
    }
  }

  private static void readRunLen16(IndexInput in, int count, int[] docIDs) throws IOException {
    final int runLenDocs = in.readVInt();
    int i;
    int index = 0;
    int runLen = 0;
    for (i = 0; i < runLenDocs - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      runLen += (int) (l1 >>> 56);
      int doc =  (int) (l1 >>> 40) & 0xffff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l1 >>> 32) & 0xff;
      doc =  (int) (l1 >>> 16) & 0xffff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l1 >>> 8) & 0xff;
      doc =  (int) (((l1 & 0xff) << 8) | (l2 >>> 56));
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l2 >>> 48) & 0xff;
      doc =  (int) (l2 >>> 32) & 0xffff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l2 >>> 24) & 0xff;
      doc =  (int) (l2 >>> 8) & 0xffff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) l2 & 0xff;
      doc =  (int) (l3 >>> 48) & 0xffff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l3 >>> 40) & 0xff;
      doc =  (int) (l3 >>> 24) & 0xffff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l3 >>> 16) & 0xff;
      doc =  (int) l3 & 0xffff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
    }
    for (; i < runLenDocs; ++i) {
      runLen += Byte.toUnsignedInt(in.readByte());
      int doc = Short.toUnsignedInt(in.readShort());
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
    }
    assert index == count;
  }

  private static void readInts8(IndexInput in, int count, int[] docIDs) throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      long l = in.readLong();
      docIDs[i] =  (int) (l >>> 56);
      docIDs[i+1] = (int) (l >>> 48) & 0xff;
      docIDs[i+2] = (int) (l >>> 40) & 0xff;
      docIDs[i+3] = (int) (l >>> 32) & 0xff;
      docIDs[i+4] = (int) (l >>> 24) & 0xff;
      docIDs[i+5] = (int) (l >>> 16) & 0xff;
      docIDs[i+6] = (int) (l >>> 8) & 0xff;
      docIDs[i+7] = (int) (l & 0xff);
    }
    for (; i < count - 3; i += 4) {
      long l = in.readInt();
      docIDs[i] = (int) (l >>> 24) & 0xff;
      docIDs[i+1] = (int) (l >>> 16) & 0xff;
      docIDs[i+2] = (int) (l >>> 8) & 0xff;
      docIDs[i+3] = (int) (l & 0xff);
    }
    for (; i < count; ++i) {
      docIDs[i] = Byte.toUnsignedInt(in.readByte());
    }
  }

  private static void readRunLen8(IndexInput in, int count, int[] docIDs) throws IOException {
    final int runLenDocs = in.readVInt();
    int index = 0;
    int runLen = 0;
    int i;
    for (i = 0; i < runLenDocs - 3; i += 4) {
      long l = in.readLong();
      runLen += (int) (l >>> 56);
      int doc = (int) (l >>> 48) & 0xff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l >>> 40) & 0xff;
      doc = (int) (l >>> 32) & 0xff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l >>> 24) & 0xff;
      doc = (int) (l >>> 16) & 0xff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l >>> 8) & 0xff;
      doc = (int) l & 0xff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
    }
    for (; i < runLenDocs - 1; i += 2) {
      long l = in.readInt();
      runLen += (int) (l >>> 24) & 0xff;
      int doc = (int) (l >>> 16) & 0xff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
      runLen += (int) (l >>> 8) & 0xff;
      doc = (int) l & 0xff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
    }
    for (; i < runLenDocs; ++i) {
      runLen += in.readByte() & 0xff;
      int doc = in.readByte() & 0xff;
      for (; index < runLen; index++) {
        docIDs[index] = doc;
      }
    }
    assert index == count;
  }


  /** Read {@code count} integers and feed the result directly to {@link IntersectVisitor#visit(int)}. */
  static void readInts(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
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
    for (int i =0; i < count; ) {
      int runLen = in.readVInt();
      doc += in.readInt();
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      i += runLen;
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

  private static void readRunLen32(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    final int runLenDocs = in.readVInt();
    int i;
    for (i = 0; i < runLenDocs - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      long l4 = in.readLong();
      long l5 = in.readLong();
      int runLen  = (int) (l1 >>> 56);
      int doc =  (int) (l1 >>> 24);
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) (l1 >>> 16) & 0xff;
      doc =  (int) (((l1 & 0xffff) << 16) | (l2 >>> 48));
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) (l2 >>> 40) & 0xff;
      doc =  (int) (l2 >>> 8);
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) l2 & 0xff;
      doc =  (int) (l3 >>> 32);
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) (l3 >>> 24) & 0xff;
      doc =  (int) (((l3 & 0xffffff) << 8) | (l4 >>> 56));
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  =  (int) (l4 >>> 48) & 0xff;
      doc =  (int) (l4 >>> 16);
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) (l4 >>> 8) & 0xff;
      doc =  (int) (((l4 & 0xff) << 24) | (l5 >>> 40));
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) (l5 >>> 32) & 0xff;
      doc =  (int) l5;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
    }
    for (; i < runLenDocs; ++i) {
      int runLen = Byte.toUnsignedInt(in.readByte());
      int doc = in.readInt();
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
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
    int runLenDocs = in.readVInt();
    int i;
    for (i = 0; i < runLenDocs - 1; i += 2) {
      long l = in.readLong();
      int runLen = (int) (l >>> 56);
      int doc = (int) (l >>> 32)& 0xffffff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen =  (int) (l >>> 24) & 0xff;
      doc = (int) l & 0xffffff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
    }
    for (; i < runLenDocs; ++i) {
      int l = in.readInt();
      int runLen =  (l >>> 24);
      int doc = l & 0xffffff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
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

  private static void readRunLen16(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    final int runLenDocs = in.readVInt();
    int i;
    for (i = 0; i < runLenDocs - 7; i += 8) {
      long l1 = in.readLong();
      long l2 = in.readLong();
      long l3 = in.readLong();
      int runLen  = (int) (l1 >>> 56);
      int doc =  (int) (l1 >>> 40) & 0xffff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) (l1 >>> 32) & 0xff;
      doc =  (int) (l1 >>> 16) & 0xffff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) (l1 >>> 8) & 0xff;
      doc =  (int) (((l1 & 0xff) << 8) | (l2 >>> 56));
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) (l2 >>> 48) & 0xff;
      doc =  (int) (l2 >>> 32) & 0xffff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) (l2 >>> 24) & 0xff;
      doc =  (int) (l2 >>> 8) & 0xffff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) l2 & 0xff;
      doc =  (int) (l3 >>> 48) & 0xffff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) (l3 >>> 40) & 0xff;
      doc =  (int) (l3 >>> 24) & 0xffff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen  = (int) (l3 >>> 16) & 0xff;
      doc =  (int) l3 & 0xffff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
    }
    for (; i < runLenDocs; ++i) {
      int runLen = Byte.toUnsignedInt(in.readByte());
      int doc = Short.toUnsignedInt(in.readShort());
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
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

  private static void readRunLen8(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int runLenDocs = in.readVInt();
    int i;
    for (i = 0; i < runLenDocs - 3; i += 4) {
      long l = in.readLong();
      int runLen = (int) (l >>> 56);
      int doc = (int) (l >>> 48) & 0xff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen = (int) (l >>> 40) & 0xff;
      doc = (int) (l >>> 32) & 0xff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen = (int) (l >>> 24) & 0xff;
      doc = (int) (l >>> 16) & 0xff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen = (int) (l >>> 8) & 0xff;
      doc = (int) l & 0xff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
    }
    for (; i < runLenDocs - 1; i += 2) {
      long l = in.readInt();
      int runLen = (int) (l >>> 24) & 0xff;
      int doc = (int) (l >>> 16) & 0xff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
      runLen = (int) (l >>> 8) & 0xff;
      doc = (int) l & 0xff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
    }
    for (; i < runLenDocs; ++i) {
      int runLen = in.readByte() & 0xff;
      int doc = in.readByte() & 0xff;
      for (int j = 0; j < runLen; j++) {
        visitor.visit(doc);
      }
    }
  }
}
