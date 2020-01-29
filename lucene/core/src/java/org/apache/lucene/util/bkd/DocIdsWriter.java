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

  private static final byte DELTA = (byte) 0;
  private static final byte EQUALS = (byte) 2;
  private static final byte RUNLEN = (byte) 4;
  private static final byte RUNLEN24 = (byte) 6;
  private static final byte DELTARUNLEN = (byte) 8;
  private static final byte INT24 = (byte) 24;
  private static final byte INT32 = (byte) 32;

  private DocIdsWriter() {}

  static void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
    // docs can be sorted either when all docs in a block have the same value
    // or when a segment is sorted
    boolean sorted = true;
    int runLenDocs = 1;
    int docId = docIds[start];
    for (int i = 1; i < count; ++i) {
      if (sorted && docIds[start + i - 1] > docIds[start + i]) {
        sorted = false;
      }
      if (docIds[start + i] != docId) {
        docId = docIds[start + i];
        runLenDocs++;
      }
    }
    if (runLenDocs == 1) {
      out.writeByte(EQUALS);
      out.writeInt(docId);
    } else if (sorted) {
      if (runLenDocs < count / 2) {
        writeDeltaRunLen(docIds, start, count, out);
      } else {
        writeDelta(docIds, start, count, out);
      }
    } else {
      long max = 0;
      for (int i = 0; i < count; ++i) {
        max |= Integer.toUnsignedLong(docIds[start + i]);
      }
      if (max <= 0xffffff) {
        if (runLenDocs < count / 1.34) {
          // runLen24 is too slow for decoding
          writeRunLen24(docIds, start, count, out);
        } else {
          writeInt24(docIds, start, count, out);
        }
      } else {
        if (runLenDocs < count / 1.25) {
          writeRunLen(docIds, start, count, out);
        } else {
          writeInt32(docIds, start, count, out);
        }
      }
    }
  }

  private static void writeDelta(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(DELTA);
    int previous = 0;
    for (int i = 0; i < count; ++i) {
      int doc = docIds[start + i];
      out.writeVInt(doc - previous);
      previous = doc;
    }
  }

  private static void writeRunLen(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(RUNLEN);
    int docId = docIds[start];
    int numDocs = 1;
    for (int i = 1; i < count; ++i) {
      int doc = docIds[start + i];
      if (doc != docId) {
        out.writeVInt(numDocs);
        out.writeInt(docId);
        docId = doc;
        numDocs = 1;
      } else {
        numDocs++;
      }
    }
    out.writeVInt(numDocs);
    out.writeInt(docId);
  }

  private static void writeDeltaRunLen(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(DELTARUNLEN);
    int docId = docIds[start];
    int previous = 0;
    int numDocs = 1;
    for (int i = 1; i < count; ++i) {
      int doc = docIds[start + i];
      if (doc != docId) {
        out.writeVInt(numDocs);
        out.writeVInt(docId - previous);
        previous = docId;
        docId = doc;
        numDocs = 1;
      } else {
        numDocs++;
      }
    }
    out.writeVInt(numDocs);
    out.writeVInt(docId - previous);
  }

  private static void writeRunLen24(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(RUNLEN24);
    int docId = docIds[start];
    int numDocs = 1;
    for (int i = 1; i < count; ++i) {
      int doc = docIds[start + i];
      if (doc != docId) {
        out.writeVInt(numDocs);
        out.writeShort((short) (docId >>> 8));
        out.writeByte((byte) docId);
        docId = doc;
        numDocs = 1;
      } else {
        numDocs++;
      }
    }
    out.writeVInt(numDocs);
    out.writeShort((short) (docId >>> 8));
    out.writeByte((byte) docId);
  }

  private static void writeInt24(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(INT24);
    for (int i = 0; i < count; ++i) {
      out.writeShort((short) (docIds[start + i] >>> 8));
      out.writeByte((byte) docIds[start + i]);
    }
  }

  private static void writeInt32(int[] docIds, int start, int count, DataOutput out) throws IOException {
    out.writeByte(INT32);
    for (int i = 0; i < count; ++i) {
      out.writeInt(docIds[start + i]);
    }
  }


  /** Read {@code count} integers into {@code docIDs}. */
  static void readInts(IndexInput in, int count, int[] docIDs) throws IOException {
    final byte bpv = in.readByte();
    switch (bpv) {
      case DELTA:
        readDeltaVInts(in, count, docIDs);
        break;
      case EQUALS:
        readAllEquals(in, count, docIDs);
        break;
      case RUNLEN:
        readRunLen(in, count, docIDs);
        break;
      case RUNLEN24:
        readRunLen24(in, count, docIDs);
        break;
      case DELTARUNLEN:
        readDeltaRunLen(in, count, docIDs);
        break;
      case INT24:
        readInts24(in, count, docIDs);
        break;
      case INT32:
        readInts32(in, count, docIDs);
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

  private static void readAllEquals(IndexInput in, int count, int[] docIDs) throws IOException {
    final int doc = in.readInt();
    for (int i = 0; i < count; i++) {
      docIDs[i] = doc;
    }
  }

  private static void readRunLen(IndexInput in, int count, int[] docIDs) throws IOException {
    for (int i = 0; i < count;) {
      final int runLen = in.readVInt();
      final int doc = in.readInt();
      for (int j = 0; j < runLen; j++) {
        docIDs[i++] = doc;
      }
    }
  }

  private static void readDeltaRunLen(IndexInput in, int count, int[] docIDs) throws IOException {
    int doc = 0;
    for (int i = 0; i < count;) {
      final int runLen = in.readVInt();
      doc += in.readVInt();
      for (int j = 0; j < runLen; j++) {
        docIDs[i++] = doc;
      }
    }
  }

  private static void readRunLen24(IndexInput in, int count, int[] docIDs) throws IOException {
    for (int i = 0; i < count;) {
      final int runLen = in.readVInt();
      final int doc = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
      for (int j = 0; j < runLen; j++) {
        docIDs[i++] = doc;
      }
    }
  }

  private static void readInts24(IndexInput in, int count, int[] docIDs) throws IOException {
    int i;
    for (i = 0; i < count - 7; i += 8) {
      final long l1 = in.readLong();
      final long l2 = in.readLong();
      final long l3 = in.readLong();
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

  private static void readInts32(IndexInput in, int count, int[] docIDs) throws IOException {
    for (int i = 0; i < count; i++) {
      docIDs[i] = in.readInt();
    }
  }

  /** Read {@code count} integers and feed the result directly to {@link IntersectVisitor#visit(int)}. */
  static void readInts(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    final byte bpv = in.readByte();
    switch (bpv) {
      case DELTA:
        readDeltaVInts(in, count, visitor);
        break;
      case EQUALS:
        readAllEquals(in, count, visitor);
        break;
      case RUNLEN:
        readRunLen(in, count, visitor);
        break;
      case RUNLEN24:
        readRunLen24(in, count, visitor);
        break;
      case DELTARUNLEN:
        readDeltaRunLen(in, count, visitor);
        break;
      case INT24:
        readInts24(in, count, visitor);
        break;
      case INT32:
        readInts32(in, count, visitor);
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

  private static void readAllEquals(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    visitor.visit(in.readInt());
  }

  private static void readRunLen(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count;) {
      i += in.readVInt();
      visitor.visit(in.readInt());
    }
  }

  private static void readDeltaRunLen(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int doc = 0;
    for (int i = 0; i < count;) {
      i += in.readVInt();
      doc += in.readVInt();
      visitor.visit(doc);
    }
  }

  private static void readRunLen24(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count;) {
      i += in.readVInt();
      visitor.visit((Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte()));
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

  private static void readInts32(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count; i++) {
      visitor.visit(in.readInt());
    }
  }
}
