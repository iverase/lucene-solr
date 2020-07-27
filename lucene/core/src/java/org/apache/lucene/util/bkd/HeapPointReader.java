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

import org.apache.lucene.util.BytesRef;

/**
 * Utility class to read buffered points from in-heap arrays.
 *
 * @lucene.internal
 * */
public final class HeapPointReader implements PointReader {
  private int curRead;
  final byte[][] block;
  final BKDConfig config;
  final int end;
  private final HeapPointValue pointValue;

  public HeapPointReader(BKDConfig config, byte[][] block, int start, int end) {
    this.block = block;
    curRead = start-1;
    this.end = end;
    this.config = config;
    if (start < end) {
      this.pointValue = new HeapPointValue(config, block);
    } else {
      //no values
      this.pointValue = null;
    }
  }

  @Override
  public boolean next() {
    curRead++;
    return curRead < end;
  }

  @Override
  public PointValue pointValue() {
    pointValue.setOffset(curRead);
    return pointValue;
  }

  @Override
  public void close() {
  }

  /**
   * Reusable implementation for a point value on-heap
   */
  static class HeapPointValue implements PointValue {

    final BytesRef packedValue;
    final BytesRef packedValueDocID;
    final int packedValueLength;
    final byte[][] values;

    HeapPointValue(BKDConfig config, byte[][] values) {
      this.values = values;
      this.packedValueLength = config.packedBytesLength;
      this.packedValue = new BytesRef(values[0], 0, packedValueLength);
      this.packedValueDocID = new BytesRef(values[0], 0, config.bytesPerDoc);
    }

    /**
     * Sets a new value by changing the offset.
     */
    public void setOffset(int offset) {
      packedValue.bytes = values[offset];
      packedValueDocID.bytes = values[offset];
    }

    @Override
    public BytesRef packedValue() {
      return packedValue;
    }

    @Override
    public int docID() {
      return ((packedValueDocID.bytes[packedValueLength] & 0xFF) << 24) | ((packedValueDocID.bytes[packedValueLength+1] & 0xFF) << 16)
          | ((packedValueDocID.bytes[packedValueLength+2] & 0xFF) <<  8) |  (packedValueDocID.bytes[packedValueLength+3] & 0xFF);
    }

    @Override
    public BytesRef packedValueDocIDBytes() {
      return packedValueDocID;
    }
  }
}
