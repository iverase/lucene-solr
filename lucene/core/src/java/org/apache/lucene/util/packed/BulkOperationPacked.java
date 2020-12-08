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
package org.apache.lucene.util.packed;



/**
 * Non-specialized {@link BulkOperation} for {@link PackedInts.Format#PACKED}.
 */
class BulkOperationPacked extends BulkOperation {

  private final int bitsPerValue;
  private final int longBlockCount;
  private final int longValueCount;
  private final int byteBlockCount;
  private final int byteValueCount;
  private final long mask;
  private final int intMask;

  public BulkOperationPacked(int bitsPerValue) {
    this.bitsPerValue = bitsPerValue;
    assert bitsPerValue > 0 && bitsPerValue <= 64;
    int blocks = bitsPerValue;
    while ((blocks & 1) == 0) {
      blocks >>>= 1;
    }
    this.longBlockCount = blocks;
    this.longValueCount = 64 * longBlockCount / bitsPerValue;
    int byteBlockCount = 8 * longBlockCount;
    int byteValueCount = longValueCount;
    while ((byteBlockCount & 1) == 0 && (byteValueCount & 1) == 0) {
      byteBlockCount >>>= 1;
      byteValueCount >>>= 1;
    }
    this.byteBlockCount = byteBlockCount;
    this.byteValueCount = byteValueCount;
    if (bitsPerValue == 64) {
      this.mask = ~0L;
    } else {
      this.mask = (1L << bitsPerValue) - 1;
    }
    this.intMask = (int) mask;
    assert longValueCount * bitsPerValue == 64 * longBlockCount;
  }

  @Override
  public int longBlockCount() {
    return longBlockCount;
  }

  @Override
  public int longValueCount() {
    return longValueCount;
  }

  @Override
  public int byteBlockCount() {
    return byteBlockCount;
  }

  @Override
  public int byteValueCount() {
    return byteValueCount;
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    int pos = 0;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      final int bitsLeft = 64 - pos;
      if (bitsLeft == bitsPerValue) {
        values[valuesOffset++] = (blocks[blocksOffset++] >>> pos) & mask;
        pos = 0;
      } else if (bitsLeft < bitsPerValue) {
        values[valuesOffset] = blocks[blocksOffset++] >>> pos;
        pos = bitsPerValue - bitsLeft;
        values[valuesOffset++] |= (blocks[blocksOffset] & ((1L << pos) - 1)) << bitsLeft;
      } else {
        values[valuesOffset++] = (blocks[blocksOffset] >>> pos) & mask;
        pos += bitsPerValue;
      }
    }
    assert pos == 0;
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values,
                     int valuesOffset, int iterations) {
    long nextValue = 0L;
    int pos = 0;
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final int bitsLeft = bitsPerValue - pos;
      final long bytes = blocks[blocksOffset++] & 0xFFL;
      if (bitsLeft > 8) {
        // just buffer
        nextValue |= bytes << pos;
        pos += 8;
      } else {
        // flush
        values[valuesOffset++] = nextValue | ((bytes & ((1L << bitsLeft) - 1)) << pos);
        pos = bitsLeft;
        while (pos <= 8 - bitsPerValue) {
          values[valuesOffset++] = (bytes >>> pos) & mask;
          pos += bitsPerValue;
        }
        nextValue = bytes >>> pos;
        pos = 8 - pos;
      }
    }
    assert pos == 0;
  }
  
  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    int pos = 0;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      final int bitsLeft = 64 - pos;
      if (bitsLeft == bitsPerValue) {
        values[valuesOffset++] = (int) (blocks[blocksOffset++] >>> pos) & intMask;
        pos = 0;
      } else if (bitsLeft < bitsPerValue) {
        values[valuesOffset] =(int) (blocks[blocksOffset++] >>> pos);
        pos = bitsPerValue - bitsLeft;
        values[valuesOffset++] |= (((blocks[blocksOffset])) & ((1L << pos) - 1)) << bitsLeft;
      } else {
        values[valuesOffset++] = (int)(blocks[blocksOffset] >>> pos) & intMask;
        pos+= bitsPerValue;
      }
    }
    assert pos == 0;
  }
  
  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    int nextValue = 0;
    int pos = 0;
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final int bitsLeft = bitsPerValue - pos;
      final int bytes = blocks[blocksOffset++] & 0xFF;
      if (bitsLeft > 8) {
        // just buffer
        nextValue |= bytes << pos;
        pos += 8;
      } else {
        // flush
        values[valuesOffset++] = (nextValue | ((bytes & ((1 << bitsLeft) - 1)) << pos));
        pos = bitsLeft;
        while (pos <= 8 - bitsPerValue) {
          values[valuesOffset++] = ((bytes >>> pos) & intMask);
          pos += bitsPerValue;
        }
        // then buffer
        nextValue = bytes >>> pos;
        pos = 8 - pos;
      }
    }
    assert pos == 0;
  }

  @Override
  public void encode(long[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int pos = 0;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      final int bitsLeft = 64 - pos;
      if (bitsLeft > bitsPerValue) {
        nextBlock |= values[valuesOffset++] << pos;
        pos += bitsPerValue;
      } else if (bitsLeft == bitsPerValue) {
        nextBlock |=  values[valuesOffset++] << pos;
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        pos = 0;
      } else { // bitsLeft < bitsPerValue
        nextBlock |= (values[valuesOffset] & ((1L << bitsLeft) - 1)) << pos;
        blocks[blocksOffset++] = nextBlock;
        nextBlock = values[valuesOffset++] >>> bitsLeft;
        pos = bitsPerValue - bitsLeft;
      }
    }
    assert pos == 0;
  }

  @Override
  public void encode(int[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int pos = 0;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      final int bitsLeft = 64 - pos;
      if (bitsLeft > bitsPerValue) {
        nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL) << pos;
        pos += bitsPerValue;
      } else if (bitsLeft == bitsPerValue) {
        nextBlock |=  (values[valuesOffset++] & 0xFFFFFFFFL) << pos;
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        pos = 0;
      } else { // bitsLeft < bitsPerValue
        nextBlock |= (values[valuesOffset] & ((1L << bitsLeft) - 1)) << pos;
        blocks[blocksOffset++] = nextBlock;
        nextBlock = values[valuesOffset++] >>> bitsLeft;
        pos = bitsPerValue - bitsLeft;
      }
    }
    assert pos == 0;
  }

  @Override
  public void encode(long[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    int nextBlock = 0;
    int pos = 0;
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final int bitsLeft = 8 - pos;
      final long v = values[valuesOffset++];
      assert PackedInts.unsignedBitsRequired(v) <= bitsPerValue;
      if (bitsPerValue < bitsLeft) {
        // just buffer
        nextBlock |= v << pos;
        pos += bitsPerValue;
      } else {
        // flush as many blocks as possible
        blocks[blocksOffset++] = (byte) (nextBlock | (v  << pos));
        int flushPos = bitsLeft;
        while (flushPos <= bitsPerValue - 8) {
          blocks[blocksOffset++] = (byte) (v >> flushPos);
          flushPos += 8;
        }
        // then buffer
        pos = bitsPerValue - flushPos;
        final long mask = ((1L << pos) - 1) << flushPos;
        nextBlock = (int)  (((v & mask) >>> flushPos));
      }
    }
    assert pos == 0;
  }

  @Override
  public void encode(int[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    int nextBlock = 0;
    int pos = 0;
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final int bitsLeft = 8 - pos;
      final int v = values[valuesOffset++];
      assert PackedInts.unsignedBitsRequired(v & 0xFFFFFFFFL) <= bitsPerValue;
      if (bitsPerValue < bitsLeft) {
        // just buffer
        nextBlock |= v << pos;
        pos += bitsPerValue;
      } else {
        // flush as many blocks as possible
        blocks[blocksOffset++] = (byte) (nextBlock | (v << pos));
        int flushPos = bitsLeft;
        while (flushPos <= bitsPerValue - 8) {
          blocks[blocksOffset++] = (byte) (v >>> flushPos);
          flushPos += 8;
        }
        // then buffer
        pos = bitsPerValue - flushPos;
        final int mask = ((1 << pos) - 1) << flushPos;
        nextBlock = (((v & mask) >>> flushPos));
      }
    }
    assert pos == 0;
  }
}
