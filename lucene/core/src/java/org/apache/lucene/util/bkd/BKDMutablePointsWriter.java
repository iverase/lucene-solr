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

import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;



/**
 *  Recursively builds a block KD-tree to assign all incoming in-memory points in N-dim space to smaller
 *  and smaller N-dim rectangles (cells) until the number of points in a given
 *  rectangle is &lt;= <code>maxPointsInLeafNode</code>.  The tree is
 *  fully balanced, which means the leaf nodes will have between 50% and 100% of
 *  the requested <code>maxPointsInLeafNode</code>.  Values that fall exactly
 *  on a cell boundary may be in either cell.
 *
 *  <p>The number of dimensions can be 1 to 8, but every byte[] value is fixed length.
 *
 *  <p>This consumes heap during writing: it allocates a <code>Long[numLeaves]</code>,
 *  a <code>byte[numLeaves*(1+bytesPerDim)]</code>.
 *
 *  <p>
 *  <b>NOTE</b>: This can write at most Integer.MAX_VALUE total points.
 *
 * @lucene.experimental */
public class BKDMutablePointsWriter {


  /** Number of splits before we compute the exact bounding box of an inner node. */
  private static final int SPLITS_BEFORE_EXACT_BOUNDS = 4;

  /** dimensions length and size */
  protected final BKDConfig config;

  final byte[] scratchDiff;
  final byte[] scratch1;

  final BytesRef scratchBytesRef1 = new BytesRef();
  final BytesRef scratchBytesRef2 = new BytesRef();
  final int[] commonPrefixLengths;

  /** Minimum per-dim values, packed */
  protected final byte[] minPackedValue;

  /** Maximum per-dim values, packed */
  protected final byte[] maxPackedValue;

  public BKDMutablePointsWriter(BKDConfig config) throws IOException {

    this.config = config;
    // scratch objects
    scratchDiff = new byte[config.bytesPerDim];
    scratch1 = new byte[config.packedBytesLength];
    commonPrefixLengths = new int[config.numDims];
    minPackedValue = new byte[config.packedIndexBytesLength];
    maxPackedValue = new byte[config.packedIndexBytesLength];
  }

  /** Write a field from a {@link MutablePointValues}. This way of writing
   *  points is faster than regular writes with {@link BKDWriter} since
   *  there is opportunity for reordering points in-memory before writing them to
   *  disk. This method does not use transient disk in order to reorder points.
   */
  public long writeField(BKDIndexWriter indexWriter, MutablePointValues values, int maxDoc) throws IOException {

    int pointCount =  Math.toIntExact(values.size());

    int countPerLeaf = pointCount;
    int numLeaves = 1;

    while (countPerLeaf > config.maxPointsInLeafNode) {
      countPerLeaf = (countPerLeaf + 1) / 2;
      numLeaves *= 2;
    }

    checkMaxLeafNodeCount(numLeaves);

    final byte[] splitPackedValues = new byte[numLeaves * (config.bytesPerDim + 1)];
    final long[] leafBlockFPs = new long[numLeaves];

    // compute the min/max for this slice
    computePackedValueBounds(values, 0, pointCount, minPackedValue, maxPackedValue, scratchBytesRef1);

    final int[] parentSplits = new int[config.numIndexDims];
    build(1, numLeaves, values, 0, pointCount, indexWriter,
        minPackedValue.clone(), maxPackedValue.clone(), parentSplits,
        splitPackedValues, leafBlockFPs, maxDoc);
    assert Arrays.equals(parentSplits, new int[config.numIndexDims]);

    long indexFP = indexWriter.getFilePointer();
    indexWriter.writeIndex(config, countPerLeaf , leafBlockFPs, splitPackedValues, minPackedValue, maxPackedValue, pointCount, values.getDocCount());
    return indexFP;
  }

  /* Recursively reorders the provided reader and writes the bkd-tree on the fly; this method is used
   * when we are writing a new segment directly from IndexWriter's indexing buffer (MutablePointsReader). */
  private void build(int nodeID, int leafNodeOffset,
                     MutablePointValues reader, int from, int to,
                     BKDIndexWriter indexWriter,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     int[] parentSplits,
                     byte[] splitPackedValues,
                     long[] leafBlockFPs,
                     int maxDoc) throws IOException {

    if (nodeID >= leafNodeOffset) {
      // leaf node
      final int count = to - from;
      assert count <= config.maxPointsInLeafNode;

      // Compute common prefixes
      Arrays.fill(commonPrefixLengths, config.bytesPerDim);
      reader.getValue(from, scratchBytesRef1);
      for (int i = from + 1; i < to; ++i) {
        reader.getValue(i, scratchBytesRef2);
        for (int dim = 0; dim < config.numDims; dim++) {
          final int offset = dim * config.bytesPerDim;
          int dimensionPrefixLength = commonPrefixLengths[dim];
          commonPrefixLengths[dim] = Arrays.mismatch(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset,
              scratchBytesRef1.offset + offset + dimensionPrefixLength,
              scratchBytesRef2.bytes, scratchBytesRef2.offset + offset,
              scratchBytesRef2.offset + offset + dimensionPrefixLength);
          if (commonPrefixLengths[dim] == -1) {
            commonPrefixLengths[dim] = dimensionPrefixLength;
          }
        }
      }

      // Find the dimension that has the least number of unique bytes at commonPrefixLengths[dim]
      FixedBitSet[] usedBytes = new FixedBitSet[config.numDims];
      for (int dim = 0; dim < config.numDims; ++dim) {
        if (commonPrefixLengths[dim] < config.bytesPerDim) {
          usedBytes[dim] = new FixedBitSet(256);
        }
      }
      for (int i = from + 1; i < to; ++i) {
        for (int dim = 0; dim < config.numDims; dim++) {
          if (usedBytes[dim] != null) {
            byte b = reader.getByteAt(i, dim * config.bytesPerDim + commonPrefixLengths[dim]);
            usedBytes[dim].set(Byte.toUnsignedInt(b));
          }
        }
      }
      int sortedDim = 0;
      int sortedDimCardinality = Integer.MAX_VALUE;
      for (int dim = 0; dim < config.numDims; ++dim) {
        if (usedBytes[dim] != null) {
          final int cardinality = usedBytes[dim].cardinality();
          if (cardinality < sortedDimCardinality) {
            sortedDim = dim;
            sortedDimCardinality = cardinality;
          }
        }
      }

      // sort by sortedDim
      MutablePointsReaderUtils.sortByDim(config, sortedDim, commonPrefixLengths,
          reader, from, to, scratchBytesRef1, scratchBytesRef2);

      BytesRef comparator = scratchBytesRef1;
      BytesRef collector = scratchBytesRef2;
      reader.getValue(from, comparator);
      int leafCardinality = 1;
      for (int i = from + 1; i < to; ++i) {
        reader.getValue(i, collector);
        for (int dim = 0; dim < config.numDims; dim++) {
          final int start = dim * config.bytesPerDim + commonPrefixLengths[dim];
          final int end = dim * config.bytesPerDim + config.bytesPerDim;
          if (Arrays.mismatch(collector.bytes, collector.offset + start, collector.offset + end,
              comparator.bytes, comparator.offset + start, comparator.offset + end) != -1) {
            leafCardinality++;
            BytesRef scratch = collector;
            collector = comparator;
            comparator = scratch;
            break;
          }
        }
      }
      // Save the block file pointer:
      leafBlockFPs[nodeID - leafNodeOffset] = indexWriter.getFilePointer();

      BKDLeafBlock packedValues = new BKDLeafBlock() {
        @Override
        public int count() {
          return count;
        }

        @Override
        public BytesRef packedValue(int position) {
          reader.getValue(from + position, scratchBytesRef1);
          return scratchBytesRef1;
        }

        @Override
        public int docId(int position) {
          return reader.getDocID(from + position);
        }
      };

      assert BKDLeafBlock.valuesInOrderAndBounds(config, packedValues, sortedDim, minPackedValue, maxPackedValue);

      indexWriter.writeLeafBlock(config, packedValues, commonPrefixLengths, sortedDim, leafCardinality);
    } else {
      // inner node

      final int splitDim;
      // compute the split dimension and partition around it
      if (config.numIndexDims == 1) {
        splitDim = 0;
      } else {
        // for dimensions > 2 we recompute the bounds for the current inner node to help the algorithm choose best
        // split dimensions. Because it is an expensive operation, the frequency we recompute the bounds is given
        // by SPLITS_BEFORE_EXACT_BOUNDS.
        if (nodeID > 1 && config.numIndexDims > 2 && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
          computePackedValueBounds(reader, from, to, minPackedValue, maxPackedValue, scratchBytesRef1);
        }
        splitDim = split(minPackedValue, maxPackedValue, parentSplits);
      }

      final int mid = (from + to + 1) >>> 1;

      int commonPrefixLen = Arrays.mismatch(minPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim, maxPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim);
      if (commonPrefixLen == -1) {
        commonPrefixLen = config.bytesPerDim;
      }

      MutablePointsReaderUtils.partition(config, maxDoc, splitDim, commonPrefixLen,
          reader, from, to, mid, scratchBytesRef1, scratchBytesRef2);

      // set the split value
      final int address = nodeID * (1 + config.bytesPerDim);
      splitPackedValues[address] = (byte) splitDim;
      reader.getValue(mid, scratchBytesRef1);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * config.bytesPerDim, splitPackedValues, address + 1, config.bytesPerDim);

      byte[] minSplitPackedValue = ArrayUtil.copyOfSubArray(minPackedValue, 0, config.packedIndexBytesLength);
      byte[] maxSplitPackedValue = ArrayUtil.copyOfSubArray(maxPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * config.bytesPerDim,
          minSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * config.bytesPerDim,
          maxSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);

      // recurse
      parentSplits[splitDim]++;
      build(nodeID * 2, leafNodeOffset, reader, from, mid, indexWriter,
          minPackedValue, maxSplitPackedValue, parentSplits,
          splitPackedValues, leafBlockFPs, maxDoc);
      build(nodeID * 2 + 1, leafNodeOffset, reader, mid, to, indexWriter,
          minSplitPackedValue, maxPackedValue, parentSplits,
          splitPackedValues, leafBlockFPs, maxDoc);
      parentSplits[splitDim]--;
    }
  }

  private void checkMaxLeafNodeCount(int numLeaves) {
    if ((1 + config.bytesPerDim) * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + config.maxPointsInLeafNode + ") and reindex");
    }
  }

  /**
   * Pick the next dimension to split.
   * @param minPackedValue the min values for all dimensions
   * @param maxPackedValue the max values for all dimensions
   * @param parentSplits how many times each dim has been split on the parent levels
   * @return the dimension to split
   */
  protected int split(byte[] minPackedValue, byte[] maxPackedValue, int[] parentSplits) {
    // First look at whether there is a dimension that has split less than 2x less than
    // the dim that has most splits, and return it if there is such a dimension and it
    // does not only have equals values. This helps ensure all dimensions are indexed.
    int maxNumSplits = 0;
    for (int numSplits : parentSplits) {
      maxNumSplits = Math.max(maxNumSplits, numSplits);
    }
    for (int dim = 0; dim < config.numIndexDims; ++dim) {
      final int offset = dim * config.bytesPerDim;
      if (parentSplits[dim] < maxNumSplits / 2 &&
          Arrays.compareUnsigned(minPackedValue, offset, offset + config.bytesPerDim, maxPackedValue, offset, offset + config.bytesPerDim) != 0) {
        return dim;
      }
    }

    // Find which dim has the largest span so we can split on it:
    int splitDim = -1;
    for(int dim = 0; dim < config.numIndexDims; dim++) {
      NumericUtils.subtract(config.bytesPerDim, dim, maxPackedValue, minPackedValue, scratchDiff);
      if (splitDim == -1 || Arrays.compareUnsigned(scratchDiff, 0, config.bytesPerDim, scratch1, 0, config.bytesPerDim) > 0) {
        System.arraycopy(scratchDiff, 0, scratch1, 0, config.bytesPerDim);
        splitDim = dim;
      }
    }

    //System.out.println("SPLIT: " + splitDim);
    return splitDim;
  }

  private void computePackedValueBounds(MutablePointValues values, int from, int to, byte[] minPackedValue, byte[] maxPackedValue, BytesRef scratch) {
    if (from == to) {
      return;
    }
    values.getValue(from, scratch);
    System.arraycopy(scratch.bytes, scratch.offset, minPackedValue, 0, config.packedIndexBytesLength);
    System.arraycopy(scratch.bytes, scratch.offset, maxPackedValue, 0, config.packedIndexBytesLength);
    for (int i = from + 1 ; i < to; ++i) {
      values.getValue(i, scratch);
      for(int dim = 0; dim < config.numIndexDims; dim++) {
        final int startOffset = dim * config.bytesPerDim;
        final int endOffset = startOffset + config.bytesPerDim;
        if (Arrays.compareUnsigned(scratch.bytes, scratch.offset + startOffset, scratch.offset + endOffset, minPackedValue, startOffset, endOffset) < 0) {
          System.arraycopy(scratch.bytes, scratch.offset + startOffset, minPackedValue, startOffset, config.bytesPerDim);
        } else if (Arrays.compareUnsigned(scratch.bytes, scratch.offset + startOffset, scratch.offset + endOffset, maxPackedValue, startOffset, endOffset) > 0) {
          System.arraycopy(scratch.bytes, scratch.offset + startOffset, maxPackedValue, startOffset, config.bytesPerDim);
        }
      }
    }
  }
}
