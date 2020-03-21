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

// TODO
//   - allow variable length byte[] (across docs and dims), but this is quite a bit more hairy
//   - we could also index "auto-prefix terms" here, and use better compression, and maybe only use for the "fully contained" case so we'd
//     only index docIDs
//   - the index could be efficiently encoded as an FST, so we don't have wasteful
//     (monotonic) long[] leafBlockFPs; or we could use MonotonicLongValues ... but then
//     the index is already plenty small: 60M OSM points --> 1.1 MB with 128 points
//     per leaf, and you can reduce that by putting more points per leaf
//   - we could use threads while building; the higher nodes are very parallelizable

/**
 *  Recursively builds a block KD-tree to assign all incoming points in N-dim space to smaller
 *  and smaller N-dim rectangles (cells) until the number of points in a given
 *  rectangle is &lt;= <code>maxPointsInLeafNode</code>.  The tree is
 *  fully balanced, which means the leaf nodes will have between 50% and 100% of
 *  the requested <code>maxPointsInLeafNode</code>.  Values that fall exactly
 *  on a cell boundary may be in either cell.
 *
 *  <p>The number of dimensions can be 1 to 8, but every byte[] value is fixed length.
 *
 *  <p>This consumes heap during writing: it allocates a <code>Long[numLeaves]</code>,
 *  a <code>byte[numLeaves*(1+bytesPerDim)]</code> and then uses up to the specified
 *  {@code maxMBSortInHeap} heap space for writing.
 *
 *  <p>
 *  <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> / (1+bytesPerDim)
 *  total points.
 *
 * @lucene.experimental */

public class BKDOnHeapWriter {
  /** Number of splits before we compute the exact bounding box of an inner node. */
  private static final int SPLITS_BEFORE_EXACT_BOUNDS = 4;

  /** The configuration of this BKD points */
  protected final BKDConfig config;

  final byte[] scratchDiff;
  final byte[] scratch;

  final BytesRef scratchBytesRef1 = new BytesRef();
  final BytesRef scratchBytesRef2 = new BytesRef();
  final int[] commonPrefixLengths;

  //protected final FixedBitSet docsSeen;

  /** Minimum per-dim values, packed */
  protected final byte[] minPackedValue;

  /** Maximum per-dim values, packed */
  protected final byte[] maxPackedValue;

  protected long pointCount;

  /** An upper bound on how many points the caller will add (includes deletions) */
  private final long totalPointCount;

  private final int maxDoc;


  public BKDOnHeapWriter(BKDConfig config, int maxDoc, long totalPointCount)  {
    verifyParams(totalPointCount);
    this.config = config;
    this.totalPointCount = totalPointCount;
    this.maxDoc = maxDoc;


    scratchDiff = new byte[config.bytesPerDim];
    scratch = new byte[config.packedBytesLength];
    commonPrefixLengths = new int[config.numDataDims];

    minPackedValue = new byte[config.packedIndexBytesLength];
    maxPackedValue = new byte[config.packedIndexBytesLength];

  }

  private static void verifyParams(long totalPointCount) {
    if (totalPointCount < 0) {
      throw new IllegalArgumentException("totalPointCount must be >=0 (got: " + totalPointCount + ")");
    }
  }

  /** Write a field from a {@link MutablePointValues}. This way of writing
   *  points is faster than regular writes with {@link BKDWriter#add} since
   *  there is opportunity for reordering points before writing them to
   *  disk. This method does not use transient disk in order to reorder points.
   */
  public long writeField(BKDIndexWriter out, MutablePointValues values) throws IOException {
    /* we recursively pick the split dimension, compute the
    * median value and partition other values around it. */

    if (pointCount != 0) {
      throw new IllegalStateException("cannot mix add and writeField");
    }

    long countPerLeaf = pointCount = values.size();
    if (pointCount > totalPointCount) {
      throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + pointCount+ " values");
    }
    long innerNodeCount = 1;

    while (countPerLeaf > config.maxPointsInLeafNode) {
      countPerLeaf = (countPerLeaf+1)/2;
      innerNodeCount *= 2;
    }

    int numLeaves = Math.toIntExact(innerNodeCount);

    checkMaxLeafNodeCount(numLeaves);

    final byte[] splitPackedValues = new byte[numLeaves * (config.bytesPerDim + 1)];
    final long[] leafBlockFPs = new long[numLeaves];

    // compute the min/max for this slice
    MutablePointsReaderUtils.computePackedValueBounds(config, values, 0, Math.toIntExact(pointCount), minPackedValue, maxPackedValue, scratchBytesRef1);
//

    final int[] parentSplits = new int[config.numIndexDims];
    build(1, numLeaves, values, 0, Math.toIntExact(pointCount), out,
        minPackedValue.clone(), maxPackedValue.clone(), parentSplits,
        splitPackedValues, leafBlockFPs);
    assert Arrays.equals(parentSplits, new int[config.numIndexDims]);

    long indexFP = out.getFilePointer();
    out.writeIndex(config, Math.toIntExact(countPerLeaf), leafBlockFPs, splitPackedValues, minPackedValue, maxPackedValue, pointCount, values.getDocCount());
    return indexFP;
  }

  /* Recursively reorders the provided reader and writes the bkd-tree on the fly; this method is used
   * when we are writing a new segment directly from IndexWriter's indexing buffer (MutablePointsReader). */
  private void build(int nodeID, int leafNodeOffset,
                     MutablePointValues reader, int from, int to,
                     BKDIndexWriter out,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     int[] parentSplits,
                     byte[] splitPackedValues,
                     long[] leafBlockFPs) throws IOException {

    if (nodeID >= leafNodeOffset) {
      // leaf node
      final int count = to - from;
      assert count <= config.maxPointsInLeafNode;
      // compute common prefix
      MutablePointsReaderUtils.computeCommonPrefix(config, commonPrefixLengths,
          reader, from, to, scratchBytesRef1, scratchBytesRef2);
      // Find the dimension that has the least number of unique bytes at commonPrefixLengths[dim]
      final int sortedDim = MutablePointsReaderUtils.computeSortedDim(config, commonPrefixLengths, reader, from, to);
      // sort by sortedDim
      MutablePointsReaderUtils.sortByDim(config, sortedDim, commonPrefixLengths, reader, from, to, scratchBytesRef1, scratchBytesRef2);
      // compute leaf cardinality
      final int leafCardinality = MutablePointsReaderUtils.computeCardinality(config, commonPrefixLengths, reader, from, to, scratchBytesRef1, scratchBytesRef2);
      // Save the block file pointer:
      leafBlockFPs[nodeID - leafNodeOffset] = out.getFilePointer();

      final BKDLeafBlock packedValues = new BKDLeafBlock() {
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

      assert BKDLeafBlock.valuesInOrderAndBounds(config, sortedDim, minPackedValue, maxPackedValue, packedValues);
      out.writeLeafBlock(config, packedValues, commonPrefixLengths, sortedDim, leafCardinality, scratch);
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
          MutablePointsReaderUtils.computePackedValueBounds(config, reader, from, to, minPackedValue, maxPackedValue, scratchBytesRef1);
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

      MutablePointsReaderUtils.partition(config, maxDoc, splitDim, commonPrefixLen, reader, from, to, mid, scratchBytesRef1, scratchBytesRef2);

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
      build(nodeID * 2, leafNodeOffset, reader, from, mid, out,
          minPackedValue, maxSplitPackedValue, parentSplits,
          splitPackedValues, leafBlockFPs);
      build(nodeID * 2 + 1, leafNodeOffset, reader, mid, to, out,
          minSplitPackedValue, maxPackedValue, parentSplits,
          splitPackedValues, leafBlockFPs);
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
    for(int dim=0; dim < config.numIndexDims; dim++) {
      NumericUtils.subtract(config.bytesPerDim, dim, maxPackedValue, minPackedValue, scratchDiff);
      if (splitDim == -1 || Arrays.compareUnsigned(scratchDiff, 0, config.bytesPerDim, scratch, 0, config.bytesPerDim) > 0) {
        System.arraycopy(scratchDiff, 0, scratch, 0, config.bytesPerDim);
        splitDim = dim;
      }
    }
    return splitDim;
  }

//  public static long writeField(BKDConfig config, BKDIndexWriter out, MutablePointValues values, int maxDoc, long totalPointCount) throws IOException {
//    BKDOnHeapWriter writer = new BKDOnHeapWriter(config, maxDoc, totalPointCount);
//    return writer.writeField(out, values);
//  }
}
