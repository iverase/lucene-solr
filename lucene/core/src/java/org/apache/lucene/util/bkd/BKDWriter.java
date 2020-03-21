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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
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

public class BKDWriter implements Closeable {
  /** Default maximum heap to use, before spilling to (slower) disk */
  public static final float DEFAULT_MAX_MB_SORT_IN_HEAP = 16.0f;

  /** Number of splits before we compute the exact bounding box of an inner node. */
  private static final int SPLITS_BEFORE_EXACT_BOUNDS = 4;

  /** The configuration of this BKD points */
  protected final BKDConfig config;

  final TrackingDirectoryWrapper tempDir;
  final String tempFileNamePrefix;
  final double maxMBSortInHeap;

  final byte[] scratchDiff;
  final byte[] scratch;

  final int[] commonPrefixLengths;

  protected final FixedBitSet docsSeen;

  private PointWriter pointWriter;
  private boolean finished;

  private IndexOutput tempInput;

  private final int maxPointsSortInHeap;

  /** Minimum per-dim values, packed */
  protected final byte[] minPackedValue;

  /** Maximum per-dim values, packed */
  protected final byte[] maxPackedValue;

  protected long pointCount;

  /** An upper bound on how many points the caller will add (includes deletions) */
  private final long totalPointCount;

  public BKDWriter(BKDConfig config, int maxDoc, Directory tempDir, String tempFileNamePrefix, double maxMBSortInHeap, long totalPointCount) throws IOException {
    verifyParams(maxMBSortInHeap, totalPointCount);
    // We use tracking dir to deal with removing files on exception, so each place that
    // creates temp files doesn't need crazy try/finally/sucess logic:
    this.tempDir = new TrackingDirectoryWrapper(tempDir);
    this.tempFileNamePrefix = tempFileNamePrefix;
    this.config = config;
    this.totalPointCount = totalPointCount;
    docsSeen = new FixedBitSet(maxDoc);


    scratchDiff = new byte[config.bytesPerDim];
    scratch = new byte[config.packedBytesLength];
    commonPrefixLengths = new int[config.numDataDims];

    minPackedValue = new byte[config.packedIndexBytesLength];
    maxPackedValue = new byte[config.packedIndexBytesLength];
    Arrays.fill(minPackedValue, (byte) 0xff);
    Arrays.fill(maxPackedValue, (byte) 0);

    // Maximum number of points we hold in memory at any time
    maxPointsSortInHeap = (int) ((maxMBSortInHeap * 1024 * 1024) / (config.bytesPerDoc));

    // Finally, we must be able to hold at least the leaf node in heap during build:
    if (maxPointsSortInHeap < config.maxPointsInLeafNode) {
      throw new IllegalArgumentException("maxMBSortInHeap=" + maxMBSortInHeap + " only allows for maxPointsSortInHeap=" +
          maxPointsSortInHeap + ", but this is less than maxPointsInLeafNode=" + config.maxPointsInLeafNode +
          "; either increase maxMBSortInHeap or decrease maxPointsInLeafNode");
    }

    this.maxMBSortInHeap = maxMBSortInHeap;
    initPointWriter();
  }

  private static void verifyParams(double maxMBSortInHeap, long totalPointCount) {
    if (maxMBSortInHeap < 0.0) {
      throw new IllegalArgumentException("maxMBSortInHeap must be >= 0.0 (got: " + maxMBSortInHeap + ")");
    }
    if (totalPointCount < 0) {
      throw new IllegalArgumentException("totalPointCount must be >=0 (got: " + totalPointCount + ")");
    }
  }

  private void initPointWriter() throws IOException {
    assert pointWriter == null : "Point writer is already initialized";
    //total point count is an estimation but the final point count must be equal or lower to that number.
    if (totalPointCount > maxPointsSortInHeap) {
      pointWriter = new OfflinePointWriter(tempDir, tempFileNamePrefix, config.packedBytesLength, "spill", 0);
      tempInput = ((OfflinePointWriter)pointWriter).out;
    } else {
      pointWriter = new HeapPointWriter(Math.toIntExact(totalPointCount), config.packedBytesLength);
    }
  }


  public void add(byte[] packedValue, int docID) throws IOException {
    if (packedValue.length != config.packedBytesLength) {
      throw new IllegalArgumentException("packedValue should be length=" + config.packedBytesLength + " (got: " + packedValue.length + ")");
    }
    if (pointCount >= totalPointCount) {
      throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + (pointCount + 1) + " values");
    }

    for (int dim = 0; dim < config.numIndexDims; dim++) {
      int offset = dim * config.bytesPerDim;
      if (Arrays.compareUnsigned(packedValue, offset, offset + config.bytesPerDim, minPackedValue, offset, offset + config.bytesPerDim) < 0) {
        System.arraycopy(packedValue, offset, minPackedValue, offset, config.bytesPerDim);
      }
      if (Arrays.compareUnsigned(packedValue, offset, offset + config.bytesPerDim, maxPackedValue, offset, offset + config.bytesPerDim) > 0) {
        System.arraycopy(packedValue, offset, maxPackedValue, offset, config.bytesPerDim);
      }
    }

    pointWriter.append(packedValue, docID);
    pointCount++;
    docsSeen.set(docID);
  }

  /** How many points have been added so far */
  public long getPointCount() {
    return pointCount;
  }

  /** Writes the BKD tree to the provided {@link IndexOutput} and returns the file offset where index was written. */
  public long finish(BKDIndexWriter out) throws IOException {
    // Catch user silliness:
    if (finished == true) {
      throw new IllegalStateException("already finished");
    }

    if (pointCount == 0) {
      throw new IllegalStateException("must index at least one point");
    }

    //mark as finished
    finished = true;

    pointWriter.close();
    BKDRadixSelector.PathSlice points = new BKDRadixSelector.PathSlice(pointWriter, 0, pointCount);
    //clean up pointers
    tempInput = null;
    pointWriter = null;

    long countPerLeaf = pointCount;
    long innerNodeCount = 1;

    while (countPerLeaf > config.maxPointsInLeafNode) {
      countPerLeaf = (countPerLeaf+1)/2;
      innerNodeCount *= 2;
    }

    int numLeaves = (int) innerNodeCount;

    checkMaxLeafNodeCount(numLeaves);

    // NOTE: we could save the 1+ here, to use a bit less heap at search time, but then we'd need a somewhat costly check at each
    // step of the recursion to recompute the split dim:

    // Indexed by nodeID, but first (root) nodeID is 1.  We do 1+ because the lead byte at each recursion says which dim we split on.
    byte[] splitPackedValues = new byte[Math.toIntExact(numLeaves * (1 + config.bytesPerDim))];

    // +1 because leaf count is power of 2 (e.g. 8), and innerNodeCount is power of 2 minus 1 (e.g. 7)
    long[] leafBlockFPs = new long[numLeaves];

    // Make sure the math above "worked":
    assert pointCount / numLeaves <= config.maxPointsInLeafNode: "pointCount=" + pointCount + " numLeaves=" + numLeaves + " maxPointsInLeafNode=" + config.maxPointsInLeafNode;

    //We re-use the selector so we do not need to create an object every time.
    BKDRadixSelector radixSelector = new BKDRadixSelector(config, maxPointsSortInHeap, tempDir, tempFileNamePrefix);

    boolean success = false;
    try {
      final LeafBlock leafBlock = new LeafBlock();
      final int[] parentSplits = new int[config.numIndexDims];
      build(1, numLeaves, points,
          out, radixSelector,
          minPackedValue.clone(), maxPackedValue.clone(),
          parentSplits,
          splitPackedValues,
          leafBlockFPs,
          leafBlock);
      assert Arrays.equals(parentSplits, new int[config.numIndexDims]);

      // If no exception, we should have cleaned everything up:
      assert tempDir.getCreatedFiles().isEmpty();
      success = true;
    } finally {
      if (success == false) {
        IOUtils.deleteFilesIgnoringExceptions(tempDir, tempDir.getCreatedFiles());
      }
    }

    // Write index:
    long indexFP = out.getFilePointer();
    out.writeIndex(config, Math.toIntExact(countPerLeaf), leafBlockFPs, splitPackedValues, minPackedValue, maxPackedValue, pointCount, docsSeen.cardinality());
    return indexFP;
  }

  @Override
  public void close() throws IOException {
    finished = true;
    if (tempInput != null) {
      // NOTE: this should only happen on exception, e.g. caller calls close w/o calling finish:
      try {
        tempInput.close();
      } finally {
        tempDir.deleteFile(tempInput.getName());
        tempInput = null;
      }
    }
  }

  /** Called on exception, to check whether the checksum is also corrupt in this source, and add that
   *  information (checksum matched or didn't) as a suppressed exception. */
  private Error verifyChecksum(Throwable priorException, PointWriter writer) throws IOException {
    assert priorException != null;

    // TODO: we could improve this, to always validate checksum as we recurse, if we shared left and
    // right reader after recursing to children, and possibly within recursed children,
    // since all together they make a single pass through the file.  But this is a sizable re-org,
    // and would mean leaving readers (IndexInputs) open for longer:
    if (writer instanceof OfflinePointWriter) {
      // We are reading from a temp file; go verify the checksum:
      String tempFileName = ((OfflinePointWriter) writer).name;
      if (tempDir.getCreatedFiles().contains(tempFileName)) {
        try (ChecksumIndexInput in = tempDir.openChecksumInput(tempFileName, IOContext.READONCE)) {
          CodecUtil.checkFooter(in, priorException);
        }
      }
    }

    // We are reading from heap; nothing to add:
    throw IOUtils.rethrowAlways(priorException);
  }

  /** Pull a partition back into heap once the point count is low enough while recursing. */
  private HeapPointWriter switchToHeap(PointWriter source) throws IOException {
    int count = Math.toIntExact(source.count());
    try (PointReader reader = source.getReader(0, source.count());
         HeapPointWriter writer = new HeapPointWriter(count, config.packedBytesLength)) {
      for(int i=0;i<count;i++) {
        boolean hasNext = reader.next();
        assert hasNext;
        writer.append(reader.pointValue());
      }
      source.destroy();
      return writer;
    } catch (Throwable t) {
      throw verifyChecksum(t, source);
    }
  }

  /** The point writer contains the data that is going to be splitted using radix selection.
   /*  This method is used when we are merging previously written segments, in the numDims > 1 case. */
  private void build(int nodeID, int leafNodeOffset,
                     BKDRadixSelector.PathSlice points,
                     BKDIndexWriter out,
                     BKDRadixSelector radixSelector,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     int[] parentSplits,
                     byte[] splitPackedValues,
                     long[] leafBlockFPs,
                     LeafBlock leafBlock) throws IOException {

    if (nodeID >= leafNodeOffset) {
      // Leaf node: write block
      // We can write the block in any order so by default we write it sorted by the dimension that has the
      // least number of unique bytes at commonPrefixLengths[dim], which makes compression more efficient
      HeapPointWriter heapSource;
      if (points.writer instanceof HeapPointWriter == false) {
        // Adversarial cases can cause this, e.g. merging big segments with most of the points deleted
        heapSource = switchToHeap(points.writer);
      } else {
        heapSource = (HeapPointWriter) points.writer;
      }

      final int from = Math.toIntExact(points.start);
      final int to = Math.toIntExact(points.start + points.count);
      final int count = to - from;
      assert count > 0: "nodeID=" + nodeID + " leafNodeOffset=" + leafNodeOffset;

      // compute common prefixes, we store the values in scratch1
      heapSource.computeCommonPrefix(from, to, config.numDataDims, config.bytesPerDim, commonPrefixLengths, scratch);
      // compute sorted dim
      final int sortedDim = heapSource.computeSortedDim(from, to, config.numDataDims, config.bytesPerDim, commonPrefixLengths);
      // sort the chosen dimension
      radixSelector.heapRadixSort(heapSource, from, to, sortedDim, commonPrefixLengths[sortedDim]);
      // compute cardinality
      final int leafCardinality = heapSource.computeCardinality(from, to, config.numDataDims, config.bytesPerDim, commonPrefixLengths);
      // Save the block file pointer:
      leafBlockFPs[nodeID - leafNodeOffset] = out.getFilePointer();

      leafBlock.setRange(heapSource, from, count);

      assert BKDLeafBlock.valuesInOrderAndBounds(config, sortedDim, minPackedValue, maxPackedValue, leafBlock);
      out.writeLeafBlock(config, leafBlock, commonPrefixLengths, sortedDim,  leafCardinality, scratch);
    } else {
      // Inner node: partition/recurse
      final int splitDim;
      if (config.numIndexDims == 1) {
        splitDim = 0;
      } else {
        // for dimensions > 2 we recompute the bounds for the current inner node to help the algorithm choose best
        // split dimensions. Because it is an expensive operation, the frequency we recompute the bounds is given
        // by SPLITS_BEFORE_EXACT_BOUNDS.
        if (nodeID > 1 && config.numIndexDims > 2 && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
          points.computePackedValueBounds(config, minPackedValue, maxPackedValue);
        }
        splitDim = split(minPackedValue, maxPackedValue, parentSplits);
      }

      assert nodeID < splitPackedValues.length : "nodeID=" + nodeID + " splitValues.length=" + splitPackedValues.length;

      // How many points will be in the left tree:
      long rightCount = points.count / 2;
      long leftCount = points.count - rightCount;

      BKDRadixSelector.PathSlice[] slices = new BKDRadixSelector.PathSlice[2];

      int commonPrefixLen = Arrays.mismatch(minPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim, maxPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim);
      if (commonPrefixLen == -1) {
        commonPrefixLen = config.bytesPerDim;
      }

      byte[] splitValue = radixSelector.select(points, slices, points.start, points.start + points.count,  points.start + leftCount, splitDim, commonPrefixLen);

      int address = nodeID * (1 + config.bytesPerDim);
      splitPackedValues[address] = (byte) splitDim;
      System.arraycopy(splitValue, 0, splitPackedValues, address + 1, config.bytesPerDim);

      byte[] minSplitPackedValue = new byte[config.packedIndexBytesLength];
      System.arraycopy(minPackedValue, 0, minSplitPackedValue, 0, config.packedIndexBytesLength);

      byte[] maxSplitPackedValue = new byte[config.packedIndexBytesLength];
      System.arraycopy(maxPackedValue, 0, maxSplitPackedValue, 0, config.packedIndexBytesLength);

      System.arraycopy(splitValue, 0, minSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);
      System.arraycopy(splitValue, 0, maxSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);

      parentSplits[splitDim]++;
      // Recurse on left tree:
      build(2 * nodeID, leafNodeOffset, slices[0],
          out, radixSelector, minPackedValue, maxSplitPackedValue,
          parentSplits, splitPackedValues, leafBlockFPs, leafBlock);

      // Recurse on right tree:
      build(2 * nodeID + 1, leafNodeOffset, slices[1],
          out, radixSelector, minSplitPackedValue, maxPackedValue
          , parentSplits, splitPackedValues, leafBlockFPs, leafBlock);

      parentSplits[splitDim]--;
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

  private void checkMaxLeafNodeCount(int numLeaves) {
    if ((1 + config.bytesPerDim) * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + config.maxPointsInLeafNode + ") and reindex");
    }
  }

  private static class LeafBlock implements BKDLeafBlock {

    int count;
    int from;
    HeapPointWriter pointWriter;

    LeafBlock() {
    }

    void setRange(HeapPointWriter pointWriter, int from, int count) {
      this.count = count;
      this.from = from;
      this.pointWriter = pointWriter;
    }

    @Override
    public int count() {
      return count;
    }

    @Override
    public BytesRef packedValue(int position) {
      PointValue value = pointWriter.getPackedValueSlice(from + position);
      return value.packedValue();
    }

    @Override
    public int docId(int position) {
      return pointWriter.getPackedValueSlice(from + position).docID();
    }
  }
}
