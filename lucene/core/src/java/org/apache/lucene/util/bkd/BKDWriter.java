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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
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
import org.apache.lucene.util.PriorityQueue;

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
 *  partially balanced, which means the leaf nodes will have
 *  the requested <code>maxPointsInLeafNode</code> except one that might have less.
 *  Leaf nodes may straddle the two bottom levels of the binary tree.
 *  Values that fall exactly on a cell boundary may be in either cell.
 *
 *  <p>The number of dimensions can be 1 to 8, but every byte[] value is fixed length.
 *
 *  <p>This consumes heap during writing: it allocates a <code>Long[numLeaves]</code>,
 *  a <code>byte[numLeaves*(1+bytesPerDim)]</code> and then uses up to the specified
 *  {@code maxMBSortInHeap} heap space for writing.
 *
 *  <p>
 *  <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> / bytesPerDim
 *  total points.
 *
 * @lucene.experimental */

public class BKDWriter implements Closeable {

  /** Default maximum heap to use, before spilling to (slower) disk */
  public static final float DEFAULT_MAX_MB_SORT_IN_HEAP = 16.0f;

  /** Number of splits before we compute the exact bounding box of an inner node. */
  private static final int SPLITS_BEFORE_EXACT_BOUNDS = 4;

  /** dimensions length and size */
  protected final BKDConfig config;

  final TrackingDirectoryWrapper tempDir;
  final String tempFileNamePrefix;
  final double maxMBSortInHeap;

  final byte[] scratchDiff;
  final byte[] scratch1;

  final BytesRef scratchBytesRef1 = new BytesRef();
  final BytesRef scratchBytesRef2 = new BytesRef();
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

  private final int maxDoc;

  /** Contains the methods to serialize the tree into the index */
  private final BKDIndexWriter indexWriter;

  public BKDWriter(BKDConfig config, int maxDoc, Directory tempDir, String tempFileNamePrefix,
                   double maxMBSortInHeap, long totalPointCount) throws IOException {
    verifyParams(maxMBSortInHeap, totalPointCount);
    // We use tracking dir to deal with removing files on exception, so each place that
    // creates temp files doesn't need crazy try/finally/sucess logic:
    this.tempDir = new TrackingDirectoryWrapper(tempDir);
    this.tempFileNamePrefix = tempFileNamePrefix;
    this.config = config;
    this.totalPointCount = totalPointCount;
    this.maxDoc = maxDoc;
    docsSeen = new FixedBitSet(maxDoc);

    scratchDiff = new byte[config.bytesPerDim];
    scratch1 = new byte[config.packedBytesLength];
    commonPrefixLengths = new int[config.numDims];

    minPackedValue = new byte[config.packedIndexBytesLength];
    maxPackedValue = new byte[config.packedIndexBytesLength];

    // Maximum number of points we hold in memory at any time
    maxPointsSortInHeap = (int) ((maxMBSortInHeap * 1024 * 1024) / (config.bytesPerDoc));

    // Finally, we must be able to hold at least the leaf node in heap during build:
    if (maxPointsSortInHeap < config.maxPointsInLeafNode) {
      throw new IllegalArgumentException("maxMBSortInHeap=" + maxMBSortInHeap + " only allows for maxPointsSortInHeap=" + maxPointsSortInHeap + ", but this is less than maxPointsInLeafNode=" + config.maxPointsInLeafNode + "; either increase maxMBSortInHeap or decrease maxPointsInLeafNode");
    }

    this.maxMBSortInHeap = maxMBSortInHeap;

    indexWriter = new BKDIndexWriter(config);
  }

  public static void verifyParams(double maxMBSortInHeap, long totalPointCount) {
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
    if (pointCount == 0) {
      initPointWriter();
      System.arraycopy(packedValue, 0, minPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(packedValue, 0, maxPackedValue, 0, config.packedIndexBytesLength);
    } else {
      for(int dim=0; dim < config.numIndexDims; dim++) {
        int offset = dim * config.bytesPerDim;
        if (Arrays.compareUnsigned(packedValue, offset, offset + config.bytesPerDim, minPackedValue, offset, offset + config.bytesPerDim) < 0) {
          System.arraycopy(packedValue, offset, minPackedValue, offset, config.bytesPerDim);
        } else if (Arrays.compareUnsigned(packedValue, offset, offset + config.bytesPerDim, maxPackedValue, offset, offset + config.bytesPerDim) > 0) {
          System.arraycopy(packedValue, offset, maxPackedValue, offset, config.bytesPerDim);
        }
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

  private static class MergeReader {
    final BKDReader bkd;
    final BKDReader.IntersectState state;
    final MergeState.DocMap docMap;

    /** Current doc ID */
    public int docID;

    /** Which doc in this block we are up to */
    private int docBlockUpto;

    /** How many docs in the current block */
    private int docsInBlock;

    /** Which leaf block we are up to */
    private int blockID;

    private final byte[] packedValues;

    public MergeReader(BKDReader bkd, MergeState.DocMap docMap) throws IOException {
      this.bkd = bkd;
      state = new BKDReader.IntersectState(bkd.in.clone(),
          bkd.numDataDims,
          bkd.packedBytesLength,
          bkd.packedIndexBytesLength,
          bkd.maxPointsInLeafNode,
          null,
          null);
      this.docMap = docMap;
      state.in.seek(bkd.getMinLeafBlockFP());
      this.packedValues = new byte[bkd.maxPointsInLeafNode * bkd.packedBytesLength];
    }

    public boolean next() throws IOException {
      //System.out.println("MR.next this=" + this);
      while (true) {
        if (docBlockUpto == docsInBlock) {
          if (blockID == bkd.leafNodeOffset) {
            //System.out.println("  done!");
            return false;
          }
          //System.out.println("  new block @ fp=" + state.in.getFilePointer());
          docsInBlock = bkd.readDocIDs(state.in, state.in.getFilePointer(), state.scratchIterator);
          assert docsInBlock > 0;
          docBlockUpto = 0;
          bkd.visitDocValues(state.commonPrefixLengths, state.scratchDataPackedValue, state.scratchMinIndexPackedValue, state.scratchMaxIndexPackedValue, state.in, state.scratchIterator, docsInBlock, new IntersectVisitor() {
            int i = 0;

            @Override
            public void visit(int docID) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              assert docID == state.scratchIterator.docIDs[i];
              System.arraycopy(packedValue, 0, packedValues, i * bkd.packedBytesLength, bkd.packedBytesLength);
              i++;
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              return Relation.CELL_CROSSES_QUERY;
            }

          });

          blockID++;
        }

        final int index = docBlockUpto++;
        int oldDocID = state.scratchIterator.docIDs[index];

        int mappedDocID;
        if (docMap == null) {
          mappedDocID = oldDocID;
        } else {
          mappedDocID = docMap.get(oldDocID);
        }

        if (mappedDocID != -1) {
          // Not deleted!
          docID = mappedDocID;
          System.arraycopy(packedValues, index * bkd.packedBytesLength, state.scratchDataPackedValue, 0, bkd.packedBytesLength);
          return true;
        }
      }
    }
  }

  private static class BKDMergeQueue extends PriorityQueue<MergeReader> {
    private final int bytesPerDim;

    public BKDMergeQueue(int bytesPerDim, int maxSize) {
      super(maxSize);
      this.bytesPerDim = bytesPerDim;
    }

    @Override
    public boolean lessThan(MergeReader a, MergeReader b) {
      assert a != b;

      int cmp = Arrays.compareUnsigned(a.state.scratchDataPackedValue, 0, bytesPerDim, b.state.scratchDataPackedValue, 0, bytesPerDim);
      if (cmp < 0) {
        return true;
      } else if (cmp > 0) {
        return false;
      }

      // Tie break by sorting smaller docIDs earlier:
      return a.docID < b.docID;
    }
  }

  /** flat representation of a kd-tree */
  interface BKDTreeLeafNodes {
    /** number of leaf nodes */
    int numLeaves();
    /** pointer to the leaf node previously written. Leaves are order from
     * left to right, so leaf at {@code index} 0 is the leftmost leaf and
     * the the leaf at {@code numleaves()} -1 is the rightmost leaf */
    long getLeafLP(int index);
    /** split value between two leaves. The split value at position n corresponds to the
     *  leaves at (n -1) and n. */
    BytesRef getSplitValue(int index);
    /** split dimension between two leaves. The split dimension at position n corresponds to the
     *  leaves at (n -1) and n.*/
    int getSplitDimension(int index);
  }

  /** Write a field from a {@link MutablePointValues}. This way of writing
   *  points is faster than regular writes with {@link BKDWriter#add} since
   *  there is opportunity for reordering points before writing them to
   *  disk. This method does not use transient disk in order to reorder points.
   */
  public long writeField(IndexOutput out, String fieldName, MutablePointValues reader) throws IOException {
    if (config.numDims == 1) {
      return writeField1Dim(out, fieldName, reader);
    } else {
      return writeFieldNDims(out, fieldName, reader);
    }
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

  /* In the 2+D case, we recursively pick the split dimension, compute the
   * median value and partition other values around it. */
  private long writeFieldNDims(IndexOutput out, String fieldName, MutablePointValues values) throws IOException {
    if (pointCount != 0) {
      throw new IllegalStateException("cannot mix add and writeField");
    }

    // Catch user silliness:
    if (finished == true) {
      throw new IllegalStateException("already finished");
    }

    // Mark that we already finished:
    finished = true;

    pointCount = values.size();

    final int numLeaves = Math.toIntExact((pointCount + config.maxPointsInLeafNode - 1) / config.maxPointsInLeafNode);
    final int numSplits = numLeaves - 1;

    checkMaxLeafNodeCount(numLeaves);

    final byte[] splitPackedValues = new byte[numSplits * config.bytesPerDim];
    final byte[] splitDimensionValues = new byte[numSplits];
    final long[] leafBlockFPs = new long[numLeaves];

    // compute the min/max for this slice
    computePackedValueBounds(values, 0, Math.toIntExact(pointCount), minPackedValue, maxPackedValue, scratchBytesRef1);
    for (int i = 0; i < Math.toIntExact(pointCount); ++i) {
      docsSeen.set(values.getDocID(i));
    }

    final int[] parentSplits = new int[config.numIndexDims];
    build(0, numLeaves, values, 0, Math.toIntExact(pointCount), out,
        minPackedValue.clone(), maxPackedValue.clone(), parentSplits,
        splitPackedValues, splitDimensionValues, leafBlockFPs,
        new int[config.maxPointsInLeafNode]);
    assert Arrays.equals(parentSplits, new int[config.numIndexDims]);

    scratchBytesRef1.length = config.bytesPerDim;
    scratchBytesRef1.bytes = splitPackedValues;

    BKDTreeLeafNodes leafNodes  = new BKDTreeLeafNodes() {
      @Override
      public long getLeafLP(int index) {
        return leafBlockFPs[index];
      }

      @Override
      public BytesRef getSplitValue(int index) {
        scratchBytesRef1.offset = index * config.bytesPerDim;
        return scratchBytesRef1;
      }

      @Override
      public int getSplitDimension(int index) {
        return splitDimensionValues[index] & 0xff;
      }

      @Override
      public int numLeaves() {
        return leafBlockFPs.length;
      }
    };

    long indexFP = out.getFilePointer();
    indexWriter.writeIndex(out, config.maxPointsInLeafNode, leafNodes, minPackedValue, maxPackedValue, pointCount, docsSeen.cardinality());
    return indexFP;
  }

  /* In the 1D case, we can simply sort points in ascending order and use the
   * same writing logic as we use at merge time. */
  private long writeField1Dim(IndexOutput out, String fieldName, MutablePointValues reader) throws IOException {
    MutablePointsReaderUtils.sort(config, maxDoc, reader, 0, Math.toIntExact(reader.size()));

    final OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(out);

    reader.intersect(new IntersectVisitor() {

      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {
        oneDimWriter.add(packedValue, docID);
      }

      @Override
      public void visit(int docID) throws IOException {
        throw new IllegalStateException();
      }

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return Relation.CELL_CROSSES_QUERY;
      }
    });

    return oneDimWriter.finish();
  }

  /** More efficient bulk-add for incoming {@link BKDReader}s.  This does a merge sort of the already
   *  sorted values and currently only works when numDims==1.  This returns -1 if all documents containing
   *  dimensional values were deleted. */
  public long merge(IndexOutput out, List<MergeState.DocMap> docMaps, List<BKDReader> readers) throws IOException {
    assert docMaps == null || readers.size() == docMaps.size();

    BKDMergeQueue queue = new BKDMergeQueue(config.bytesPerDim, readers.size());

    for(int i=0;i<readers.size();i++) {
      BKDReader bkd = readers.get(i);
      MergeState.DocMap docMap;
      if (docMaps == null) {
        docMap = null;
      } else {
        docMap = docMaps.get(i);
      }
      MergeReader reader = new MergeReader(bkd, docMap);
      if (reader.next()) {
        queue.add(reader);
      }
    }

    OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(out);

    while (queue.size() != 0) {
      MergeReader reader = queue.top();
      // System.out.println("iter reader=" + reader);

      oneDimWriter.add(reader.state.scratchDataPackedValue, reader.docID);

      if (reader.next()) {
        queue.updateTop();
      } else {
        // This segment was exhausted
        queue.pop();
      }
    }

    return oneDimWriter.finish();
  }

  private class OneDimensionBKDWriter {

    final IndexOutput out;
    final List<Long> leafBlockFPs = new ArrayList<>();
    final List<byte[]> leafBlockStartValues = new ArrayList<>();
    final byte[] leafValues = new byte[config.maxPointsInLeafNode * config.packedBytesLength];
    final int[] leafDocs = new int[config.maxPointsInLeafNode];
    private long valueCount;
    private int leafCount;
    private int leafCardinality;

    OneDimensionBKDWriter(IndexOutput out) {
      if (config.numIndexDims != 1) {
        throw new UnsupportedOperationException("numIndexDims must be 1 but got " + config.numIndexDims);
      }
      if (pointCount != 0) {
        throw new IllegalStateException("cannot mix add and merge");
      }

      // Catch user silliness:
      if (finished == true) {
        throw new IllegalStateException("already finished");
      }

      // Mark that we already finished:
      finished = true;

      this.out = out;

      lastPackedValue = new byte[config.packedBytesLength];
    }

    // for asserts
    final byte[] lastPackedValue;
    private int lastDocID;

    void add(byte[] packedValue, int docID) throws IOException {
      assert valueInOrder(valueCount + leafCount,
          0, lastPackedValue, packedValue, 0, docID, lastDocID);

      if (leafCount == 0 || Arrays.mismatch(leafValues, (leafCount - 1) * config.bytesPerDim, leafCount * config.bytesPerDim, packedValue, 0, config.bytesPerDim) != -1) {
        leafCardinality++;
      }
      System.arraycopy(packedValue, 0, leafValues, leafCount * config.packedBytesLength, config.packedBytesLength);
      leafDocs[leafCount] = docID;
      docsSeen.set(docID);
      leafCount++;

      if (valueCount + leafCount > totalPointCount) {
        throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + (valueCount + leafCount) + " values");
      }

      if (leafCount == config.maxPointsInLeafNode) {
        // We write a block once we hit exactly the max count ... this is different from
        // when we write N > 1 dimensional points where we write between max/2 and max per leaf block
        writeLeafBlock(leafCardinality);
        leafCardinality = 0;
        leafCount = 0;
      }

      assert (lastDocID = docID) >= 0; // only assign when asserts are enabled
    }

    public long finish() throws IOException {
      if (leafCount > 0) {
        writeLeafBlock(leafCardinality);
        leafCardinality = 0;
        leafCount = 0;
      }

      if (valueCount == 0) {
        return -1;
      }

      pointCount = valueCount;

      long indexFP = out.getFilePointer();

      scratchBytesRef1.length = config.bytesPerDim;
      scratchBytesRef1.offset = 0;
      assert leafBlockStartValues.size() + 1 == leafBlockFPs.size();
      BKDTreeLeafNodes leafNodes = new BKDTreeLeafNodes() {
        @Override
        public long getLeafLP(int index) {
          return leafBlockFPs.get(index);
        }

        @Override
        public BytesRef getSplitValue(int index) {
          scratchBytesRef1.bytes = leafBlockStartValues.get(index);
          return scratchBytesRef1;
        }

        @Override
        public int getSplitDimension(int index) {
          return 0;
        }

        @Override
        public int numLeaves() {
          return leafBlockFPs.size();
        }
      };
      indexWriter.writeIndex(out, config.maxPointsInLeafNode, leafNodes, minPackedValue, maxPackedValue, pointCount, docsSeen.cardinality());
      return indexFP;
    }

    private void writeLeafBlock(int leafCardinality) throws IOException {
      assert leafCount != 0;
      if (valueCount == 0) {
        System.arraycopy(leafValues, 0, minPackedValue, 0, config.packedIndexBytesLength);
      }
      System.arraycopy(leafValues, (leafCount - 1) * config.packedBytesLength, maxPackedValue, 0, config.packedIndexBytesLength);

      valueCount += leafCount;

      if (leafBlockFPs.size() > 0) {
        // Save the first (minimum) value in each leaf block except the first, to build the split value index in the end:
        leafBlockStartValues.add(ArrayUtil.copyOfSubArray(leafValues, 0, config.packedBytesLength));
      }
      leafBlockFPs.add(out.getFilePointer());
      checkMaxLeafNodeCount(leafBlockFPs.size());

      // Find per-dim common prefix:
      int offset = (leafCount - 1) * config.packedBytesLength;
      int prefix = Arrays.mismatch(leafValues, 0, config.bytesPerDim, leafValues, offset, offset + config.bytesPerDim);
      if (prefix == -1) {
        prefix = config.bytesPerDim;
      }

      commonPrefixLengths[0] = prefix;

      scratchBytesRef1.length = config.packedBytesLength;
      scratchBytesRef1.bytes = leafValues;

      final IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        @Override
        public BytesRef apply(int i) {
          scratchBytesRef1.offset = config.packedBytesLength * i;
          return scratchBytesRef1;
        }
      };
      assert valuesInOrderAndBounds(leafCount, 0, ArrayUtil.copyOfSubArray(leafValues, 0, config.packedBytesLength),
          ArrayUtil.copyOfSubArray(leafValues, (leafCount - 1) * config.packedBytesLength, leafCount * config.packedBytesLength),
          packedValues, leafDocs, 0);

      indexWriter.writeLeafBlock(out, leafDocs, 0, leafCount,
          commonPrefixLengths, leafValues,
          0, packedValues, leafCardinality);
    }
  }

  static int getNumLeftLeafNodes(int numLeaves) {
    assert numLeaves > 1: "getNumLeftLeaveNodes() called with " + numLeaves;
    // return the level that can be filled with this number of leaves
    int lastFullLevel = 31 - Integer.numberOfLeadingZeros(numLeaves);
    // how many leaf nodes are in the full level
    int leavesFullLevel = 1 << lastFullLevel;
    // half of the leaf nodes from the full level goes to the left
    int numLeftLeafNodes = leavesFullLevel / 2;
    // leaf nodes that do not fit in the full level
    int unbalancedLeafNodes = numLeaves - leavesFullLevel;
    // distribute unbalanced leaf nodes
    numLeftLeafNodes += Math.min(unbalancedLeafNodes, numLeftLeafNodes);
    // we should always place unbalanced leaf nodes on the left
    assert numLeftLeafNodes >= numLeaves - numLeftLeafNodes && numLeftLeafNodes <= 2L * (numLeaves - numLeftLeafNodes);
    return numLeftLeafNodes;
  }

  // TODO: if we fixed each partition step to just record the file offset at the "split point", we could probably handle variable length
  // encoding and not have our own ByteSequencesReader/Writer

  // useful for debugging:
  /*
  private void printPathSlice(String desc, PathSlice slice, int dim) throws IOException {
    System.out.println("    " + desc + " dim=" + dim + " count=" + slice.count + ":");
    try(PointReader r = slice.writer.getReader(slice.start, slice.count)) {
      int count = 0;
      while (r.next()) {
        byte[] v = r.packedValue();
        System.out.println("      " + count + ": " + new BytesRef(v, dim*bytesPerDim, bytesPerDim));
        count++;
        if (count == slice.count) {
          break;
        }
      }
    }
  }
  */

  private void checkMaxLeafNodeCount(int numLeaves) {
    if (config.bytesPerDim * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + config.maxPointsInLeafNode + ") and reindex");
    }
  }

  /** Writes the BKD tree to the provided {@link IndexOutput} and returns the file offset where index was written. */
  public long finish(IndexOutput out) throws IOException {
    // System.out.println("\nBKDTreeWriter.finish pointCount=" + pointCount + " out=" + out + " heapWriter=" + heapPointWriter);

    // TODO: specialize the 1D case?  it's much faster at indexing time (no partitioning on recurse...)

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

    final int numLeaves = Math.toIntExact((pointCount + config.maxPointsInLeafNode - 1) / config.maxPointsInLeafNode);
    final int numSplits = numLeaves - 1;

    checkMaxLeafNodeCount(numLeaves);

    // NOTE: we could save the 1+ here, to use a bit less heap at search time, but then we'd need a somewhat costly check at each
    // step of the recursion to recompute the split dim:

    // Indexed by nodeID, but first (root) nodeID is 1.  We do 1+ because the lead byte at each recursion says which dim we split on.
    byte[] splitPackedValues = new byte[Math.toIntExact(numSplits*config.bytesPerDim)];
    byte[] splitDimensionValues = new byte[numSplits];

    // +1 because leaf count is power of 2 (e.g. 8), and innerNodeCount is power of 2 minus 1 (e.g. 7)
    long[] leafBlockFPs = new long[numLeaves];

    // Make sure the math above "worked":
    assert pointCount / numLeaves <= config.maxPointsInLeafNode: "pointCount=" + pointCount + " numLeaves=" + numLeaves + " maxPointsInLeafNode=" + config.maxPointsInLeafNode;

    //We re-use the selector so we do not need to create an object every time.
    BKDRadixSelector radixSelector = new BKDRadixSelector(config, maxPointsSortInHeap, tempDir, tempFileNamePrefix);

    boolean success = false;
    try {

      final int[] parentSplits = new int[config.numIndexDims];
      build(0, numLeaves, points,
          out, radixSelector,
          minPackedValue.clone(), maxPackedValue.clone(),
          parentSplits,
          splitPackedValues,
          splitDimensionValues,
          leafBlockFPs,
          new int[config.maxPointsInLeafNode]);
      assert Arrays.equals(parentSplits, new int[config.numIndexDims]);

      // If no exception, we should have cleaned everything up:
      assert tempDir.getCreatedFiles().isEmpty();
      //long t2 = System.nanoTime();
      //System.out.println("write time: " + ((t2-t1)/1000000.0) + " msec");

      success = true;
    } finally {
      if (success == false) {
        IOUtils.deleteFilesIgnoringExceptions(tempDir, tempDir.getCreatedFiles());
      }
    }

    scratchBytesRef1.bytes = splitPackedValues;
    scratchBytesRef1.length = config.bytesPerDim;
    BKDTreeLeafNodes leafNodes  = new BKDTreeLeafNodes() {
      @Override
      public long getLeafLP(int index) {
        return leafBlockFPs[index];
      }

      @Override
      public BytesRef getSplitValue(int index) {
        scratchBytesRef1.offset = index * config.bytesPerDim;
        return scratchBytesRef1;
      }

      @Override
      public int getSplitDimension(int index) {
        return splitDimensionValues[index] & 0xff;
      }

      @Override
      public int numLeaves() {
        return leafBlockFPs.length;
      }
    };

    // Write index:
    long indexFP = out.getFilePointer();
    indexWriter.writeIndex(out, config.maxPointsInLeafNode, leafNodes, minPackedValue, maxPackedValue, pointCount, docsSeen.cardinality());
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

  /** Called only in assert */
  private boolean valueInBounds(BytesRef packedValue, byte[] minPackedValue, byte[] maxPackedValue) {
    for(int dim = 0; dim < config.numIndexDims; dim++) {
      int offset = config.bytesPerDim * dim;
      if (Arrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + config.bytesPerDim, minPackedValue, offset, offset + config.bytesPerDim) < 0) {
        return false;
      }
      if (Arrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + config.bytesPerDim, maxPackedValue, offset, offset + config.bytesPerDim) > 0) {
        return false;
      }
    }

    return true;
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

  /* Recursively reorders the provided reader and writes the bkd-tree on the fly; this method is used
   * when we are writing a new segment directly from IndexWriter's indexing buffer (MutablePointsReader). */
  private void build(int leavesOffset, int numLeaves,
                     MutablePointValues reader, int from, int to,
                     IndexOutput out,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     int[] parentSplits,
                     byte[] splitPackedValues,
                     byte[] splitDimensionValues,
                     long[] leafBlockFPs,
                     int[] spareDocIds) throws IOException {

    if (numLeaves == 1) {
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
      leafBlockFPs[leavesOffset] = out.getFilePointer();

      // Collect doc IDs
      int[] docIDs = spareDocIds;
      for (int i = from; i < to; ++i) {
        docIDs[i - from] = reader.getDocID(i);
      }

      // Collect the common prefixes:
      reader.getValue(from, scratchBytesRef1);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset, scratch1, 0, config.packedBytesLength);

      // Collect the full values:
      IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        @Override
        public BytesRef apply(int i) {
          reader.getValue(from + i, scratchBytesRef1);
          return scratchBytesRef1;
        }
      };
      assert valuesInOrderAndBounds(count, sortedDim, minPackedValue, maxPackedValue, packedValues,
          docIDs, 0);
      indexWriter.writeLeafBlock(out, docIDs, 0, count,
          commonPrefixLengths, scratch1,
          sortedDim, packedValues, leafCardinality);
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
        if (numLeaves != leafBlockFPs.length && config.numIndexDims > 2 && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
          computePackedValueBounds(reader, from, to, minPackedValue, maxPackedValue, scratchBytesRef1);
        }
        splitDim = split(minPackedValue, maxPackedValue, parentSplits);
      }

      // How many leaves will be in the left tree:
      int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
      // How many points will be in the left tree:
      final int mid = from + numLeftLeafNodes * config.maxPointsInLeafNode;

      int commonPrefixLen = Arrays.mismatch(minPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim, maxPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim);
      if (commonPrefixLen == -1) {
        commonPrefixLen = config.bytesPerDim;
      }

      MutablePointsReaderUtils.partition(config, maxDoc, splitDim, commonPrefixLen,
          reader, from, to, mid, scratchBytesRef1, scratchBytesRef2);

      // set the split value
      final int rightOffset = leavesOffset + numLeftLeafNodes;
      final int splitOffset = rightOffset - 1;
      // set the split value
      final int address = splitOffset * config.bytesPerDim;
      splitDimensionValues[splitOffset] = (byte) splitDim;
      reader.getValue(mid, scratchBytesRef1);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * config.bytesPerDim, splitPackedValues, address, config.bytesPerDim);

      byte[] minSplitPackedValue = ArrayUtil.copyOfSubArray(minPackedValue, 0, config.packedIndexBytesLength);
      byte[] maxSplitPackedValue = ArrayUtil.copyOfSubArray(maxPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * config.bytesPerDim,
          minSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * config.bytesPerDim,
          maxSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);

      // recurse
      parentSplits[splitDim]++;
      build(leavesOffset, numLeftLeafNodes, reader, from, mid, out,
          minPackedValue, maxSplitPackedValue, parentSplits,
          splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);
      build(rightOffset, numLeaves - numLeftLeafNodes, reader, mid, to, out,
          minSplitPackedValue, maxPackedValue, parentSplits,
          splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);
      parentSplits[splitDim]--;
    }
  }

  private void computePackedValueBounds(BKDRadixSelector.PathSlice slice, byte[] minPackedValue, byte[] maxPackedValue) throws IOException {
    try (PointReader reader = slice.writer.getReader(slice.start, slice.count)) {
      if (reader.next() == false) {
        return;
      }
      BytesRef value = reader.pointValue().packedValue();
      System.arraycopy(value.bytes, value.offset, minPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(value.bytes, value.offset, maxPackedValue, 0, config.packedIndexBytesLength);
      while (reader.next()) {
        value = reader.pointValue().packedValue();
        for(int dim = 0; dim < config.numIndexDims; dim++) {
          final int startOffset = dim * config.bytesPerDim;
          final int endOffset = startOffset + config.bytesPerDim;
          if (Arrays.compareUnsigned(value.bytes, value.offset + startOffset, value.offset + endOffset, minPackedValue, startOffset, endOffset) < 0) {
            System.arraycopy(value.bytes, value.offset + startOffset, minPackedValue, startOffset, config.bytesPerDim);
          } else if (Arrays.compareUnsigned(value.bytes, value.offset + startOffset, value.offset + endOffset, maxPackedValue, startOffset, endOffset) > 0) {
            System.arraycopy(value.bytes, value.offset + startOffset, maxPackedValue, startOffset, config.bytesPerDim);
          }
        }
      }
    }
  }

  /** The point writer contains the data that is going to be splitted using radix selection.
   /*  This method is used when we are merging previously written segments, in the numDims > 1 case. */
  private void build(int leavesOffset, int numLeaves,
                     BKDRadixSelector.PathSlice points,
                     IndexOutput out,
                     BKDRadixSelector radixSelector,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     int[] parentSplits,
                     byte[] splitPackedValues,
                     byte[] splitDimensionValues,
                     long[] leafBlockFPs,
                     int[] spareDocIds) throws IOException {

    if (numLeaves == 1) {

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

      int from = Math.toIntExact(points.start);
      int to = Math.toIntExact(points.start + points.count);
      //we store common prefix on scratch1
      computeCommonPrefixLength(heapSource, scratch1, from, to);

      int sortedDim = 0;
      int sortedDimCardinality = Integer.MAX_VALUE;
      FixedBitSet[] usedBytes = new FixedBitSet[config.numDims];
      for (int dim = 0; dim < config.numDims; ++dim) {
        if (commonPrefixLengths[dim] < config.bytesPerDim) {
          usedBytes[dim] = new FixedBitSet(256);
        }
      }
      //Find the dimension to compress
      for (int dim = 0; dim < config.numDims; dim++) {
        int prefix = commonPrefixLengths[dim];
        if (prefix < config.bytesPerDim) {
          int offset = dim * config.bytesPerDim;
          for (int i = from; i < to; ++i) {
            PointValue value = heapSource.getPackedValueSlice(i);
            BytesRef packedValue = value.packedValue();
            int bucket = packedValue.bytes[packedValue.offset + offset + prefix] & 0xff;
            usedBytes[dim].set(bucket);
          }
          int cardinality =usedBytes[dim].cardinality();
          if (cardinality < sortedDimCardinality) {
            sortedDim = dim;
            sortedDimCardinality = cardinality;
          }
        }
      }

      // sort the chosen dimension
      radixSelector.heapRadixSort(heapSource, from, to, sortedDim, commonPrefixLengths[sortedDim]);
      // compute cardinality
      int leafCardinality = heapSource.computeCardinality(from ,to, config.numDims, config.bytesPerDim, commonPrefixLengths);

      // Save the block file pointer:
      leafBlockFPs[leavesOffset] = out.getFilePointer();

      // Collect doc IDs
      int count = to - from;
      assert count > 0: "numLeaves=" + numLeaves + " leavesOffset=" + leavesOffset;
      assert count <= spareDocIds.length : "count=" + count + " > length=" + spareDocIds.length;
      int[] docIDs = spareDocIds;
      for (int i = 0; i < count; i++) {
        docIDs[i] = heapSource.getPackedValueSlice(from + i).docID();
      }

      // collect the full values:
      IntFunction<BytesRef> packedValues = new IntFunction<>() {
        final BytesRef scratch = new BytesRef();

        {
          scratch.length = config.packedBytesLength;
        }

        @Override
        public BytesRef apply(int i) {
          PointValue value = heapSource.getPackedValueSlice(from + i);
          return value.packedValue();
        }
      };
      assert valuesInOrderAndBounds(count, sortedDim, minPackedValue, maxPackedValue, packedValues,
          docIDs, 0);
      indexWriter.writeLeafBlock(out, docIDs, 0, count,
          commonPrefixLengths, scratch1,
          sortedDim, packedValues, leafCardinality);

    } else {
      // Inner node: partition/recurse

      final int splitDim;
      if (config.numIndexDims == 1) {
        splitDim = 0;
      } else {
        // for dimensions > 2 we recompute the bounds for the current inner node to help the algorithm choose best
        // split dimensions. Because it is an expensive operation, the frequency we recompute the bounds is given
        // by SPLITS_BEFORE_EXACT_BOUNDS.
        if (numLeaves != leafBlockFPs.length && config.numIndexDims > 2 && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
          computePackedValueBounds(points, minPackedValue, maxPackedValue);
        }
        splitDim = split(minPackedValue, maxPackedValue, parentSplits);
      }

      assert numLeaves <= leafBlockFPs.length : "numLeaves=" + numLeaves + " leafBlockFPs.length=" + leafBlockFPs.length;

      // How many leaves will be in the left tree:
      final int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
      // How many points will be in the left tree:
      final long leftCount = numLeftLeafNodes * config.maxPointsInLeafNode;

      BKDRadixSelector.PathSlice[] slices = new BKDRadixSelector.PathSlice[2];

      int commonPrefixLen = Arrays.mismatch(minPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim, maxPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim);
      if (commonPrefixLen == -1) {
        commonPrefixLen = config.bytesPerDim;
      }

      byte[] splitValue = radixSelector.select(points, slices, points.start, points.start + points.count,  points.start + leftCount, splitDim, commonPrefixLen);

      final int rightOffset = leavesOffset + numLeftLeafNodes;
      final int splitValueOffset = rightOffset - 1;

      splitDimensionValues[splitValueOffset] = (byte) splitDim;
      int address = splitValueOffset * config.bytesPerDim;
      System.arraycopy(splitValue, 0, splitPackedValues, address, config.bytesPerDim);

      byte[] minSplitPackedValue = new byte[config.packedIndexBytesLength];
      System.arraycopy(minPackedValue, 0, minSplitPackedValue, 0, config.packedIndexBytesLength);

      byte[] maxSplitPackedValue = new byte[config.packedIndexBytesLength];
      System.arraycopy(maxPackedValue, 0, maxSplitPackedValue, 0, config.packedIndexBytesLength);

      System.arraycopy(splitValue, 0, minSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);
      System.arraycopy(splitValue, 0, maxSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);

      parentSplits[splitDim]++;
      // Recurse on left tree:
      build(leavesOffset, numLeftLeafNodes, slices[0],
          out, radixSelector, minPackedValue, maxSplitPackedValue,
          parentSplits, splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);

      // Recurse on right tree:
      build(rightOffset, numLeaves - numLeftLeafNodes, slices[1],
          out, radixSelector, minSplitPackedValue, maxPackedValue,
          parentSplits, splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);

      parentSplits[splitDim]--;
    }
  }

  private void computeCommonPrefixLength(HeapPointWriter heapPointWriter, byte[] commonPrefix, int from, int to) {
    Arrays.fill(commonPrefixLengths, config.bytesPerDim);
    PointValue value = heapPointWriter.getPackedValueSlice(from);
    BytesRef packedValue = value.packedValue();
    for (int dim = 0; dim < config.numDims; dim++) {
      System.arraycopy(packedValue.bytes, packedValue.offset + dim * config.bytesPerDim, commonPrefix, dim * config.bytesPerDim, config.bytesPerDim);
    }
    for (int i = from + 1; i < to; i++) {
      value =  heapPointWriter.getPackedValueSlice(i);
      packedValue = value.packedValue();
      for (int dim = 0; dim < config.numDims; dim++) {
        if (commonPrefixLengths[dim] != 0) {
          int j = Arrays.mismatch(commonPrefix, dim * config.bytesPerDim, dim * config.bytesPerDim + commonPrefixLengths[dim], packedValue.bytes, packedValue.offset + dim * config.bytesPerDim, packedValue.offset + dim * config.bytesPerDim + commonPrefixLengths[dim]);
          if (j != -1) {
            commonPrefixLengths[dim] = j;
          }
        }
      }
    }
  }

  // only called from assert
  private boolean valuesInOrderAndBounds(int count, int sortedDim, byte[] minPackedValue, byte[] maxPackedValue,
                                         IntFunction<BytesRef> values, int[] docs, int docsOffset) throws IOException {
    byte[] lastPackedValue = new byte[config.packedBytesLength];
    int lastDoc = -1;
    for (int i = 0; i < count; i++) {
      BytesRef packedValue = values.apply(i);
      assert packedValue.length == config.packedBytesLength;
      assert valueInOrder(i, sortedDim, lastPackedValue, packedValue.bytes, packedValue.offset,
          docs[docsOffset + i], lastDoc);
      lastDoc = docs[docsOffset + i];

      // Make sure this value does in fact fall within this leaf cell:
      assert valueInBounds(packedValue, minPackedValue, maxPackedValue);
    }
    return true;
  }

  // only called from assert
  private boolean valueInOrder(long ord, int sortedDim, byte[] lastPackedValue, byte[] packedValue, int packedValueOffset,
                               int doc, int lastDoc) {
    int dimOffset = sortedDim * config.bytesPerDim;
    if (ord > 0) {
      int cmp = Arrays.compareUnsigned(lastPackedValue, dimOffset, dimOffset + config.bytesPerDim, packedValue, packedValueOffset + dimOffset, packedValueOffset + dimOffset + config.bytesPerDim);
      if (cmp > 0) {
        throw new AssertionError("values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, config.packedBytesLength) + " ord=" + ord);
      }
      if (cmp == 0  && config.numDims > config.numIndexDims) {
        int dataOffset = config.numIndexDims * config.bytesPerDim;
        cmp = Arrays.compareUnsigned(lastPackedValue, dataOffset, config.packedBytesLength, packedValue, packedValueOffset + dataOffset, packedValueOffset + config.packedBytesLength);
        if (cmp > 0) {
          throw new AssertionError("data values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, config.packedBytesLength) + " ord=" + ord);
        }
      }
      if (cmp == 0 && doc < lastDoc) {
        throw new AssertionError("docs out of order: last doc=" + lastDoc + " current doc=" + doc + " ord=" + ord);
      }
    }
    System.arraycopy(packedValue, packedValueOffset, lastPackedValue, 0, config.packedBytesLength);
    return true;
  }
}