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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PriorityQueue;

public class OneDimensionBKDWriter {

  final BKDIndexWriter out;
  final List<Long> leafBlockFPs = new ArrayList<>();
  final List<byte[]> leafBlockStartValues = new ArrayList<>();
  private final BKDConfig config;
  final byte[] leafValues;
  final int[] leafDocs;
  private long valueCount;
  private int leafCount;
  private int leafCardinality;

  protected long pointCount;

  /** An upper bound on how many points the caller will add (includes deletions) */
  private final long totalPointCount;

  final byte[] scratch;

  final int[] commonPrefixLengths;

  final BytesRef scratchBytesRef1 = new BytesRef();

  protected final FixedBitSet docsSeen;

  /** Minimum per-dim values, packed */
  protected final byte[] minPackedValue;

  /** Maximum per-dim values, packed */
  protected final byte[] maxPackedValue;

  private OneDimensionBKDWriter(BKDConfig config, BKDIndexWriter out, int maxDoc, long totalPointCount) {
    if (config.numIndexDims != 1) {
      throw new UnsupportedOperationException("numIndexDims must be 1 but got " + config.numIndexDims);
    }
    if (totalPointCount < 0) {
      throw new IllegalArgumentException("totalPointCount must be >=0 (got: " + totalPointCount + ")");
    }
    scratch = new byte[config.packedBytesLength];
    this.totalPointCount = totalPointCount;
    this.docsSeen = new FixedBitSet(maxDoc);
    commonPrefixLengths = new int[config.numDataDims];
    minPackedValue = new byte[config.packedIndexBytesLength];
    maxPackedValue = new byte[config.packedIndexBytesLength];
    this.out = out;
    leafDocs = new int[config.maxPointsInLeafNode];
    leafValues = new byte[config.maxPointsInLeafNode * config.packedBytesLength];
    lastPackedValue = new byte[config.packedBytesLength];
    this.config = config;
  }

  // for asserts
  final byte[] lastPackedValue;
  private int lastDocID;

  void add(byte[] packedValue, int docID) throws IOException {
    assert BKDLeafBlock.valueInOrder(config, valueCount + leafCount,
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

    int numInnerNodes = leafBlockStartValues.size();

    byte[] index = new byte[(1 + numInnerNodes) * (1 + config.bytesPerDim)];
    rotateToTree(1, 0, numInnerNodes, index, leafBlockStartValues);
    long[] arr = new long[leafBlockFPs.size()];
    for(int i=0;i<leafBlockFPs.size();i++) {
      arr[i] = leafBlockFPs.get(i);
    }
    out.writeIndex(config, config.maxPointsInLeafNode, arr, index, minPackedValue, maxPackedValue, pointCount, docsSeen.cardinality());
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

    BKDLeafBlock packedValues = new BKDLeafBlock() {
      @Override
      public int count() {
        return leafCount;
      }

      @Override
      public BytesRef packedValue(int position) {
        scratchBytesRef1.offset = config.packedBytesLength * position;
        return scratchBytesRef1;
      }

      @Override
      public int docId(int position) {
        return leafDocs[position];
      }
    };

    assert BKDLeafBlock.valuesInOrderAndBounds( config, 0, ArrayUtil.copyOfSubArray(leafValues, 0, config.packedBytesLength),
        ArrayUtil.copyOfSubArray(leafValues, (leafCount - 1) * config.packedBytesLength, leafCount * config.packedBytesLength),
        packedValues);

    out.writeLeafBlock(config, packedValues, commonPrefixLengths, 0, leafCardinality, scratch);
  }

  // TODO: there must be a simpler way?
  private void rotateToTree(int nodeID, int offset, int count, byte[] index, List<byte[]> leafBlockStartValues) {
    if (count == 1) {
      // Leaf index node
      System.arraycopy(leafBlockStartValues.get(offset), 0, index, nodeID*(1 + config.bytesPerDim) + 1, config.bytesPerDim);
    } else if (count > 1) {
      // Internal index node: binary partition of count
      int countAtLevel = 1;
      int totalCount = 0;
      while (true) {
        int countLeft = count - totalCount;
        if (countLeft <= countAtLevel) {
          // This is the last level, possibly partially filled:
          int lastLeftCount = Math.min(countAtLevel/2, countLeft);
          assert lastLeftCount >= 0;
          int leftHalf = (totalCount-1)/2 + lastLeftCount;

          int rootOffset = offset + leftHalf;

          System.arraycopy(leafBlockStartValues.get(rootOffset), 0, index, nodeID*(1 + config.bytesPerDim) + 1, config.bytesPerDim);

          // TODO: we could optimize/specialize, when we know it's simply fully balanced binary tree
          // under here, to save this while loop on each recursion

          // Recurse left
          rotateToTree(2*nodeID, offset, leftHalf, index, leafBlockStartValues);

          // Recurse right
          rotateToTree(2*nodeID+1, rootOffset+1, count-leftHalf-1, index, leafBlockStartValues);
          return;
        }
        totalCount += countAtLevel;
        countAtLevel *= 2;
      }
    } else {
      assert count == 0;
    }
  }

  /** How many points have been added so far */
  public long getPointCount() {
    return pointCount;
  }

  private void checkMaxLeafNodeCount(int numLeaves) {
    if ((1 + config.bytesPerDim) * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + config.maxPointsInLeafNode + ") and reindex");
    }
  }

  /* In the 1D case, we can simply sort points in ascending order and use the
   * same writing logic as we use at merge time. */
  public static long writeField1Dim(BKDConfig config, BKDIndexWriter out, MutablePointValues reader, int maxDoc, long totalPointCount) throws IOException {
    final OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(config, out, maxDoc, totalPointCount);

    MutablePointsReaderUtils.sort(config, maxDoc, reader, 0, Math.toIntExact(reader.size()));

    reader.intersect(new PointValues.IntersectVisitor() {

      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {
        oneDimWriter.add(packedValue, docID);
      }

      @Override
      public void visit(int docID) {
        throw new IllegalStateException();
      }

      @Override
      public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return PointValues.Relation.CELL_CROSSES_QUERY;
      }
    });

    return oneDimWriter.finish();
  }

  /** More efficient bulk-add for incoming {@link BKDReader}s.  This does a merge sort of the already
   *  sorted values and currently only works when numDims==1.  This returns -1 if all documents containing
   *  dimensional values were deleted. */
  public static long merge(BKDConfig config, BKDIndexWriter out, List<MergeState.DocMap> docMaps, List<BKDReader> readers, int maxDoc, long totalPointCount) throws IOException {
    assert docMaps == null || readers.size() == docMaps.size();
    final OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(config, out, maxDoc, totalPointCount);

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
          bkd.visitDocValues(state.commonPrefixLengths, state.scratchDataPackedValue, state.scratchMinIndexPackedValue, state.scratchMaxIndexPackedValue, state.in, state.scratchIterator, docsInBlock, new PointValues.IntersectVisitor() {
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
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              return PointValues.Relation.CELL_CROSSES_QUERY;
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
}


