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

/**
 *  Builds a block KD-tree from the incoming 1-dim points.  The tree is
 *  un-balance , which means the leaf nodes will have between 100% of
 *  the requested <code>maxPointsInLeafNode</code> except the last one that
 *  can have less points but the last level of the tree might not be fully filled.
 *  Values that fall exactly on a cell boundary may be in either cell.
 *
 *  <p>The number of dimensions must be 1, but every byte[] value is fixed length.
 *
 *  <p>This consumes heap during writing: it allocates a <code>Long[numLeaves]</code>,
 *  a <code>byte[numLeaves*(bytesPerDim)]</code>.
 *
 *  <p>
 *  <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> / (bytesPerDim)
 *  total points.
 *
 * @lucene.experimental */
public class OneDimensionBKDWriter {

  final BKDConfig config;
  final BKDIndexWriter indexWriter;
  final List<Long> leafBlockFPs = new ArrayList<>();
  final List<byte[]> leafBlockStartValues = new ArrayList<>();
  final byte[] leafValues;
  final int[] leafDocs;
  private long pointCount;
  private int leafCount;
  private int leafCardinality;
  private final long totalPointCount;

  final BytesRef scratchBytesRef = new BytesRef();
  final int[] commonPrefixLengths = new int[1];
  /** Minimum per-dim values, packed */
  protected final byte[] minPackedValue;

  /** Maximum per-dim values, packed */
  protected final byte[] maxPackedValue;

  private final BKDLeafBlock leafBlock;

  private OneDimensionBKDWriter(BKDConfig config, BKDIndexWriter indexWriter, long totalPointCount) {
    if (config.numIndexDims != 1) {
      throw new UnsupportedOperationException("numIndexDims must be 1 but got " + config.numIndexDims);
    }
    this.config = config;
    this.totalPointCount = totalPointCount;
    this.indexWriter = indexWriter;

    this.minPackedValue = new byte[config.packedIndexBytesLength];
    this.maxPackedValue = new byte[config.packedIndexBytesLength];

    this.lastPackedValue = new byte[config.packedBytesLength];
    this.leafValues = new byte[config.maxPointsInLeafNode * config.packedBytesLength];
    this.leafDocs = new int[config.maxPointsInLeafNode];

    scratchBytesRef.length = config.packedBytesLength;
    scratchBytesRef.bytes = leafValues;

    this.leafBlock = new BKDLeafBlock() {
      @Override
      public int count() {
        return leafCount;
      }

      @Override
      public BytesRef packedValue(int position) {
        scratchBytesRef.offset = config.packedBytesLength * position;
        return scratchBytesRef;
      }

      @Override
      public int docId(int position) {
        return leafDocs[position];
      }
    };
  }

  // for asserts
  final byte[] lastPackedValue;
  private int lastDocID;

  void add(byte[] packedValue, int docID) throws IOException {
    assert BKDLeafBlock.valueInOrder(config, pointCount + leafCount,
        0, lastPackedValue, packedValue, 0, docID, lastDocID);

    if (leafCount == 0 || Arrays.mismatch(leafValues, (leafCount - 1) * config.bytesPerDim, leafCount * config.bytesPerDim, packedValue, 0, config.bytesPerDim) != -1) {
      leafCardinality++;
    }
    System.arraycopy(packedValue, 0, leafValues, leafCount * config.packedBytesLength, config.packedBytesLength);
    leafDocs[leafCount] = docID;
    //docsSeen.set(docID);
    leafCount++;

    if (pointCount + leafCount > totalPointCount) {
      throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + (pointCount + leafCount) + " values");
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

  public long finish(int numDocs) throws IOException {
    if (leafCount > 0) {
      writeLeafBlock(leafCardinality);
      leafCardinality = 0;
      leafCount = 0;
    }

    if (pointCount == 0) {
      return -1;
    }

    long indexFP = indexWriter.getFilePointer();

    int numInnerNodes = leafBlockStartValues.size();
    byte[] index = new byte[(1 + numInnerNodes) * config.bytesPerDim];
    rotateToTree(1, 0, numInnerNodes, index, leafBlockStartValues);

    long[] newLeafBlockFPs = rotateLeafBlocks(leafBlockFPs);

    scratchBytesRef.bytes = index;
    scratchBytesRef.length = config.bytesPerDim;

    BKDInnerNodes nodes = new BKDInnerNodes() {
      @Override
      public int numberOfLeaves() {
        return newLeafBlockFPs.length;
      }

      @Override
      public int splitDimension(int nodeID) {
        return 0;
      }

      @Override
      public BytesRef splitPackedValue(int nodeID) {
        scratchBytesRef.offset = nodeID * config.bytesPerDim;
        return scratchBytesRef;
      }

      @Override
      public long leafBlockFP(int leafNode) {
        return newLeafBlockFPs[leafNode];
      }
    };

    indexWriter.writeIndex(config, nodes, config.maxPointsInLeafNode, minPackedValue, maxPackedValue, pointCount, numDocs);
    return indexFP;
  }

  private void writeLeafBlock(int leafCardinality) throws IOException {
    assert leafCount != 0;
    if (pointCount == 0) {
      System.arraycopy(leafValues, 0, minPackedValue, 0, config.packedIndexBytesLength);
    }
    System.arraycopy(leafValues, (leafCount - 1) * config.packedBytesLength, maxPackedValue, 0, config.packedIndexBytesLength);

    pointCount += leafCount;

    if (leafBlockFPs.size() > 0) {
      // Save the first (minimum) value in each leaf block except the first, to build the split value index in the end:
      leafBlockStartValues.add(ArrayUtil.copyOfSubArray(leafValues, 0, config.packedBytesLength));
    }
    leafBlockFPs.add(indexWriter.getFilePointer());
    checkMaxLeafNodeCount(leafBlockFPs.size());

    // Find per-dim common prefix:
    int offset = (leafCount - 1) * config.packedBytesLength;
    int prefix = Arrays.mismatch(leafValues, 0, config.bytesPerDim, leafValues, offset, offset + config.bytesPerDim);
    if (prefix == -1) {
      prefix = config.bytesPerDim;
    }

    commonPrefixLengths[0] = prefix;

    assert BKDLeafBlock.valuesInOrderAndBounds(config, leafBlock, 0, ArrayUtil.copyOfSubArray(leafValues, 0, config.packedBytesLength),
        ArrayUtil.copyOfSubArray(leafValues, (leafCount - 1) * config.packedBytesLength, leafCount * config.packedBytesLength));

    indexWriter.writeLeafBlock(config, leafBlock, commonPrefixLengths,  0, leafCardinality);
  }

  private long[] rotateLeafBlocks(List<Long> leafBlockFPs) {
    // Possibly rotate the leaf block FPs, if the index not fully balanced binary tree .
    // In this case the leaf nodes may straddle the two bottom levels of the binary tree:
    int numLeaves = leafBlockFPs.size();
    if (numLeaves > 1) {
      int levelCount = 2;
      while (true) {
        if (numLeaves >= levelCount && numLeaves <= 2 * levelCount) {
          if (2 * levelCount - numLeaves != 0) {
            int lastLevel = 2 * (numLeaves - levelCount);
            assert lastLevel >= 0;
            // Last level is partially filled, so we must rotate the leaf FPs to match.  We do this here, after loading
            // at read-time, so that we can still delta code them on disk at write:
            long[] newLeafBlockFPs = new long[numLeaves];
            int partition = numLeaves - lastLevel;
            for (int i = 0; i < partition; i++) {
              newLeafBlockFPs[i] = leafBlockFPs.get(lastLevel + i);
            }
            for (int i = partition; i < numLeaves; i++) {
              newLeafBlockFPs[i] = leafBlockFPs.get(i - partition);
            }
           return newLeafBlockFPs;
          }
          break;
        }

        levelCount *= 2;
      }
    }
    long[] newLeafBlockFPs = new long[numLeaves];
    for (int i = 0; i < numLeaves; i ++) {
      newLeafBlockFPs[i] = leafBlockFPs.get(i);
    }
    return newLeafBlockFPs;
  }

  private void rotateToTree(int nodeID, int offset, int count, byte[] index, List<byte[]> leafBlockStartValues) {
    if (count == 1) {
      // Leaf index node
      System.arraycopy(leafBlockStartValues.get(offset), 0, index, nodeID * config.bytesPerDim, config.bytesPerDim);
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
          int leftHalf = (totalCount - 1) / 2 + lastLeftCount;

          int rootOffset = offset + leftHalf;

          System.arraycopy(leafBlockStartValues.get(rootOffset), 0, index, nodeID * (config.bytesPerDim), config.bytesPerDim);

          // TODO: we could optimize/specialize, when we know it's simply fully balanced binary tree
          // under here, to save this while loop on each recursion

          // Recurse left
          rotateToTree(2 * nodeID, offset, leftHalf, index, leafBlockStartValues);

          // Recurse right
          rotateToTree(2 * nodeID + 1, rootOffset + 1, count - leftHalf - 1, index, leafBlockStartValues);
          return;
        }
        totalCount += countAtLevel;
        countAtLevel *= 2;
      }
    } else {
      assert count == 0;
    }
  }

  private void checkMaxLeafNodeCount(int numLeaves) {
    if (config.bytesPerDim * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + config.maxPointsInLeafNode + ") and reindex");
    }
  }

  /** writes a KD-tree from the incoming 1-dimension {@link MutablePointValues} */
  public static long writeField(BKDConfig config, BKDIndexWriter indexWriter, MutablePointValues reader, int maxDoc) throws IOException {
    if (config.numDims != 1) {
      throw new IllegalArgumentException("too many dimensions; expected one dimension, got + " + config.numDims);
    }
    MutablePointsReaderUtils.sort(config, maxDoc, reader, 0, Math.toIntExact(reader.size()));

    final OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(config, indexWriter, reader.size());

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

    return oneDimWriter.finish(reader.getDocCount());
  }

  /** writes a KD-tree from the incoming list of 1-dimension {@link BKDReader}s */
  public static long merge(BKDConfig config, BKDIndexWriter indexWriter, List<MergeState.DocMap> docMaps, List<BKDReader> readers, int maxDoc) throws IOException {
    if (config.numDims != 1) {
      throw new IllegalArgumentException("too many dimensions; expected one dimension, got + " + config.numDims);
    }
    assert docMaps == null || readers.size() == docMaps.size();

    BKDMergeQueue queue = new BKDMergeQueue(config.bytesPerDim, readers.size());

    // Worst case total maximum size (if none of the points are deleted):
    long totalPointsCount = 0;
    for(int i=0;i<readers.size();i++) {
      BKDReader bkd = readers.get(i);
      MergeState.DocMap docMap;
      if (docMaps == null) {
        docMap = null;
      } else {
        docMap = docMaps.get(i);
      }
      totalPointsCount += bkd.size();
      MergeReader reader = new MergeReader(bkd, docMap);
      if (reader.next()) {
        queue.add(reader);
      }
    }

    FixedBitSet docsSeen = new FixedBitSet(maxDoc);
    OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(config, indexWriter, totalPointsCount);

    while (queue.size() != 0) {
      MergeReader reader = queue.top();
      docsSeen.set(reader.docID);
      oneDimWriter.add(reader.state.scratchDataPackedValue, reader.docID);

      if (reader.next()) {
        queue.updateTop();
      } else {
        // This segment was exhausted
        queue.pop();
      }
    }

    return oneDimWriter.finish(docsSeen.cardinality());
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
      while (true) {
        if (docBlockUpto == docsInBlock) {
          if (blockID == bkd.leafNodeOffset) {
            return false;
          }
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
}
