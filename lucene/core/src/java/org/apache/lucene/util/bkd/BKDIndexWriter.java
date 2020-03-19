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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/** Utility class providing methods to serialize a KD-tree.
 *
 * @lucene.experimental */
public class BKDIndexWriter {

  public static final String CODEC_NAME = "BKD";
  public static final int VERSION_START = 4; // version used by Lucene 7.0
  //public static final int VERSION_CURRENT = VERSION_START;
  public static final int VERSION_LEAF_STORES_BOUNDS = 5;
  public static final int VERSION_SELECTIVE_INDEXING = 6;
  public static final int VERSION_LOW_CARDINALITY_LEAVES = 7;
  public static final int VERSION_CURRENT = VERSION_LOW_CARDINALITY_LEAVES;

  /** How many dimensions we are storing at the leaf (data) nodes */
  private final int numDataDims;

  /** How many dimensions we are indexing in the internal nodes */
  private final int numIndexDims;

  /** How many bytes each value in each dimension takes. */
  private final int bytesPerDim;

  /** numDataDims * bytesPerDim */
  private final int packedBytesLength;

  /** numIndexDims * bytesPerDim */
  private final int packedIndexBytesLength;

  private final int maxPointsInLeafNode;

  private final byte[] scratch;

  // Reused when writing leaf blocks
  private final ByteBuffersDataOutput scratchOut = ByteBuffersDataOutput.newResettableInstance();

  BKDIndexWriter(int numDataDims, int numIndexDims, int bytesPerDim, int maxPointsInLeafNode) {
    this.numDataDims = numDataDims;
    this.numIndexDims = numIndexDims;
    this.bytesPerDim = bytesPerDim;
    this.packedBytesLength = numDataDims * bytesPerDim;
    this.packedIndexBytesLength = numIndexDims * bytesPerDim;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.scratch = new byte[packedBytesLength];
  }

  /** writes a leaf block in the provided DataOutput */
  public void writeLeafBlock(DataOutput out, BKDLeafBlock leafBlock,
                             int[] commonPrefixes, int sortedDim, int leafCardinality) throws IOException {
    int count = leafBlock.count();
    assert count > 0 : "count must be bigger than 0";
    assert count <= maxPointsInLeafNode: "maxPointsInLeafNode=" + maxPointsInLeafNode + " > count=" + count;
    assert scratchOut.size() == 0;
    // Write docIDs first, as their own chunk, so that at intersect time we can add all docIDs w/o
    // loading the values:
    writeLeafBlockDocs(scratchOut, leafBlock);
    // Write common prefixes:
    writeCommonPrefixes(scratchOut, commonPrefixes, leafBlock.packedValue(0));
    // Write point values:
    writeLeafBlockPackedValues(scratchOut, commonPrefixes, sortedDim, leafBlock, leafCardinality);
    scratchOut.copyTo(out);
    scratchOut.reset();
  }

  /** writes inner nodes in the provided DataOutput */
  public void writeIndex(DataOutput out, int countPerLeaf, long[] leafBlockFPs, byte[] splitPackedValues,
                         byte[] minPackedValue, byte[] maxPackedValue, long pointCount, int numberDocs) throws IOException {
    final byte[] packedIndex = packIndex(leafBlockFPs, splitPackedValues);
    final int numLeaves = leafBlockFPs.length;
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
    out.writeVInt(numDataDims);
    out.writeVInt(numIndexDims);
    out.writeVInt(countPerLeaf);
    out.writeVInt(bytesPerDim);

    assert numLeaves > 0;
    out.writeVInt(numLeaves);
    out.writeBytes(minPackedValue, 0, packedIndexBytesLength);
    out.writeBytes(maxPackedValue, 0, packedIndexBytesLength);

    out.writeVLong(pointCount);
    out.writeVInt(numberDocs);
    out.writeVInt(packedIndex.length);
    out.writeBytes(packedIndex, 0, packedIndex.length);
  }

  /** Packs the two arrays, representing a balanced binary tree, into a compact byte[] structure. */
  private byte[] packIndex(long[] leafBlockFPs, byte[] splitPackedValues) throws IOException {

    final int numLeaves = leafBlockFPs.length;

    // Possibly rotate the leaf block FPs, if the index not fully balanced binary tree (only happens
    // if it was created by OneDimensionBKDWriter).  In this case the leaf nodes may straddle the two bottom
    // levels of the binary tree:
    if (numIndexDims == 1 && numLeaves > 1) {
      int levelCount = 2;
      while (true) {
        if (numLeaves >= levelCount && numLeaves <= 2*levelCount) {
          int lastLevel = 2*(numLeaves - levelCount);
          assert lastLevel >= 0;
          if (lastLevel != 0) {
            // Last level is partially filled, so we must rotate the leaf FPs to match.  We do this here, after loading
            // at read-time, so that we can still delta code them on disk at write:
            long[] newLeafBlockFPs = new long[numLeaves];
            System.arraycopy(leafBlockFPs, lastLevel, newLeafBlockFPs, 0, leafBlockFPs.length - lastLevel);
            System.arraycopy(leafBlockFPs, 0, newLeafBlockFPs, leafBlockFPs.length - lastLevel, lastLevel);
            leafBlockFPs = newLeafBlockFPs;
          }
          break;
        }

        levelCount *= 2;
      }
    }

    // This is the "file" we append the byte[] to:
    List<byte[]> blocks = new ArrayList<>();
    byte[] lastSplitValues = new byte[bytesPerDim * numIndexDims];
    //System.out.println("\npack index");
    assert scratchOut.size() == 0;
    int totalSize = recursePackIndex(scratchOut, leafBlockFPs, splitPackedValues, 0l, blocks, 1, lastSplitValues, new boolean[numIndexDims], false);
    scratchOut.reset();
    // Compact the byte[] blocks into single byte index:
    byte[] index = new byte[totalSize];
    int upto = 0;
    for(byte[] block : blocks) {
      System.arraycopy(block, 0, index, upto, block.length);
      upto += block.length;
    }
    assert upto == totalSize;

    return index;
  }

  /**
   * lastSplitValues is per-dimension split value previously seen; we use this to prefix-code the split byte[] on each inner node
   */
  private int recursePackIndex(ByteBuffersDataOutput writeBuffer, long[] leafBlockFPs, byte[] splitPackedValues, long minBlockFP, List<byte[]> blocks,
                               int nodeID, byte[] lastSplitValues, boolean[] negativeDeltas, boolean isLeft) throws IOException {
    if (nodeID >= leafBlockFPs.length) {
      int leafID = nodeID - leafBlockFPs.length;
      //System.out.println("recursePack leaf nodeID=" + nodeID);

      // In the unbalanced case it's possible the left most node only has one child:
      if (leafID < leafBlockFPs.length) {
        long delta = leafBlockFPs[leafID] - minBlockFP;
        if (isLeft) {
          assert delta == 0;
          return 0;
        } else {
          assert nodeID == 1 || delta > 0: "nodeID=" + nodeID;
          writeBuffer.writeVLong(delta);
          return appendBlock(writeBuffer, blocks);
        }
      } else {
        return 0;
      }
    } else {
      long leftBlockFP;
      if (isLeft == false) {
        leftBlockFP = getLeftMostLeafBlockFP(leafBlockFPs, nodeID);
        long delta = leftBlockFP - minBlockFP;
        assert nodeID == 1 || delta > 0 : "expected nodeID=1 or delta > 0; got nodeID=" + nodeID + " and delta=" + delta;
        writeBuffer.writeVLong(delta);
      } else {
        // The left tree's left most leaf block FP is always the minimal FP:
        leftBlockFP = minBlockFP;
      }

      int address = nodeID * (1+bytesPerDim);
      int splitDim = splitPackedValues[address++] & 0xff;

      //System.out.println("recursePack inner nodeID=" + nodeID + " splitDim=" + splitDim + " splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

      // find common prefix with last split value in this dim:
      int prefix = Arrays.mismatch(splitPackedValues, address, address + bytesPerDim, lastSplitValues,
          splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim);
      if (prefix == -1) {
        prefix = bytesPerDim;
      }

      //System.out.println("writeNodeData nodeID=" + nodeID + " splitDim=" + splitDim + " numDims=" + numDims + " bytesPerDim=" + bytesPerDim + " prefix=" + prefix);

      int firstDiffByteDelta;
      if (prefix < bytesPerDim) {
        //System.out.println("  delta byte cur=" + Integer.toHexString(splitPackedValues[address+prefix]&0xFF) + " prev=" + Integer.toHexString(lastSplitValues[splitDim * bytesPerDim + prefix]&0xFF) + " negated?=" + negativeDeltas[splitDim]);
        firstDiffByteDelta = (splitPackedValues[address+prefix]&0xFF) - (lastSplitValues[splitDim * bytesPerDim + prefix]&0xFF);
        if (negativeDeltas[splitDim]) {
          firstDiffByteDelta = -firstDiffByteDelta;
        }
        //System.out.println("  delta=" + firstDiffByteDelta);
        assert firstDiffByteDelta > 0;
      } else {
        firstDiffByteDelta = 0;
      }

      // pack the prefix, splitDim and delta first diff byte into a single vInt:
      int code = (firstDiffByteDelta * (1+bytesPerDim) + prefix) * numIndexDims + splitDim;

      //System.out.println("  code=" + code);
      //System.out.println("  splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

      writeBuffer.writeVInt(code);

      // write the split value, prefix coded vs. our parent's split value:
      int suffix = bytesPerDim - prefix;
      byte[] savSplitValue = new byte[suffix];
      if (suffix > 1) {
        writeBuffer.writeBytes(splitPackedValues, address+prefix+1, suffix-1);
      }

      byte[] cmp = lastSplitValues.clone();

      System.arraycopy(lastSplitValues, splitDim * bytesPerDim + prefix, savSplitValue, 0, suffix);

      // copy our split value into lastSplitValues for our children to prefix-code against
      System.arraycopy(splitPackedValues, address+prefix, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

      int numBytes = appendBlock(writeBuffer, blocks);

      // placeholder for left-tree numBytes; we need this so that at search time if we only need to recurse into the right sub-tree we can
      // quickly seek to its starting point
      int idxSav = blocks.size();
      blocks.add(null);

      boolean savNegativeDelta = negativeDeltas[splitDim];
      negativeDeltas[splitDim] = true;

      int leftNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValues, leftBlockFP, blocks, 2*nodeID, lastSplitValues, negativeDeltas, true);

      if (nodeID * 2 < leafBlockFPs.length) {
        writeBuffer.writeVInt(leftNumBytes);
      } else {
        assert leftNumBytes == 0: "leftNumBytes=" + leftNumBytes;
      }

      byte[] bytes2 = writeBuffer.toArrayCopy();
      writeBuffer.reset();
      // replace our placeholder:
      blocks.set(idxSav, bytes2);

      negativeDeltas[splitDim] = false;
      int rightNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValues, leftBlockFP, blocks, 2*nodeID+1, lastSplitValues, negativeDeltas, false);

      negativeDeltas[splitDim] = savNegativeDelta;

      // restore lastSplitValues to what caller originally passed us:
      System.arraycopy(savSplitValue, 0, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

      assert Arrays.equals(lastSplitValues, cmp);

      return numBytes + bytes2.length + leftNumBytes + rightNumBytes;
    }
  }

  private long getLeftMostLeafBlockFP(long[] leafBlockFPs, int nodeID) {
    // TODO: can we do this cheaper, e.g. a closed form solution instead of while loop?  Or
    // change the recursion while packing the index to return this left-most leaf block FP
    // from each recursion instead?
    //
    // Still, the overall cost here is minor: this method's cost is O(log(N)), and while writing
    // we call it O(N) times (N = number of leaf blocks)
    while (nodeID < leafBlockFPs.length) {
      nodeID *= 2;
    }
    int leafID = nodeID - leafBlockFPs.length;
    long result = leafBlockFPs[leafID];
    if (result < 0) {
      throw new AssertionError(result + " for leaf " + leafID);
    }
    return result;
  }

  /** Appends the current contents of writeBuffer as another block on the growing in-memory file */
  private int appendBlock(ByteBuffersDataOutput writeBuffer, List<byte[]> blocks) throws IOException {
    byte[] block = writeBuffer.toArrayCopy();
    blocks.add(block);
    writeBuffer.reset();
    return block.length;
  }

  private void writeLeafBlockDocs(DataOutput out, BKDLeafBlock leafBlock) throws IOException {
    out.writeVInt(leafBlock.count());
    DocIdsWriter.writeDocIds(leafBlock, out);
  }

  private void writeCommonPrefixes(DataOutput out, int[] commonPrefixes, BytesRef packedValue) throws IOException {
    for(int dim=0;dim<numDataDims;dim++) {
      out.writeVInt(commonPrefixes[dim]);
      //System.out.println(commonPrefixes[dim] + " of " + bytesPerDim);
      out.writeBytes(packedValue.bytes, packedValue.offset + dim*bytesPerDim, commonPrefixes[dim]);
    }
  }

  private void writeLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int sortedDim, BKDLeafBlock packedValues, int leafCardinality) throws IOException {
    final int count = packedValues.count();
    int prefixLenSum = Arrays.stream(commonPrefixLengths).sum();
    if (prefixLenSum == packedBytesLength) {
      // all values in this block are equal
      out.writeByte((byte) -1);
    } else {
      assert commonPrefixLengths[sortedDim] < bytesPerDim;
      // estimate if storing the values with cardinality is cheaper than storing all values.
      int compressedByteOffset = sortedDim * bytesPerDim + commonPrefixLengths[sortedDim];
      int highCardinalityCost;
      int lowCardinalityCost;
      if (count == leafCardinality) {
        // all values in this block are different
        highCardinalityCost = 0;
        lowCardinalityCost = 1;
      } else {
        // compute cost of runLen compression
        int numRunLens = 0;
        for (int i = 0; i < count; ) {
          // do run-length compression on the byte at compressedByteOffset
          int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
          assert runLen <= 0xff;
          numRunLens++;
          i += runLen;
        }
        // Add cost of runLen compression
        highCardinalityCost = count * (packedBytesLength - prefixLenSum - 1) + 2 * numRunLens;
        // +1 is the byte needed for storing the cardinality
        lowCardinalityCost = leafCardinality * (packedBytesLength - prefixLenSum + 1);
      }
      if (lowCardinalityCost <= highCardinalityCost) {
        out.writeByte((byte) -2);
        writeLowCardinalityLeafBlockPackedValues(out, commonPrefixLengths, packedValues);
      } else {
        out.writeByte((byte) sortedDim);
        writeHighCardinalityLeafBlockPackedValues(out, commonPrefixLengths, sortedDim, packedValues, compressedByteOffset);
      }
    }
  }

  private static int runLen(BKDLeafBlock packedValues, int start, int end, int byteOffset) {
    BytesRef first = packedValues.packedValue(start);
    byte b = first.bytes[first.offset + byteOffset];
    for (int i = start + 1; i < end; ++i) {
      BytesRef ref = packedValues.packedValue(i);
      byte b2 = ref.bytes[ref.offset + byteOffset];
      assert Byte.toUnsignedInt(b2) >= Byte.toUnsignedInt(b);
      if (b != b2) {
        return i - start;
      }
    }
    return end - start;
  }

  private void writeLowCardinalityLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, BKDLeafBlock packedValues) throws IOException {
    final int count = packedValues.count();
    if (numIndexDims != 1) {
      writeActualBounds(out, commonPrefixLengths, packedValues);
    }
    BytesRef value = packedValues.packedValue(0);
    System.arraycopy(value.bytes, value.offset, scratch, 0, packedBytesLength);
    int cardinality = 1;
    for (int i = 1; i < count; i++) {
      value = packedValues.packedValue(i);
      for(int dim = 0; dim < numDataDims; dim++) {
        final int start = dim * bytesPerDim + commonPrefixLengths[dim];
        final int end = dim * bytesPerDim + bytesPerDim;
        if (Arrays.mismatch(value.bytes, value.offset + start, value.offset + end, scratch, start, end) != -1) {
          out.writeVInt(cardinality);
          for (int j = 0; j < numDataDims; j++) {
            out.writeBytes(scratch, j * bytesPerDim + commonPrefixLengths[j], bytesPerDim - commonPrefixLengths[j]);
          }
          System.arraycopy(value.bytes, value.offset, scratch, 0, packedBytesLength);
          cardinality = 1;
          break;
        } else if (dim == numDataDims - 1){
          cardinality++;
        }
      }
    }
    out.writeVInt(cardinality);
    for (int i = 0; i < numDataDims; i++) {
      out.writeBytes(scratch, i * bytesPerDim + commonPrefixLengths[i], bytesPerDim - commonPrefixLengths[i]);
    }
  }

  private void writeActualBounds(DataOutput out, int[] commonPrefixLengths, BKDLeafBlock packedValues) throws IOException {
    for (int dim = 0; dim < numIndexDims; ++dim) {
      int commonPrefixLength = commonPrefixLengths[dim];
      int suffixLength = bytesPerDim - commonPrefixLength;
      if (suffixLength > 0) {
        BytesRef[] minMax = computeMinMax(packedValues, dim * bytesPerDim + commonPrefixLength, suffixLength);
        BytesRef min = minMax[0];
        BytesRef max = minMax[1];
        out.writeBytes(min.bytes, min.offset, min.length);
        out.writeBytes(max.bytes, max.offset, max.length);
      }
    }
  }

  /** Return an array that contains the min and max values for the [offset, offset+length] interval
   *  of the given {@link BytesRef}s. */
  private static BytesRef[] computeMinMax(BKDLeafBlock packedValues, int offset, int length) {
    assert length > 0;
    BytesRefBuilder min = new BytesRefBuilder();
    BytesRefBuilder max = new BytesRefBuilder();
    BytesRef first = packedValues.packedValue(0);
    min.copyBytes(first.bytes, first.offset + offset, length);
    max.copyBytes(first.bytes, first.offset + offset, length);
    for (int i = 1; i < packedValues.count(); ++i) {
      BytesRef candidate = packedValues.packedValue(i);
      if (Arrays.compareUnsigned(min.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) > 0) {
        min.copyBytes(candidate.bytes, candidate.offset + offset, length);
      } else if (Arrays.compareUnsigned(max.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) < 0) {
        max.copyBytes(candidate.bytes, candidate.offset + offset, length);
      }
    }
    return new BytesRef[]{min.get(), max.get()};
  }

  private void writeLeafBlockPackedValuesRange(DataOutput out, int[] commonPrefixLengths, int start, int end, BKDLeafBlock packedValues) throws IOException {
    for (int i = start; i < end; ++i) {
      BytesRef ref = packedValues.packedValue(i);
      assert ref.length == packedBytesLength;

      for(int dim=0;dim<numDataDims;dim++) {
        int prefix = commonPrefixLengths[dim];
        out.writeBytes(ref.bytes, ref.offset + dim*bytesPerDim + prefix, bytesPerDim-prefix);
      }
    }
  }

  private void writeHighCardinalityLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int sortedDim, BKDLeafBlock packedValues, int compressedByteOffset) throws IOException {
    if (numIndexDims != 1) {
      writeActualBounds(out, commonPrefixLengths, packedValues);
    }
    final int count = packedValues.count();
    commonPrefixLengths[sortedDim]++;
    for (int i = 0; i < count; ) {
      // do run-length compression on the byte at compressedByteOffset
      int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
      assert runLen <= 0xff;
      BytesRef first = packedValues.packedValue(i);
      byte prefixByte = first.bytes[first.offset + compressedByteOffset];
      out.writeByte(prefixByte);
      out.writeByte((byte) runLen);
      writeLeafBlockPackedValuesRange(out, commonPrefixLengths, i, i + runLen, packedValues);
      i += runLen;
      assert i <= count;
    }
  }
}
