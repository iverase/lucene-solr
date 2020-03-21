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

import java.util.Arrays;

import org.apache.lucene.util.BytesRef;

public interface BKDLeafBlock {

  int count();

  BytesRef packedValue(int position);

  int docId(int position);


  // only called from assert
  static boolean valuesInOrderAndBounds(BKDConfig config, int sortedDim, byte[] minPackedValue, byte[] maxPackedValue, BKDLeafBlock values) {
    byte[] lastPackedValue = new byte[config.packedBytesLength];
    int lastDoc = -1;
    for (int i = 0; i < values.count(); i++) {
      BytesRef packedValue = values.packedValue(i);
      assert packedValue.length == config.packedBytesLength;
      assert valueInOrder(config, i, sortedDim, lastPackedValue, packedValue.bytes, packedValue.offset, values.docId(i), lastDoc);
      lastDoc = values.docId(i);

      // Make sure this value does in fact fall within this leaf cell:
      assert valueInBounds(config, packedValue, minPackedValue, maxPackedValue);
    }
    return true;
  }

  // only called from assert
  static boolean valueInOrder(BKDConfig config, long ord, int sortedDim, byte[] lastPackedValue, byte[] packedValue, int packedValueOffset,
                                      int doc, int lastDoc) {
    int dimOffset = sortedDim * config.bytesPerDim;
    if (ord > 0) {
      int cmp = Arrays.compareUnsigned(lastPackedValue, dimOffset, dimOffset + config.bytesPerDim, packedValue, packedValueOffset + dimOffset, packedValueOffset + dimOffset + config.bytesPerDim);
      if (cmp > 0) {
        throw new AssertionError("values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, config.packedBytesLength) + " ord=" + ord);
      }
      if (cmp == 0  && config.numDataDims > config.numIndexDims) {
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

  /** Called only in assert */
  static boolean valueInBounds(BKDConfig config, BytesRef packedValue, byte[] minPackedValue, byte[] maxPackedValue) {
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
}
