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
package org.apache.lucene.util;


import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.Arrays;

/**
 * BitSet of fixed length (numBits), accessed with an int index, implementing {@link Bits} and
 * {@link DocIdSet}. If you need to manage more than 2.1B bits, use
 * {@link LongBitSet}.
 *
 * @lucene.internal
 */
public final class PagedFixedBitSet extends BitSet implements Bits, Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PagedFixedBitSet.class);
  private static final int PAGE_SIZE = 512 * 1024; // 512K
  protected static final int WORDS_PER_PAGE = PAGE_SIZE / Long.BYTES;
  private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(WORDS_PER_PAGE);
  private static final int PAGE_MASK = WORDS_PER_PAGE - 1;
  private final long[][] pages; // Array of longs holding the bits
  private final int numBits; // The number of bits in use
  private final int numPages; // The exact number of longs needed to hold numBits (<= bits.length)
  private final int lastPageSize;
  private final int lastPage;

  /**
   * If the given {@link PagedFixedBitSet} is large enough to hold {@code numBits+1},
   * returns the given bits, otherwise returns a new {@link PagedFixedBitSet} which
   * can hold the requested number of bits.
   * <p>
   * <b>NOTE:</b> the returned bitset reuses the underlying {@code long[]} of
   * the given {@code bits} if possible. Also, calling {@link #length()} on the
   * returned bits may return a value greater than {@code numBits}.
   */
//  public static PagedFixedBitSet ensureCapacity(PagedFixedBitSet bits, int numBits) {
//    if (numBits < bits.numBits) {
//      return bits;
//    } else {
//      // Depends on the ghost bits being clear!
//      // (Otherwise, they may become visible in the new instance)
//      int numWords = bits2words(numBits);
//      long[] arr = bits.getBits();
//      if (numWords >= arr.length) {
//        arr = ArrayUtil.grow(arr, numWords + 1);
//      }
//      return new PagedFixedBitSet(arr, arr.length << 6);
//    }
//  }

  /** returns the number of 64 bit words it would take to hold numBits */
  public static int bits2words(int numBits) {
    return ((numBits - 1) >> 6) + 1; // I.e.: get the word-offset of the last bit and add one (make sure to use >> so 0 returns 0!)
  }

  public static int pageIndex(int index) {
    return index >>> PAGE_SHIFT;
  }

  public static int indexInPage(int index) {
    return index & PAGE_MASK;
  }

  /**
   * Returns the popcount or cardinality of the intersection of the two sets.
   * Neither set is modified.
   */
  public static long intersectionCount(PagedFixedBitSet a, PagedFixedBitSet b) {
    int pages = (a.numBits > b.numBits) ? b.numPages : a.numPages;
    int lastPos = (a.numBits > b.numBits) ? b.lastPageSize : a.lastPageSize;
    long tot = 0;
    for (int i = 0; i < pages; i++) {
      int len = i == pages - 1 ? lastPos : WORDS_PER_PAGE;
      tot += BitUtil.pop_intersect(a.pages[i], b.pages[i], 0, len);
    }
    return tot;
  }

  /**
   * Returns the popcount or cardinality of the union of the two sets. Neither
   * set is modified.
   */
  public static long unionCount(PagedFixedBitSet a, PagedFixedBitSet b) {
    int pages = (a.numBits > b.numBits) ? b.numPages : a.numPages;
    int lastPos = (a.numBits > b.numBits) ? b.lastPageSize : a.lastPageSize;
    long tot = 0;
    for (int i = 0; i < pages; i++) {
      int len = i == pages - 1 ? lastPos : WORDS_PER_PAGE;
      tot += BitUtil.pop_union(a.pages[i], b.pages[i], 0, len);
    }
    if (a.numBits < b.numBits) {
      int len = pages - 1 == b.lastPage ? b.lastPageSize - lastPos : WORDS_PER_PAGE - lastPos;
      tot += BitUtil.pop_array(b.pages[pages - 1], lastPos, len);
      for (int i = pages; i < b.numPages; i++) {
        len = i == b.lastPage ? b.lastPageSize : WORDS_PER_PAGE;
        tot += BitUtil.pop_array(b.pages[i], 0, len);
      }
    } else if (a.numBits > b.numBits) {
      int len = pages - 1 == a.lastPage ? a.lastPageSize - lastPos : WORDS_PER_PAGE - lastPos;
      tot += BitUtil.pop_array(a.pages[pages - 1], lastPos, len);
      for (int i = pages; i < a.numPages; i++) {
        len = i == a.lastPage ? a.lastPageSize : WORDS_PER_PAGE;
        tot += BitUtil.pop_array(a.pages[i], 0, len);
      }
    }
    return tot;
  }

  /**
   * Returns the popcount or cardinality of "a and not b" or
   * "intersection(a, not(b))". Neither set is modified.
   */
  public static long andNotCount(PagedFixedBitSet a, PagedFixedBitSet b) {
    // Depends on the ghost bits being clear!
    int pages = (a.numBits > b.numBits) ? b.numPages : a.numPages;
    int lastPos = (a.numBits > b.numBits) ? b.lastPageSize : a.lastPageSize;
    long tot = 0;
    for (int i = 0; i < pages; i++) {
      int len = i == pages - 1 ? lastPos : WORDS_PER_PAGE;
      tot += BitUtil.pop_andnot(a.pages[i], b.pages[i], 0, len);
    }
    return tot;
  }

  /**
   * Creates a new LongBitSet.
   * The internally allocated long array will be exactly the size needed to accommodate the numBits specified.
   * @param numBits the number of bits needed
   */
  public PagedFixedBitSet(int numBits) {
    this.numBits = numBits;
    int words = bits2words(numBits);
    numPages = pageIndex(words) + 1;
    pages = new long[numPages][];
    for (int i = 0; i < numPages - 1; i++) {
      pages[i] = new long[WORDS_PER_PAGE];
    }
    lastPage = numPages - 1;
    lastPageSize = indexInPage(words) + 1;
    pages[numPages - 1] = new long[lastPageSize];
  }



  /**
   * Creates a new LongBitSet using the provided long[] array as backing store.
   * The storedBits array must be large enough to accommodate the numBits specified, but may be larger.
   * In that case the 'extra' or 'ghost' bits must be clear (or they may provoke spurious side-effects)
   * @param pages the array to use as backing store
   * @param numBits the number of bits actually needed
   */
  protected PagedFixedBitSet(long[][] pages, int numBits) {
    int wordNum = bits2words(numBits);
    this.numPages = pageIndex(wordNum) + 1;
    if (numPages > pages.length) {
      throw new IllegalArgumentException("The are not enough pages to hold " + numBits + " bits. " +
              "Minimum expected: " + numPages + ", got " + pages.length);
    }
    lastPage = numPages - 1;
    this.numBits = numBits;
    this.pages = pages;
    lastPageSize = indexInPage(wordNum) + 1;
    //assert verifyGhostBitsClear();
  }

  /**
   * Checks if the bits past numBits are clear.
   * Some methods rely on this implicit assumption: search for "Depends on the ghost bits being clear!"
   * @return true if the bits past numBits are clear.
   */
//  private boolean verifyGhostBitsClear() {
//    for (int i = numPages; i < bits.length; i++) {
//      if (bits[i] != 0) return false;
//    }
//
//    if ((numBits & 0x3f) == 0) return true;
//
//    long mask = -1L << numBits;
//
//    return (bits[numPages - 1] & mask) == 0;
//  }

  @Override
  public int length() {
    return numBits;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + numPages * RamUsageEstimator.sizeOf(pages[0]);
  }

  /** Expert. */
//  public long[] getBits() {
//    return bits;
//  }

  /** Returns number of set bits.  NOTE: this visits every
   *  long in the backing bits array, and the result is not
   *  internally cached!
   */
  @Override
  public int cardinality() {
    // Depends on the ghost bits being clear!
    int cardinality = 0;
    for (int i = 0; i < lastPage; i++) {
      cardinality += (int) BitUtil.pop_array(pages[i], 0, WORDS_PER_PAGE);
    }
    cardinality += (int) BitUtil.pop_array(pages[lastPage], 0, lastPageSize);
    return cardinality;
  }

  @Override
  public boolean get(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    int pageNumber = pageIndex(wordNum);
    int pagePosition = indexInPage(wordNum);
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    long bitmask = 1L << index;
    return (pages[pageNumber][pagePosition] & bitmask) != 0;
  }

  @Override
  public void set(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;      // div 64
    int pageNumber = pageIndex(wordNum);
    int pagePosition = indexInPage(wordNum);
    long bitmask = 1L << index;
    pages[pageNumber][pagePosition] |= bitmask;
  }

  /** Sets a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to set
   */
  public void set(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits : "startIndex=" + startIndex + ", numBits=" + numBits;
    assert endIndex >= 0 && endIndex <= numBits : "endIndex=" + endIndex + ", numBits=" + numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int startPage = pageIndex(startWord);
    int startPos = indexInPage(startWord);
    int endWord = (endIndex-1) >> 6;

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex since only the lowest 6 bits are used

    if (startWord == endWord) {
      pages[startPage][startPos] |= (startmask & endmask);
      return;
    }

    int endPage = pageIndex(endWord);
    int endPos = indexInPage(endWord);

    pages[startPage][startPos] |= startmask;;
    if (startPage == endPage) {
      Arrays.fill(pages[startPage], startPos+1, endPos, -1L);
      pages[startPage][endPos] |= endmask;
      return;
    }
    Arrays.fill(pages[startPage], startPos+1, WORDS_PER_PAGE, -1L);
    for (int i = startPage+1; i < endPage; i++) {
      Arrays.fill(pages[i], 0, WORDS_PER_PAGE, -1L);
    }
    Arrays.fill(pages[endPage], 0, endPos, - 1L);
    pages[endPage][endPos] |= endmask;
  }

  public boolean getAndSet(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;      // div 64
    int pageNumber = pageIndex(wordNum);
    int pagePosition = indexInPage(wordNum);
    long bitmask = 1L << index;
    boolean val = (pages[pageNumber][pagePosition] & bitmask) != 0;
    pages[pageNumber][pagePosition] |= bitmask;
    return val;
  }

  @Override
  public void clear(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;
    int pageNumber = pageIndex(wordNum);
    int pagePosition = indexInPage(wordNum);
    long bitmask = 1L << index;
    pages[pageNumber][pagePosition] &= ~bitmask;
  }

  public boolean getAndClear(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;      // div 64
    int pageNumber = pageIndex(wordNum);
    int pagePosition = indexInPage(wordNum);
    long bitmask = 1L << index;
    boolean val = (pages[pageNumber][pagePosition] & bitmask) != 0;
    pages[pageNumber][pagePosition] &= ~bitmask;
    return val;
  }

  @Override
  public int nextSetBit(int index) {
    // Depends on the ghost bits being clear!
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;
    int pageIndex = pageIndex(wordNum);
    int pagePosition = indexInPage(wordNum);
    long word = pages[pageIndex][pagePosition] >> index;  // skip all the bits to the right of index

    if (word!=0) {
      return index + Long.numberOfTrailingZeros(word);
    }
    while(pageIndex < numPages) {
      page = pages[pageIndex];
      int end = pageIndex == lastPage ? lastPageSize : WORDS_PER_PAGE;
      while(++pagePosition < end) {
        word = page[pagePosition];
        if (word != 0) {
          return (((pageIndex * WORDS_PER_PAGE) + pagePosition)  << 6) + Long.numberOfTrailingZeros(word);
        }
      }
      pagePosition = -1;
      pageIndex++;
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public int prevSetBit(int index) {
    assert index >= 0 && index < numBits: "index=" + index + " numBits=" + numBits;
    int wordNum = index >> 6;
    int pageIndex = pageIndex(wordNum);
    long[] page = pages[pageIndex];
    int pagePosition = indexInPage(wordNum);
    final int subIndex = index & 0x3f;  // index within the word
    long word = (page[pagePosition] << (63-subIndex));  // skip all the bits to the left of index

    if (word != 0) {
      return (wordNum << 6) + subIndex - Long.numberOfLeadingZeros(word); // See LUCENE-3197
    }

    while (--pagePosition >= 0) {
      word = page[pagePosition];
      if (word !=0 ) {
        return (wordNum << 6) + 63 - Long.numberOfLeadingZeros(word);
      }
    }

    while(--pageIndex >= 0) {
      page = pages[pageIndex];
      for (int j = WORDS_PER_PAGE - 1; j >= 0; j--) {
        word = page[j];
        if (word != 0) {
          return (wordNum << 6) + 63 - Long.numberOfLeadingZeros(word);
        }
      }
    }
    return -1;
  }

  @Override
  public void clear(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits : "startIndex=" + startIndex + ", numBits=" + numBits;
    assert endIndex >= 0 && endIndex <= numBits : "endIndex=" + endIndex + ", numBits=" + numBits;
    if (endIndex <= startIndex) {
      return;
    }

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex since only the lowest 6 bits are used

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    int startWord = startIndex >> 6;
    int startPage = pageIndex(startWord);
    int startPos = indexInPage(startWord);
    int endWord = (endIndex-1) >> 6;

    if (startWord == endWord) {
      pages[startPage][startPos] &= (startmask | endmask);
      return;
    }

    int endPage = pageIndex(endWord);
    int endPos = indexInPage(endWord);

    pages[startPage][startPos] &= startmask;
    if (startPage == endPage) {
      Arrays.fill(pages[startPage], startPos+1, endPos, 0L);
      pages[startPage][endPos] &= endmask;
      return;
    }
    Arrays.fill(pages[startPage], startPos+1, WORDS_PER_PAGE, 0L);
    for (int i = startPage+1; i < endPage; i++) {
      Arrays.fill(pages[i], 0, WORDS_PER_PAGE, 0L);
    }
    Arrays.fill(pages[endPage], 0, endPos, 0L);
    pages[endPage][endPos] &= endmask;
  }

  @Override
  public PagedFixedBitSet clone() {
    long[][] pages = new long[numPages][];
    for(int i = 0; i < numPages; i++) {
      int len = (i == lastPage) ? lastPage : WORDS_PER_PAGE;
      pages[i] = new long[len];
      System.arraycopy(this.pages[i], 0, pages[i], 0, len);
    }
    return new PagedFixedBitSet(pages, numBits);
  }

  /** Flip the bit at the provided index. */
  public void flip(int index) {
    assert index >= 0 && index < numBits: "index=" + index + " numBits=" + numBits;
    int wordNum = index >> 6; // div 64
    int pageIndex = pageIndex(wordNum);
    int pagePosition = indexInPage(wordNum);
    long bitmask = 1L << index; // mod 64 is implicit
    pages[pageIndex][pagePosition] ^= bitmask;
  }

  /** Flips a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to flip
   */
  public void flip(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits;
    assert endIndex >= 0 && endIndex <= numBits;
    if (endIndex <= startIndex) {
      return;
    }
    //TODO: make more efficient
    for (int i = startIndex; i < endIndex; i++) {
      flip(i);
    }
  }

  public void andNot(PagedFixedBitSet other) {
    final long[][] otherPages = other.pages;
    int minPages = Math.min(this.pages.length, otherPages.length);
    for (int page = 0; page < minPages; page++) {
      for (int pos = 0; pos < WORDS_PER_PAGE; pos++) {
        pages[page][pos] &= ~otherPages[page][pos];
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PagedFixedBitSet)) {
      return false;
    }
    PagedFixedBitSet other = (PagedFixedBitSet) o;
    if (numBits != other.numBits) {
      return false;
    }
    for(int i =0; i < numPages; i++) {
      if(Arrays.equals(pages[i], other.pages[i]) == false) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    // Depends on the ghost bits being clear!
    long h = 0;
    for (int j = lastPageSize; --j >= 0; ) {
      h ^= pages[numPages - 1][j];
      h = (h << 1) | (h >>> 63); // rotate left
    }
    for(int i = lastPage; --i >= 0; ) {
      for (int j = WORDS_PER_PAGE; --j >= 0; ) {
        h ^= pages[i][j];
        h = (h << 1) | (h >>> 63); // rotate left
      }
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int) ((h>>32) ^ h) + 0x98761234;
  }
}
