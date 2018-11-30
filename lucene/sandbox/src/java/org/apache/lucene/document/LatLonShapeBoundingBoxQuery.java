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
package org.apache.lucene.document;

import java.util.Arrays;

import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.document.LatLonShape.BYTES;
import static org.apache.lucene.geo.GeoEncodingUtils.MAX_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.MIN_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * Finds all previously indexed shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.LatLonShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class LatLonShapeBoundingBoxQuery extends LatLonShapeQuery {
  final byte[] bbox;
  final byte[] west;
  final int minX;
  final int maxX;
  final int minY;
  final int maxY;

  public LatLonShapeBoundingBoxQuery(String field, LatLonShape.QueryRelation queryRelation, double minLat, double maxLat, double minLon, double maxLon) {
    super(field, queryRelation);

    this.bbox = new byte[4 * LatLonShape.BYTES];
    int minXenc = encodeLongitudeCeil(minLon);
    int maxXenc = encodeLongitude(maxLon);
    int minYenc = encodeLatitudeCeil(minLat);
    int maxYenc = encodeLatitude(maxLat);
    if (minYenc > maxYenc) {
      minYenc = maxYenc;
    }
    this.minY = minYenc;
    this.maxY = maxYenc;

    if (minLon > maxLon == true) {
      // crossing dateline is split into east/west boxes
      this.west = new byte[4 * LatLonShape.BYTES];
      this.minX = minXenc;
      this.maxX = maxXenc;
      encode(MIN_LON_ENCODED, this.maxX, this.minY, this.maxY, this.west);
      encode(this.minX, MAX_LON_ENCODED, this.minY, this.maxY, this.bbox);
    } else {
      // encodeLongitudeCeil may cause minX to be > maxX iff
      // the delta between the longtude < the encoding resolution
      if (minXenc > maxXenc) {
        minXenc = maxXenc;
      }
      this.west = null;
      this.minX = minXenc;
      this.maxX = maxXenc;
      encode(this.minX, this.maxX, this.minY, this.maxY, bbox);
    }
  }

  /** encodes a bounding box into the provided byte array */
  private static void encode(final int minX, final int maxX, final int minY, final int maxY, byte[] b) {
    if (b == null) {
      b = new byte[4 * LatLonShape.BYTES];
    }
    NumericUtils.intToSortableBytes(minY, b, 0);
    NumericUtils.intToSortableBytes(minX, b, BYTES);
    NumericUtils.intToSortableBytes(maxY, b, 2 * BYTES);
    NumericUtils.intToSortableBytes(maxX, b, 3 * BYTES);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    Relation eastRelation = compareBBoxToRangeBBox(this.bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
    if (this.crossesDateline() && eastRelation == Relation.CELL_OUTSIDE_QUERY) {
      return compareBBoxToRangeBBox(this.west, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
    }

    return eastRelation;
  }

  /** static utility method to compare a bbox with a range of triangles (just the bbox of the triangle collection) */
  protected static Relation compareBBoxToRangeBBox(final byte[] bbox,
                                                   int minXOffset, int minYOffset, byte[] minTriangle,
                                                   int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    // check bounding box (DISJOINT)
    if (FutureArrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) > 0 ||
        FutureArrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, BYTES, 2 * BYTES) < 0 ||
        FutureArrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) > 0 ||
        FutureArrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 0, BYTES) < 0) {
      return Relation.CELL_OUTSIDE_QUERY;
    }

    if (FutureArrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
        FutureArrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
        FutureArrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES) >= 0 &&
        FutureArrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
      return Relation.CELL_INSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  /** returns true if the query matches the encoded triangle */
  @Override
  protected boolean queryMatches(byte[] t, int[] scratchTriangle) {
    // decode indexed triangle
    LatLonShape.decodeTriangle(t, scratchTriangle);

    int aY = scratchTriangle[0];
    int aX = scratchTriangle[1];
    int bY = scratchTriangle[2];
    int bX = scratchTriangle[3];
    int cY = scratchTriangle[4];
    int cX = scratchTriangle[5];

    if (queryRelation == LatLonShape.QueryRelation.WITHIN) {
      return queryContainsTriangle(aX, aY, bX, bY, cX, cY);
    }
    return queryMatches(aX, aY, bX, bY, cX, cY);
  }

  private boolean queryContainsTriangle(int ax, int ay, int bx, int by, int cx, int cy) {
    if (this.crossesDateline() == true) {
      return bboxContainsTriangle(ax, ay, bx, by, cx, cy, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
          || bboxContainsTriangle(ax, ay, bx, by, cx, cy, this.minX, MAX_LON_ENCODED, this.minY, this.maxY);
    }
    return bboxContainsTriangle(ax, ay, bx, by, cx, cy, minX, maxX, minY, maxY);
  }

  /** static utility method to check if a bounding box contains a point */
  private static boolean bboxContainsPoint(int x, int y, int minX, int maxX, int minY, int maxY) {
    return (x < minX || x > maxX || y < minY || y > maxY) == false;
  }

  /** static utility method to check if a bounding box contains a triangle */
  private static boolean bboxContainsTriangle(int ax, int ay, int bx, int by, int cx, int cy,
                                              int minX, int maxX, int minY, int maxY) {
    return bboxContainsPoint(ax, ay, minX, maxX, minY, maxY)
        && bboxContainsPoint(bx, by, minX, maxX, minY, maxY)
        && bboxContainsPoint(cx, cy, minX, maxX, minY, maxY);
  }

  /** instance method to check if query box contains point */
  private boolean queryContainsPoint(int x, int y) {
    if (this.crossesDateline() == true) {
      return bboxContainsPoint(x, y, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
          || bboxContainsPoint(x, y, this.minX, MAX_LON_ENCODED, this.minY, this.maxY);
    }
    return bboxContainsPoint(x, y, this.minX, this.maxX, this.minY, this.maxY);
  }

  protected boolean queryMatches(int aX, int aY, int bX, int bY, int cX, int cY) {
    // 1. query contains any triangle points
    if (queryContainsPoint(aX, aY) || queryContainsPoint(bX, bY) || queryContainsPoint(cX, cY)) {
      return true;
    }

    // compute bounding box of triangle
    int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
    int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
    int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
    int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);

    // 2. check bounding boxes are disjoint
    if (this.crossesDateline() == true) {
      if (boxesAreDisjoint(tMinX, tMaxX, tMinY, tMaxY, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
          && boxesAreDisjoint(tMinX, tMaxX, tMinY, tMaxY, this.minX, MAX_LON_ENCODED, this.minY, this.maxY)) {
        return false;
      }
    } else if (tMaxX < minX || tMinX > maxX || tMinY > maxY || tMaxY < minY) {
      return false;
    }

    // 3. check triangle contains any query points
    if (Tessellator.pointInTriangle(minX, minY, aX, aY, bX, bY, cX, cY)) {
      return true;
    } else if (Tessellator.pointInTriangle(maxX, minY, aX, aY, bX, bY, cX, cY)) {
      return true;
    } else if (Tessellator.pointInTriangle(maxX, maxY, aX, aY, bX, bY, cX, cY)) {
      return true;
    } else if (Tessellator.pointInTriangle(minX, maxY, aX, aY, bX, bY, cX, cY)) {
      return true;
    }

    // 4. last ditch effort: check crossings
    if (queryIntersects(aX, aY, bX, bY, cX, cY)) {
      return true;
    }
    return false;
  }

  /** returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query */
  private static boolean edgeIntersectsBox(int ax, int ay, int bx, int by,
                                           int minX, int maxX, int minY, int maxY) {
    // shortcut: if edge is a point (occurs w/ Line shapes); simply check bbox w/ point
    if (ax == bx && ay == by) {
      return Rectangle.containsPoint(ay, ax, minY, maxY, minX, maxX);
    }

    // shortcut: check if either of the end points fall inside the box
    if (bboxContainsPoint(ax, ay, minX, maxX, minY, maxY)
        || bboxContainsPoint(bx, by, minX, maxX, minY, maxY)) {
      return true;
    }

    // shortcut: check bboxes of edges are disjoint
    if (boxesAreDisjoint(Math.min(ax, bx), Math.max(ax, bx), Math.min(ay, by), Math.max(ay, by),
        minX, maxX, minY, maxY)) {
      return false;
    }

    // shortcut: edge is a point
    if (ax == bx && ay == by) {
      return false;
    }

    // top
    if (orient(ax, ay, bx, by, minX, maxY) * orient(ax, ay, bx, by, maxX, maxY) <= 0 &&
        orient(minX, maxY, maxX, maxY, ax, ay) * orient(minX, maxY, maxX, maxY, bx, by) <= 0) {
      return true;
    }

    // right
    if (orient(ax, ay, bx, by, maxX, maxY) * orient(ax, ay, bx, by, maxX, minY) <= 0 &&
        orient(maxX, maxY, maxX, minY, ax, ay) * orient(maxX, maxY, maxX, minY, bx, by) <= 0) {
      return true;
    }

    // bottom
    if (orient(ax, ay, bx, by, maxX, minY) * orient(ax, ay, bx, by, minX, minY) <= 0 &&
        orient(maxX, minY, minX, minY, ax, ay) * orient(maxX, minY, minX, minY, bx, by) <= 0) {
      return true;
    }

    // left
    if (orient(ax, ay, bx, by, minX, minY) * orient(ax, ay, bx, by, minX, maxY) <= 0 &&
        orient(minX, minY, minX, maxY, ax, ay) * orient(minX, minY, minX, maxY, bx, by) <= 0) {
      return true;
    }
    return false;
  }

  /** returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query */
  private boolean edgeIntersectsQuery(int ax, int ay, int bx, int by) {
    if (this.crossesDateline() == true) {
      return edgeIntersectsBox(ax, ay, bx, by, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
          || edgeIntersectsBox(ax, ay, bx, by, this.minX, MAX_LON_ENCODED, this.minY, this.maxY);
    }
    return edgeIntersectsBox(ax, ay, bx, by, this.minX, this.maxX, this.minY, this.maxY);
  }

  /** returns true if the query intersects the provided triangle (in encoded space) */
  private boolean queryIntersects(int ax, int ay, int bx, int by, int cx, int cy) {
    // check each edge of the triangle against the query
    if (edgeIntersectsQuery(ax, ay, bx, by) ||
        edgeIntersectsQuery(bx, by, cx, cy) ||
        edgeIntersectsQuery(cx, cy, ax, ay)) {
      return true;
    }
    return false;
  }

  /** utility method to check if two boxes are disjoint */
  public static boolean boxesAreDisjoint(final int aMinX, final int aMaxX, final int aMinY, final int aMaxY,
                                          final int bMinX, final int bMaxX, final int bMinY, final int bMaxY) {
    return (aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY);
  }

  public boolean crossesDateline() {
    return minX > maxX;
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o)
        && Arrays.equals(bbox, ((LatLonShapeBoundingBoxQuery)o).bbox)
        && Arrays.equals(west, ((LatLonShapeBoundingBoxQuery)o).west);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Arrays.hashCode(bbox);
    hash = 31 * hash + Arrays.hashCode(west);
    return hash;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    sb.append("Rectangle(lat=");
    sb.append(decodeLatitude(minY));
    sb.append(" TO ");
    sb.append(decodeLatitude(maxY));
    sb.append(" lon=");
    sb.append(decodeLongitude(minX));
    sb.append(" TO ");
    sb.append(decodeLongitude(maxX));
    if (maxX < minX) {
      sb.append(" [crosses dateline!]");
    }
    sb.append(")");
    return sb.toString();
  }
}
