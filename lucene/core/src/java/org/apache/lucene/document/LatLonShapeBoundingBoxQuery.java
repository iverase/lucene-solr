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

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

import static java.lang.Integer.BYTES;
import static org.apache.lucene.geo.GeoEncodingUtils.MAX_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.MIN_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoUtils.MIN_LON_INCL;
import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * Finds all previously indexed geo shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.LatLonShape#createIndexableFields} added per document.
 **/
final class LatLonShapeBoundingBoxQuery extends ShapeQuery {
  private final Rectangle rectangle;
  private final EncodedRectangle encodedRectangle;

  LatLonShapeBoundingBoxQuery(String field, QueryRelation queryRelation, Rectangle rectangle) {
    super(field, queryRelation);
    this.rectangle = rectangle;
    this.encodedRectangle = new EncodedRectangle(rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    if (queryRelation == QueryRelation.INTERSECTS || queryRelation == QueryRelation.DISJOINT) {
      return encodedRectangle.intersectRangeBBox(minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
    }
    return encodedRectangle.relateRangeBBox(minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
  }


  @Override
  protected boolean queryIntersects(byte[] t, ShapeField.DecodedTriangle scratchTriangle) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    switch (scratchTriangle.type) {
      case POINT: {
        return encodedRectangle.contains(scratchTriangle.aX, scratchTriangle.aY);
      }
      case LINE: {
        int aY = scratchTriangle.aY;
        int aX = scratchTriangle.aX;
        int bY = scratchTriangle.bY;
        int bX = scratchTriangle.bX;
        return encodedRectangle.intersectsLine(aX, aY, bX, bY);
      }
      case TRIANGLE: {
        int aY = scratchTriangle.aY;
        int aX = scratchTriangle.aX;
        int bY = scratchTriangle.bY;
        int bX = scratchTriangle.bX;
        int cY = scratchTriangle.cY;
        int cX = scratchTriangle.cX;
        return encodedRectangle.intersectsTriangle(aX, aY, bX, bY, cX, cY);
      }
      default: throw new IllegalArgumentException("Unsupported triangle type :[" + scratchTriangle.type + "]");
    }
  }

  @Override
  protected boolean queryContains(byte[] t, ShapeField.DecodedTriangle scratchTriangle) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    switch (scratchTriangle.type) {
      case POINT: {
        return encodedRectangle.contains(scratchTriangle.aX, scratchTriangle.aY);
      }
      case LINE: {
        int aY = scratchTriangle.aY;
        int aX = scratchTriangle.aX;
        int bY = scratchTriangle.bY;
        int bX = scratchTriangle.bX;
        return encodedRectangle.containsLine(aX, aY, bX, bY);
      }
      case TRIANGLE: {
        int aY = scratchTriangle.aY;
        int aX = scratchTriangle.aX;
        int bY = scratchTriangle.bY;
        int bX = scratchTriangle.bX;
        int cY = scratchTriangle.cY;
        int cX = scratchTriangle.cX;
        return encodedRectangle.containsTriangle(aX, aY, bX, bY, cX, cY);
      }
      default: throw new IllegalArgumentException("Unsupported triangle type :[" + scratchTriangle.type + "]");
    }
  }

  @Override
  protected Component2D.WithinRelation queryWithin(byte[] t, ShapeField.DecodedTriangle scratchTriangle) {
    if (encodedRectangle.crossesDateline()) {
      throw new IllegalArgumentException("withinTriangle is not supported for rectangles crossing the date line");
    }
    // decode indexed triangle
    ShapeField.decodeTriangle(t, scratchTriangle);

    switch (scratchTriangle.type) {
      case POINT: {
        return Component2D.WithinRelation.DISJOINT;
      }
      case LINE: {
        return encodedRectangle.withinLine(scratchTriangle.aX, scratchTriangle.aY, scratchTriangle.ab,
            scratchTriangle.bX, scratchTriangle.bY);
      }
      case TRIANGLE: {
        return encodedRectangle.withinTriangle(scratchTriangle.aX, scratchTriangle.aY, scratchTriangle.ab,
            scratchTriangle.bX, scratchTriangle.bY, scratchTriangle.bc,
            scratchTriangle.cX, scratchTriangle.cY, scratchTriangle.ca);
      }
      default: throw new IllegalArgumentException("Unsupported triangle type :[" + scratchTriangle.type + "]");
    }
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && rectangle.equals(((LatLonShapeBoundingBoxQuery)o).rectangle);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + rectangle.hashCode();
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
    sb.append(rectangle.toString());
    return sb.toString();
  }

  /** Holds spatial logic for a bounding box that works in the encoded space */
  private static class EncodedRectangle {
    protected final byte[] bbox;
    private final byte[] west;
    protected final int minX;
    protected final int maxX;
    protected final int minY;
    protected final int maxY;

    EncodedRectangle(double minLat, double maxLat, double minLon, double maxLon) {
      this.bbox = new byte[4 * BYTES];
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
        this.west = new byte[4 * BYTES];
        this.minX = minXenc;
        this.maxX = maxXenc;
        encode(MIN_LON_ENCODED, this.maxX, this.minY, this.maxY, this.west);
        encode(this.minX, MAX_LON_ENCODED, this.minY, this.maxY, this.bbox);
      } else {
        // encodeLongitudeCeil may cause minX to be > maxX iff
        // the delta between the longitude < the encoding resolution
        if (minXenc > maxXenc) {
          minXenc = maxXenc;
        }
        this.west = null;
        this.minX = minXenc;
        this.maxX = maxXenc;
        encode(this.minX, this.maxX, this.minY, this.maxY, bbox);
      }
    }

    /**
     * encodes a bounding box into the provided byte array
     */
    private static void encode(final int minX, final int maxX, final int minY, final int maxY, byte[] b) {
      if (b == null) {
        b = new byte[4 * BYTES];
      }
      NumericUtils.intToSortableBytes(minY, b, 0);
      NumericUtils.intToSortableBytes(minX, b, BYTES);
      NumericUtils.intToSortableBytes(maxY, b, 2 * BYTES);
      NumericUtils.intToSortableBytes(maxX, b, 3 * BYTES);
    }

    private boolean crossesDateline() {
      return minX > maxX;
    }

    /**
     * compare this to a provided range bounding box
     **/
    Relation relateRangeBBox(int minXOffset, int minYOffset, byte[] minTriangle,
                             int maxXOffset, int maxYOffset, byte[] maxTriangle) {
      Relation eastRelation = compareBBoxToRangeBBox(this.bbox,
          minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      if (this.crossesDateline() && eastRelation == Relation.CELL_OUTSIDE_QUERY) {
        return compareBBoxToRangeBBox(this.west, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      }
      return eastRelation;
    }

    /**
     * intersects this to a provided range bounding box
     **/
    Relation intersectRangeBBox(int minXOffset, int minYOffset, byte[] minTriangle,
                                int maxXOffset, int maxYOffset, byte[] maxTriangle) {
      Relation eastRelation = intersectBBoxWithRangeBBox(this.bbox,
          minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      if (this.crossesDateline() && eastRelation == Relation.CELL_OUTSIDE_QUERY) {
        return intersectBBoxWithRangeBBox(this.west, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      }
      return eastRelation;
    }

    /**
     * static utility method to compare a bbox with a range of triangles (just the bbox of the triangle collection)
     **/
    private static Relation compareBBoxToRangeBBox(final byte[] bbox,
                                                   int minXOffset, int minYOffset, byte[] minTriangle,
                                                   int maxXOffset, int maxYOffset, byte[] maxTriangle) {
      // check bounding box (DISJOINT)
      if (disjoint(bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle)) {
        return Relation.CELL_OUTSIDE_QUERY;
      }

      if (Arrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
          Arrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
          Arrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES) >= 0 &&
          Arrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
        return Relation.CELL_INSIDE_QUERY;
      }

      return Relation.CELL_CROSSES_QUERY;
    }

    /**
     * static utility method to compare a bbox with a range of triangles (just the bbox of the triangle collection)
     * for intersection
     **/
    private static Relation intersectBBoxWithRangeBBox(final byte[] bbox,
                                                       int minXOffset, int minYOffset, byte[] minTriangle,
                                                       int maxXOffset, int maxYOffset, byte[] maxTriangle) {
      // check bounding box (DISJOINT)
      if (disjoint(bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle)) {
        return Relation.CELL_OUTSIDE_QUERY;
      }

      if (Arrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
          Arrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES) >= 0) {
        if (Arrays.compareUnsigned(maxTriangle, minXOffset, minXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
            Arrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
        if (Arrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
            Arrays.compareUnsigned(maxTriangle, minYOffset, minYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
      }

      if (Arrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
          Arrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
        if (Arrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
            Arrays.compareUnsigned(minTriangle, maxYOffset, maxYOffset + BYTES, bbox, 0, BYTES) >= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
        if (Arrays.compareUnsigned(minTriangle, maxXOffset, maxXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
            Arrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES) >= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
      }

      return Relation.CELL_CROSSES_QUERY;
    }

    /**
     * static utility method to check a bbox is disjoint with a range of triangles
     **/
    private static boolean disjoint(final byte[] bbox,
                                    int minXOffset, int minYOffset, byte[] minTriangle,
                                    int maxXOffset, int maxYOffset, byte[] maxTriangle) {
      return Arrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) > 0 ||
          Arrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, BYTES, 2 * BYTES) < 0 ||
          Arrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) > 0 ||
          Arrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 0, BYTES) < 0;
    }


    /**
     * Checks if the rectangle contains the provided point
     **/
    boolean contains(int x, int y) {
      if (y < minY || y > maxY) {
        return false;
      }
     if (crossesDateline()) {
       return (x > maxX && x < minX) == false;
     } else {
       return (x > maxX || x < minX) == false;
     }
    }

    /**
     * Checks if the rectangle intersects the provided triangle
     **/
    boolean intersectsTriangle(int aX, int aY, int bX, int bY, int cX, int cY) {
      // 1. query contains any triangle points
      if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
        return true;
      }

      // compute bounding box of triangle
      int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
      int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
      int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
      int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);

      if (boxesAreDisjoint(tMinX, tMaxX, tMinY, tMaxY, minX, maxX, minY, maxY)) {
        return false;
      }
      return Component2D.pointInTriangle(tMinX, tMaxX, tMinY, tMaxY, minX, minY, aX, aY, bX, bY, cX, cY) ||
             edgeIntersectsQuery(aX, aY, bX, bY) ||
             edgeIntersectsQuery(bX, bY, cX, cY) ||
             edgeIntersectsQuery(cX, cY, aX, aY);
    }

    boolean intersectsLine(int aX, int aY, int bX, int bY) {
      if (contains(aX, aY) || contains(bX, bY)) {
        return true;
      }

      int tMinX = StrictMath.min(aX, bX);
      int tMaxX = StrictMath.max(aX, bX);
      int tMinY = StrictMath.min(aY, bY);
      int tMaxY = StrictMath.max(aY, bY);

      // 2. check bounding boxes are disjoint
      if (boxesAreDisjoint(tMinX, tMaxX, tMinY, tMaxY, minX, maxX, minY, maxY)) {
        return false;
      }
      return edgeIntersectsQuery(aX, aY, bX, bY);
    }

    /**
     * Checks if the rectangle contains the provided triangle
     **/
    boolean containsTriangle(int aX, int aY, int bX, int bY, int cX, int cY) {
      if (crossesDateline()) {
        if (Component2D.containsPoint(aX, aY, minX, MAX_LON_ENCODED, this.minY, this.maxY) &&
            Component2D.containsPoint(bX, bY, minX, MAX_LON_ENCODED, this.minY, this.maxY) &&
            Component2D.containsPoint(cX, cY, minX, MAX_LON_ENCODED, this.minY, this.maxY)) {
          return true;
        }
        return Component2D.containsPoint(aX, aY, MIN_LON_ENCODED, maxX, this.minY, this.maxY) &&
               Component2D.containsPoint(bX, bY, MIN_LON_ENCODED, maxX, this.minY, this.maxY) &&
               Component2D.containsPoint(cX, cY, MIN_LON_ENCODED, maxX, this.minY, this.maxY);
      }
      return contains(aX, aY) && contains(bX, bY) && contains(cX, cY);
    }

    boolean containsLine(int aX, int aY, int bX, int bY) {
      if (crossesDateline()) {
        if (Component2D.containsPoint(aX, aY, minX, MAX_LON_ENCODED, this.minY, this.maxY) &&
            Component2D.containsPoint(bX, bY, minX, MAX_LON_ENCODED, this.minY, this.maxY)) {
          return true;
        }
        return Component2D.containsPoint(aX, aY, MIN_LON_ENCODED, maxX, this.minY, this.maxY) &&
            Component2D.containsPoint(bX, bY, MIN_LON_ENCODED, maxX, this.minY, this.maxY);
      }
      return contains(aX, aY) && contains(bX, bY);
    }

    /**
     * Returns the Within relation to the provided triangle
     */
    Component2D.WithinRelation withinLine(int ax, int ay, boolean ab, int bx, int by) {
      if (ab == true && edgeIntersectsBox(ax, ay, bx, by, minX, maxX, minY, maxY) == true) {
          return Component2D.WithinRelation.NOTWITHIN;
      }
      return Component2D.WithinRelation.DISJOINT;
    }

    /**
     * Returns the Within relation to the provided triangle
     */
    Component2D.WithinRelation withinTriangle(int ax, int ay, boolean ab, int bx, int by, boolean bc, int cx, int cy, boolean ca) {
      // Points belong to the shape so if points are inside the rectangle then it cannot be within.
      if (contains(ax, ay) || contains(bx, by) || contains(cx, cy)) {
        return Component2D.WithinRelation.NOTWITHIN;
      }

      // Compute bounding box of triangle
      int tMinX = StrictMath.min(StrictMath.min(ax, bx), cx);
      int tMaxX = StrictMath.max(StrictMath.max(ax, bx), cx);
      int tMinY = StrictMath.min(StrictMath.min(ay, by), cy);
      int tMaxY = StrictMath.max(StrictMath.max(ay, by), cy);
      // Bounding boxes disjoint?
      if (boxesAreDisjoint(tMinX, tMaxX, tMinY, tMaxY, minX, maxX, minY, maxY)) {
        return Component2D.WithinRelation.DISJOINT;
      }
      // Points belong to the shape so if points are inside the rectangle then it cannot be within.
      if (contains(ax, ay) || contains(bx, by) || contains(cx, cy)) {
        return Component2D.WithinRelation.NOTWITHIN;
      }
      // If any of the edges intersects an edge belonging to the shape then it cannot be within.
      Component2D.WithinRelation relation = Component2D.WithinRelation.DISJOINT;
      if (edgeIntersectsBox(ax, ay, bx, by, minX, maxX, minY, maxY) == true) {
        if (ab == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }
      if (edgeIntersectsBox(bx, by, cx, cy, minX, maxX, minY, maxY) == true) {
        if (bc == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }

      if (edgeIntersectsBox(cx, cy, ax, ay, minX, maxX, minY, maxY) == true) {
        if (ca == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }
      // Check if shape is within the triangle
      if (relation == Component2D.WithinRelation.CANDIDATE ||
          Component2D.pointInTriangle(minX, maxX, minY, maxY, minX, minY, ax, ay, bx, by, cx, cy)) {
        return Component2D.WithinRelation.CANDIDATE;
      }
      return relation;
    }

    /**
     * returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query
     */
    private boolean edgeIntersectsQuery(int ax, int ay, int bx, int by) {
      if (this.crossesDateline() == true) {
        return edgeIntersectsBox(ax, ay, bx, by, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
            || edgeIntersectsBox(ax, ay, bx, by, this.minX, MAX_LON_ENCODED, this.minY, this.maxY);
      }
      return edgeIntersectsBox(ax, ay, bx, by, this.minX, this.maxX, this.minY, this.maxY);
    }

    /**
     * returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query
     */
    private static boolean edgeIntersectsBox(int ax, int ay, int bx, int by,
                                             int minX, int maxX, int minY, int maxY) {

      if ( Math.max(ax, bx) < minX || Math.min(ax, bx) > maxX || Math.min(ay, by) > maxY || Math.max(ay, by) < minY) {
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



    /**
     * utility method to check if two boxes are disjoint
     */
    private static boolean boxesAreDisjoint(final int aMinX, final int aMaxX, final int aMinY, final int aMaxY,
                                            final int bMinX, final int bMaxX, final int bMinY, final int bMaxY) {
      if (aMaxY < bMinY || aMinY > bMaxY) {
        return true;
      }
      if (bMinX > bMaxX) { // crosses dateline
        return aMinX > bMaxX && aMaxX < bMinX;
      } else {
        return aMinX > bMaxX || aMaxX < bMinX;
      }
    }
  }
}
