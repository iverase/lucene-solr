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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.Tessellator.Triangle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/**
 * An indexed shape utility class.
 * <p>
 * {@link Polygon}'s are decomposed into a triangular mesh using the {@link Tessellator} utility class
 * Each {@link Triangle} is encoded and indexed as a multi-value field.
 * <p>
 * Finding all shapes that intersect a range (e.g., bounding box) at search time is efficient.
 * <p>
 * This class defines static factory methods for common operations:
 * <ul>
 *   <li>{@link #createIndexableFields(String, Polygon)} for matching polygons that intersect a bounding box.
 *   <li>{@link #newBoxQuery newBoxQuery()} for matching polygons that intersect a bounding box.
 * </ul>

 * <b>WARNING</b>: Like {@link LatLonPoint}, vertex values are indexed with some loss of precision from the
 * original {@code double} values (4.190951585769653E-8 for the latitude component
 * and 8.381903171539307E-8 for longitude).
 * @see PointValues
 * @see LatLonDocValuesField
 *
 * @lucene.experimental
 */
public class LatLonShape {
  public static final int BYTES = LatLonPoint.BYTES;

  protected static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(7, 4, BYTES);
    TYPE.freeze();
  }

  // no instance:
  private LatLonShape() {
  }

  /** create indexable fields for polygon geometry */
  public static Field[] createIndexableFields(String fieldName, Polygon polygon) {
    // the lionshare of the indexing is done by the tessellator
    List<Triangle> tessellation = Tessellator.tessellate(polygon);
    List<LatLonTriangle> fields = new ArrayList<>();
    for (Triangle t : tessellation) {
      fields.add(new LatLonTriangle(fieldName, t));
    }
    return fields.toArray(new Field[fields.size()]);
  }

  /** create indexable fields for line geometry */
  public static Field[] createIndexableFields(String fieldName, Line line) {
    int numPoints = line.numPoints();
    List<LatLonTriangle> fields = new ArrayList<>(numPoints - 1);

    // create "flat" triangles
    double aLat, bLat, aLon, bLon, temp;
    for (int i = 0, j = 1; j < numPoints; ++i, ++j) {
      aLat = line.getLat(i);
      aLon = line.getLon(i);
      bLat = line.getLat(j);
      bLon = line.getLon(j);
      if (aLat > bLat) {
        temp = aLat;
        aLat = bLat;
        bLat = temp;
        temp = aLon;
        aLon = bLon;
        bLon = temp;
      } else if (aLat == bLat) {
        if (aLon > bLon) {
          temp = aLat;
          aLat = bLat;
          bLat = temp;
          temp = aLon;
          aLon = bLon;
          bLon = temp;
        }
      }
      fields.add(new LatLonTriangle(fieldName, aLat, aLon, bLat, bLon, aLat, aLon));
    }
    return fields.toArray(new Field[fields.size()]);
  }

  /** create indexable fields for point geometry */
  public static Field[] createIndexableFields(String fieldName, double lat, double lon) {
    return new Field[] {new LatLonTriangle(fieldName, lat, lon, lat, lon, lat, lon)};
  }

  /** create a query to find all polygons that intersect a defined bounding box
   **/
  public static Query newBoxQuery(String field, QueryRelation queryRelation, double minLatitude, double maxLatitude, double minLongitude, double maxLongitude) {
    return new LatLonShapeBoundingBoxQuery(field, queryRelation, minLatitude, maxLatitude, minLongitude, maxLongitude);
  }

  /** create a query to find all polygons that intersect a provided linestring (or array of linestrings)
   *  note: does not support dateline crossing
   **/
  public static Query newLineQuery(String field, QueryRelation queryRelation, Line... lines) {
    return new LatLonShapeLineQuery(field, queryRelation, lines);
  }

  /** create a query to find all polygons that intersect a provided polygon (or array of polygons)
   *  note: does not support dateline crossing
   **/
  public static Query newPolygonQuery(String field, QueryRelation queryRelation, Polygon... polygons) {
    return new LatLonShapePolygonQuery(field, queryRelation, polygons);
  }

  /** polygons are decomposed into tessellated triangles using {@link org.apache.lucene.geo.Tessellator}
   * these triangles are encoded and inserted as separate indexed POINT fields
   */
  private static class LatLonTriangle extends Field {

    LatLonTriangle(String name, double aLat, double aLon, double bLat, double bLon, double cLat, double cLon) {
      super(name, TYPE);
      setTriangleValue((aLon), (aLat), (bLon), (bLat), (cLon), (cLat));
    }

    LatLonTriangle(String name, Triangle t) {
      super(name, TYPE);
      setTriangleValue(t.getLon(0), t.getLat(0), t.getLon(1), t.getLat(1), t.getLon(2), t.getLat(2));
    }


    public void setTriangleValue(double aX, double aY, double bX, double bY, double cX, double cY) {
      final byte[] bytes;

      if (fieldsData == null) {
        bytes = new byte[7 * BYTES];
        fieldsData = new BytesRef(bytes);
      } else {
        bytes = ((BytesRef) fieldsData).bytes;
      }
      encodeTriangle(bytes, aY, aX, bY, bX, cY, cX);
    }
  }


  /** Query Relation Types **/
  public enum QueryRelation {
    INTERSECTS, WITHIN, DISJOINT
  }

  /**
   * A triangle is encoded using 6 points and a extra point with encoded information of how to reconstruct it. The information
   * is as follows:
   * first bit 1 if the triangle points correspond to one of vertices of the bounding box, else two vertices.
   * if first bit is 0, then the info is the following
   * second bit 0, third bit 0 then the matching vertices is the minLat, minLon
   * second bit 1, third bit 0 then the matching vertices is the maxLat, minLon
   * second bit 0, third bit 1 then the matching vertices is the minLat, maxLon
   * second bit 1, third bit 1 then the matching vertices is the maxLat, maxLon
   * The fourth bit represent the orientation (CW or CCW) of the triangle.
   *
   * else if first bit is 1:
   * second bit 1: matching vertices are [minLat, minLon] and [maxLat, maxLon]
   * second bit 0: matching vertices are [minLat, maxLon] and [maxLat, minLon]
   * The third bit represent the orientation (CW or CCW) of the triangle.
   */
  public static void encodeTriangle(byte[] bytes, double aLat, double aLon, double bLat, double bLon, double cLat, double cLon) {
    assert bytes.length == 7 * BYTES;

    int ccw = GeoUtils.orient(aLon, aLat, bLon, bLat, cLon, cLat);
    int aX = GeoEncodingUtils.encodeLongitude(aLon);
    int bX = GeoEncodingUtils.encodeLongitude(bLon);
    int cX = GeoEncodingUtils.encodeLongitude(cLon);
    int aY = GeoEncodingUtils.encodeLatitude(aLat);
    int bY = GeoEncodingUtils.encodeLatitude(bLat);
    int cY = GeoEncodingUtils.encodeLatitude(cLat);

    int minX = StrictMath.min(aX, StrictMath.min(bX, cX));
    int minY = StrictMath.min(aY, StrictMath.min(bY, cY));
    int maxX = StrictMath.max(aX, StrictMath.max(bX, cX));
    int maxY = StrictMath.max(aY, StrictMath.max(bY, cY));

    int bits =0;
    int y = 0;
    int x = 0;
    if (minY == aY && minX == aX) {
      if (maxY == bY && maxX == bX) {
        y = cY;
        x = cX;
        bits |= (1 << 0);
        bits |= (1 << 1);
        bits |= (1 << 2);
      } else if (maxY == cY && maxX == cX) {
        y = bY;
        x = bX;
        bits |= (1 << 0);
        bits |= (1 << 1);
      } else {
        if (ccw == -1) {
          y = cY;
          x = bX;
          bits |= (1 << 3);
        } else {
          y = bY;
          x = cX;
        }
      }
    } else if (minY == bY && minX == bX) {
      if (maxY == cY && maxX == cX) {
        y = aY;
        x = aX;
        bits |= (1 << 0);
        bits |= (1 << 1);
        bits |= (1 << 2);
      } else if (maxY == aY && maxX == aX) {
        y = cY;
        x = cX;
        bits |= (1 << 0);
        bits |= (1 << 1);
      } else {
        if (ccw == -1) {
          y = aY;
          x = cX;
          bits |= (1 << 3);
        } else {
          y = cY;
          x = aX;
        }
      }
    } else if (minY == cY && minX == cX) {
      if (maxY == aY && maxX == aX) {
        y = bY;
        x = bX;
        bits |= (1 << 0);
        bits |= (1 << 1);
        bits |= (1 << 2);
      } else if (maxY == bY && maxX == bX) {
        y = aY;
        x = aX;
        bits |= (1 << 0);
        bits |= (1 << 1);
      } else {
        if (ccw == -1) {
          y = bY;
          x = aX;
          bits |= (1 << 3);
        } else {
          y = aY;
          x = bX;
        }
      }
    } else if (minY == aY && maxX == aX) {
      if (maxY == bY && minX == bX) {
        y = cY;
        x = cX;
        bits |= (1 << 0);
        bits |= (1 << 2);
      } else if (maxY == cY && minX == cX) {
        y = bY;
        x = bX;
        bits |= (1 << 0);
      } else {
        bits |= (1 << 2);
        if (ccw == -1) {
          y = bY;
          x = cX;
          bits |= (1 << 3);
        } else {
          y = cY;
          x = bX;
        }
      }
    } else if (minY == bY && maxX == bX) {
      if (maxY == aY && minX == aX) {
        y = cY;
        x = cX;
        bits |= (1 << 0);
      } else if (maxY == cY && minX == cX) {
        y = aY;
        x = aX;
        bits |= (1 << 0);
        bits |= (1 << 2);
      } else {
        bits |= (1 << 2);
        if (ccw == -1) {
          y = cY;
          x = aX;
          bits |= (1 << 3);
        } else {
          y = aY;
          x = cX;
        }
      }
    } else if (minY == cY && maxX == cX) {
      if (maxY == aY && minX == aX) {
        y = bY;
        x = bX;
        bits |= (1 << 0);
        bits |= (1 << 2);
      } else if (maxY == bY && minX == bX) {
        y = aY;
        x = aX;
        bits |= (1 << 0);
      } else {
        bits |= (1 << 2);
        if (ccw == -1) {
          y = aY;
          x = bX;
          bits |= (1 << 3);
        } else {
          y = bY;
          x = aX;
        }
      }
    } else if (maxY == aY && maxX == aX) {
      bits |= (1 << 1);
      bits |= (1 << 2);
      if (ccw == -1) {
        y = cY;
        x = bX;
        bits |= (1 << 3);
      } else {
        y = bY;
        x = cX;
      }
    } else if (maxY == bY && maxX == bX) {
      bits |= (1 << 1);
      bits |= (1 << 2);
      if (ccw == -1) {
        y = aY;
        x = cX;
        bits |= (1 << 3);
      } else {
        y = cY;
        x = aX;
      }
    } else if (maxY == cY && maxX == cX) {
      bits |= (1 << 1);
      bits |= (1 << 2);
      if (ccw == -1) {
        y = bY;
        x = aX;
        bits |= (1 << 3);
      } else {
        y = aY;
        x = bX;
      }
    } else if (maxY == aY && minX == aX) {
      bits |= (1 << 1);
      if (ccw == -1) {
        y = bY;
        x = cX;
        bits |= (1 << 3);
      } else {
        y = cY;
        x = bX;
      }
    } else if (maxY == bY && minX == bX) {
      bits |= (1 << 1);
      if (ccw == -1) {
        y = cY;
        x = aX;
        bits |= (1 << 3);
      } else {
        y = aY;
        x = cX;
      }
    } else if (maxY == cY && minX == cX) {
      bits |= (1 << 1);
      if (ccw == -1) {
        y = aY;
        x = bX;
        bits |= (1 << 3);
      } else {
        y = bY;
        x = aX;
      }
    }

    NumericUtils.intToSortableBytes(minY, bytes, 0);
    NumericUtils.intToSortableBytes(minX, bytes, BYTES);
    NumericUtils.intToSortableBytes(maxY, bytes, 2 * BYTES);
    NumericUtils.intToSortableBytes(maxX, bytes, 3 * BYTES);
    NumericUtils.intToSortableBytes(y, bytes, 4 * BYTES);
    NumericUtils.intToSortableBytes(x, bytes, 5 * BYTES);
    NumericUtils.intToSortableBytes(bits, bytes, 6 * BYTES);
  }

  /**
   * Decode a triangle encoded by {@link LatLonShape#encodeTriangle(byte[], double, double, double, double, double, double)}.
   */
  public static int[] decodeTriangle(byte[] t) {
    int bits = NumericUtils.sortableBytesToInt(t, 6 * LatLonShape.BYTES);
    boolean firstBit  = (bits & 1 << 0) == 1 << 0;
    boolean secondBit = (bits & 1 << 1) == 1 << 1;
    boolean thirdBit  = (bits & 1 << 2) == 1 << 2;
    int[] triangle = new int[6];
    if (firstBit) {
      if (secondBit) {
        if (thirdBit) {
          triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
          triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
          triangle[2] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
          triangle[3] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLon
          triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
          triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
        } else {
          triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
          triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
          triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
          triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
          triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
          triangle[5] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLon
        }
      } else {
        if (thirdBit) {
          triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
          triangle[1] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLon
          triangle[2] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
          triangle[3] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
          triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
          triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
        } else {
          triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
          triangle[1] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLon
          triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
          triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
          triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
          triangle[5] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
        }
      }
    } else {
      boolean fourthBit = (bits & 1 << 3) == 1 << 3;
      if (secondBit) {
        if (thirdBit) {
          if (fourthBit) {
            triangle[0] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
            triangle[1] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLon
            triangle[2] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
            triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
            triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
            triangle[5] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
          } else {
            triangle[0] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
            triangle[1] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLon
            triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
            triangle[3] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
            triangle[4] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
            triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
          }
        } else {
          if (fourthBit) {
            triangle[0] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
            triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
            triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
            triangle[3] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLon
            triangle[4] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
            triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
          } else {
            triangle[0] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
            triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
            triangle[2] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
            triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
            triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
            triangle[5] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLon
          }
        }
      } else {
        if (thirdBit) {
          if (fourthBit) {
            triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
            triangle[1] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLon
            triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
            triangle[3] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
            triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
            triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
          } else {
            triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
            triangle[1] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLon
            triangle[2] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
            triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
            triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
            triangle[5] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
          }
        } else {
          if (fourthBit) {
            triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
            triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
            triangle[2] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
            triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
            triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
            triangle[5] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLat
          } else {
            triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * LatLonShape.BYTES); //minLat
            triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * LatLonShape.BYTES); //minLon
            triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * LatLonShape.BYTES); //lat
            triangle[3] = NumericUtils.sortableBytesToInt(t, 3 * LatLonShape.BYTES); //maxLon
            triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * LatLonShape.BYTES); //maxLat
            triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * LatLonShape.BYTES); //lon
          }
        }
      }
    }
    return triangle;
  }
}
