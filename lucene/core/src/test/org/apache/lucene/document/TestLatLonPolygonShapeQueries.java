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

import java.util.List;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.Rectangle2D;
import org.apache.lucene.geo.Tessellator;

/** random bounding box, line, and polygon query tests for random indexed {@link Polygon} types */
public class TestLatLonPolygonShapeQueries extends BaseLatLonShapeTestCase {

  @Override
  protected ShapeType getShapeType() {
    return ShapeType.POLYGON;
  }

  @Override
  protected Polygon nextShape() {
    Polygon p;
    while (true) {
      // if we can't tessellate; then random polygon generator created a malformed shape
      p = (Polygon)getShapeType().nextShape();
      try {
        Tessellator.tessellate(p);
        return p;
      } catch (IllegalArgumentException e) {
        continue;
      }
    }
  }

  @Override
  protected Field[] createIndexableFields(String field, Object polygon) {
    return LatLonShape.createIndexableFields(field, (Polygon)polygon);
  }

  @Override
  protected Validator getValidator() {
    return new PolygonValidator(this.ENCODER);
  }

  protected static class PolygonValidator extends Validator {
    protected PolygonValidator(Encoder encoder) {
      super(encoder);
    }

    @Override
    public boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape) {
      Polygon p = (Polygon)shape;
      Rectangle2D rectangle2D = Rectangle2D.create(new Rectangle(minLat, maxLat, minLon, maxLon));
      Component2D.WithinRelation withinRelation = Component2D.WithinRelation.DISJOINT;
      List<Tessellator.Triangle> tessellation = Tessellator.tessellate(p);
      for (Tessellator.Triangle t : tessellation) {
        ShapeField.DecodedTriangle decoded = encoder.encodeDecodeTriangle(t.getX(0), t.getY(0), t.isEdgefromPolygon(0),
            t.getX(1), t.getY(1), t.isEdgefromPolygon(1),
            t.getX(2), t.getY(2), t.isEdgefromPolygon(2));
        if (queryRelation == QueryRelation.WITHIN) {
          if (rectangle2D.containsTriangle(decoded.aX, decoded.aY, decoded.bX, decoded.bY, decoded.cX, decoded.cY) == false) {
            return false;
          }
        } else if (queryRelation == QueryRelation.CONTAINS) {
          Component2D.WithinRelation relation = rectangle2D.withinTriangle(decoded.aX, decoded.aY, decoded.ab, decoded.bX, decoded.bY, decoded.bc, decoded.cX, decoded.cY, decoded.ca);
          if (relation == Component2D.WithinRelation.NOTWITHIN) {
            return false;
          } else if (relation == Component2D.WithinRelation.CANDIDATE) {
            withinRelation = Component2D.WithinRelation.CANDIDATE;
          }
        } else {
          if (rectangle2D.intersectsTriangle(decoded.aX, decoded.aY, decoded.bX, decoded.bY, decoded.cX, decoded.cY) == true) {
            return queryRelation == QueryRelation.INTERSECTS;
          }
        }
      }
      if (queryRelation == QueryRelation.CONTAINS) {
        return withinRelation == Component2D.WithinRelation.CANDIDATE;
      }
      return queryRelation != QueryRelation.INTERSECTS;
    }

    @Override
    public boolean testComponentQuery(Component2D query, Object o) {
      Polygon polygon = (Polygon) o;
      if (queryRelation == QueryRelation.CONTAINS) {
        return testWithinQuery(query, LatLonShape.createIndexableFields("dummy", polygon));
      }
      return testComponentQuery(query, LatLonShape.createIndexableFields("dummy", polygon));
    }
  }

  @Nightly
  @Override
  public void testRandomBig() throws Exception {
    doTestRandom(25000);
  }
}
