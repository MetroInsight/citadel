package metroinsight.citadel.data.impl;

import static metroinsight.citadel.common.Util.cds2json;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.model.Datapoint;

public class GeomesaHbase {
  DataStore dataStore = null;
  static String simpleFeatureTypeName = "MetroInsight";
  static SimpleFeatureBuilder featureBuilder = null;
  static GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
  Vertx vertx = null;
  
  public GeomesaHbase (Vertx vertx) {
    this.vertx = vertx;
    geomesa_initialize();
  }
  
  public GeomesaHbase() {
    geomesa_initialize();

  }
  
  public void geomesa_initialize() {
      if (dataStore == null) {
        Map<String, Serializable> parameters = new HashMap<>();
        parameters.put("bigtable.table.name", "Geomesa");

        // DataStoreFinder is from Geotools, returns an indexed datastore if one is
        // available.
        try {
          dataStore = DataStoreFinder.getDataStore(parameters);
          SimpleFeatureType simpleFeatureType = createSimpleFeatureType();
          dataStore.createSchema(simpleFeatureType);
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(1);
        }
        System.out.println("Geomesa connected");
    } // end if

  }

  public void geomesa_initialize_backup() {
      if (dataStore == null) {
        Map<String, Serializable> parameters = new HashMap<>();
        parameters.put("bigtable.table.name", "Geomesa");
//        parameters.put("geomesa.ignore.dtg", true);

        // DataStoreFinder is from Geotools, returns an indexed datastore if one is
        // available.
        
        try {
          dataStore = DataStoreFinder.getDataStore(parameters);
        } catch (Exception e) {
          e.printStackTrace();
        }

        SimpleFeatureType simpleFeatureType = null;
        try {
        // establish specifics concerning the SimpleFeatureType to store
          simpleFeatureType = createSimpleFeatureType();
        } catch (Exception e) {
          e.printStackTrace();
        }

        // write Feature-specific metadata to the destination table in HBase
        // (first creating the table if it does not already exist); you only
        // need
        // to create the FeatureType schema the *first* time you write any
        // Features
        // of this type to the table
        // System.out.println("Creating feature-type (schema): " +
        // simpleFeatureTypeName);
        try {
          dataStore.createSchema(simpleFeatureType);
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.out.println("Geomesa connected");
    } // end if

  }

  static SimpleFeatureType createSimpleFeatureType_dep() throws SchemaException {
    /*
     * We use the DataUtilities class from Geotools to create a FeatureType that
     * will describe the data
     * 
     */
    SimpleFeatureType simpleFeatureType = DataUtilities.createType(simpleFeatureTypeName,
        "point_loc:Point:srid=4326," +// a Geometry attribute: Point type
        "uuid:String," +// a String attribute
        "value:String," +// a String attribute
        "date:Date"// a date attribute for time
    );
    return simpleFeatureType;
  }
  
  static SimpleFeatureType createSimpleFeatureType() throws SchemaException {
    SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
    b.setName(simpleFeatureTypeName);
    b.add("loc", Geometry.class, 4326);
    b.setDefaultGeometry("loc");
    b.add("uuid", String.class);
    b.add("date", Date.class);
    b.add("value", String.class); // TODO: Should this be String? Can't perform value-based query.
    SimpleFeatureType sft = b.buildFeatureType();
    sft.getUserData().put("geomesa.mixed.geometries", "true");
    // TODO: Below should work but does not.
    //       When featureWriter is generrated, "Could not read table name from metadata for index xz2:1"
    //sft.getDescriptor("uuid").getUserData().put("index", "join");
    //sft.getDescriptor("uuid").getUserData().put("cardinality", "high");
    //sft.getUserData().put("geomesa.xz.precision", 14); // Default is 12. Experimental.
    return sft;
  }
  
  Coordinate[] convListToCoordinates(List<List<Double>> cds) {
    Coordinate[] cdArray = new Coordinate[cds.size()];
    for (int i=0; i<cds.size(); i++) {
      cdArray[i] = new Coordinate(cds.get(i).get(0), cds.get(i).get(1));
    }
    return cdArray;
  }

  void createNewFeatures(JsonArray data) throws IOException {

    FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriterAppend(simpleFeatureTypeName, Transaction.AUTO_COMMIT);

    try {
      for (int i = 0; i < data.size(); i++) {
        JsonObject datum = data.getJsonObject(i);
        Datapoint dp = datum.mapTo(Datapoint.class);  // TODO: Note that this may take time. Needed?
        String geometryType = dp.getGeometryType();
        Coordinate[] cds = convListToCoordinates(dp.getCoordinates());
        SimpleFeature newFeature = writer.next();
        if (geometryType.equals("point")) {
          Point point = geometryFactory.createPoint(cds[0]);
          newFeature.setAttribute("loc", point);
        }
        else if (geometryType.equals("line")) {
          LineString line = geometryFactory.createLineString(cds);
          newFeature.setAttribute("loc", line);
        }
        else if (geometryType.equals("polygon")) {
          Polygon polygon = geometryFactory.createPolygon(cds);
          newFeature.setAttribute("loc", polygon);
        }
        else {
          throw new java.lang.RuntimeException("Only Point is supported for geometry type.");
        }
        newFeature.setAttribute("uuid", dp.getUuid());
        newFeature.setAttribute("value", dp.getValue());
        newFeature.setAttribute("date", new Date(dp.getTimestamp()));
        writer.write();
      }
      // accumulate this new feature in the collection
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      writer.close();
    }
  }

  
  public void geomesa_insertData(JsonArray data) {
    geomesa_insertData(data, rh -> {
      if (rh.failed()) {
        System.out.println(rh.cause());
      }
    });

  }// end function
	
  static JsonArray queryFeatures_Box_Lat_Lng(DataStore dataStore, String geomField, double x0, double y0, double x1,
      double y1) throws CQLException, IOException {

    // construct a (E)CQL filter from the search parameters,
    // and use that as the basis for the query
    String cqlGeometry = "BBOX(" + geomField + ", " + x0 + ", " + y0 + ", " + x1 + ", " + y1 + ")";
    Filter cqlFilter = CQL.toFilter(cqlGeometry);

    Query query = new Query(simpleFeatureTypeName, cqlFilter);

    // submit the query, and get back an iterator over matching features
    FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
    FeatureIterator featureItr = featureSource.getFeatures(query).features();

    JsonArray ja = new JsonArray();

    // loop through all results
    while (featureItr.hasNext()) {
      Feature feature = featureItr.next();
      try {
        JsonObject Data = new JsonObject();
        Data.put("uuid", feature.getProperty("uuid").getValue());
        Date date = (Date) feature.getProperty("date").getValue();
        Data.put("timestamp", date.getTime());
        Point point = (Point) feature.getProperty("point_loc").getValue();
        Coordinate cd = point.getCoordinates()[0];// since it a single point
        Data.put("lat", cd.x);
        Data.put("lng", cd.y);
        Data.put("value", feature.getProperty("value").getValue());
        ja.add(Data);
      } catch (Exception e) {
        e.printStackTrace();
      }

    }
    featureItr.close();

    return ja;
  }// end function

	
  public JsonArray queryFeatures_Box_Lat_Lng_Time_Range(String geomField, String dateField, Double lat_min,
      Double lng_min, Double lat_max, Double lng_max, long timestamp_min, long timestamp_max, List<String> uuids) throws Exception {

    JsonArray ja = new JsonArray();
    try {
      // construct a (E)CQL filter from the search parameters,
      // and use that as the basis for the query
      String cqlGeometry = "BBOX(" + geomField + ", " + lng_min + ", " + lat_min + ", " + lng_max + ", " + lat_max + ")";
//      String cqlGeometry = "BBOX(" + geomField + ", " + lat_min + ", " + lng_min + ", " + lat_max + ", " + lng_max + ")";
      Date datemin=new Date(Long.valueOf(timestamp_min));
      Date datemax=new Date(Long.valueOf(timestamp_max));

      SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      String date1 = format.format(datemin);
      String date2 = format.format(datemax);

      String cqlDates = "(" + dateField + " during " + date1+"/" + date2+")";
      String filter = cqlGeometry+" AND "+cqlDates;

      Iterator<String> uuidIter = uuids.iterator();
      String uuid;
      String uuidQuery = "";
      while (uuidIter.hasNext()) {
        uuid = uuidIter.next();
        uuidQuery += "OR uuid = '" + uuid + "' ";
      }

      if (!uuidQuery.isEmpty()) {
        uuidQuery = uuidQuery.substring(2);
        filter = filter + " AND (" + uuidQuery + ")";
      }

      Filter cqlFilter = CQL.toFilter(filter);
      Query query = new Query(simpleFeatureTypeName, cqlFilter);
      /*This line force the geomesa to evaluate the bounding box very accurately*/
      query.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.FALSE);

      // submit the query, and get back an iterator over matching features
      FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
      FeatureIterator<SimpleFeature> featureItr = featureSource.getFeatures(query).features();

      // loop through all results
      while (featureItr.hasNext()) {
        Feature feature = null;
        feature = featureItr.next();
        JsonObject Data = new JsonObject();
        Data.put("uuid", feature.getProperty("uuid").getValue());
        Date date = (Date) feature.getProperty("date").getValue();
        Data.put("timestamp", date.getTime());
        Geometry loc = (Geometry) feature.getProperty("loc").getValue();
        String geometryType = loc.getGeometryType();
        Coordinate[] cds = loc.getCoordinates();
        Data.put("coordinates", cds2json(cds)); 
        Data.put("value", feature.getProperty("value").getValue());
        Data.put("geometryType", geometryType);
        ja.add(Data);	
      }
      featureItr.close();

    }//end try
    catch(Exception e){
      throw e;
    }//end catch
    return ja;
  }//end function

  private JsonArray queryFeatures_Box_Lat_Lng_Time_Range_deprecated(String geomField, String dateField,
      Double lat_min, Double lng_min, Double lat_max, Double lng_max, long timestamp_min, long timestamp_max,
      List<String> uuids) {
    JsonArray ja = new JsonArray();

    try {
      // construct a (E)CQL filter from the search parameters,
      // and use that as the basis for the query
      String cqlGeometry = "BBOX(" + geomField + ", " + lng_min + ", " + lat_min + ", " + lng_max + ", " + lat_max
          + ")";
      Date datemin = new Date(Long.valueOf(timestamp_min));
      Date datemax = new Date(Long.valueOf(timestamp_max));

      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      String date1 = format.format(datemin);
      String date2 = format.format(datemax);

      String cqlDates = "(" + dateField + " during " + date1 + "/" + date2 + ")";
      String filter = cqlGeometry + " AND " + cqlDates;
      Iterator<String> uuidIter = uuids.iterator();
      String uuid;
      String uuidQuery = "";
      while (uuidIter.hasNext()) {
        uuid = uuidIter.next();
        uuidQuery += "OR uuid = '" + uuid + "' ";
      }

      if (!uuidQuery.isEmpty()) {
        uuidQuery = uuidQuery.substring(2);
        filter = filter + " AND (" + uuidQuery + ")";
      }
      Filter cqlFilter = CQL.toFilter(filter);
      
      Query query = new Query(simpleFeatureTypeName, cqlFilter);


      // submit the query, and get back an iterator over matching features
      FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(query,
          Transaction.AUTO_COMMIT);

      // loop through all results
      while (reader.hasNext()) {
        Feature feature = reader.next();

        try {
          JsonObject data = new JsonObject();
          data.put("uuid", feature.getProperty("uuid").getValue());
          Date date = (Date) feature.getProperty("date").getValue();
          data.put("timestamp", date.getTime());
          Point point = (Point) feature.getProperty("point_loc").getValue();
          Coordinate cd = point.getCoordinates()[0];// since it a single point
          JsonArray coordinate = new JsonArray();
          coordinate.add(cd.x);
          coordinate.add(cd.y);
          JsonArray coordinates = new JsonArray();
          coordinates.add(coordinate);
          data.put("value", Double.parseDouble((String) feature.getProperty("value").getValue()));
          data.put("geometryType", "point");
          data.put("coordinates", coordinates);
          ja.add(data);
        } catch (Exception e) {
          throw e;
        }

      }
      reader.close();

    } // end try
    catch (Exception e) {
      e.printStackTrace();
    } // end catch
    return ja;
  }// end function
	
  public JsonArray Query_Box_Lat_Lng(double lat_min, double lat_max, double lng_min, double lng_max) {
    try {

      if (dataStore == null) {
        geomesa_initialize();
      }

      // query a few Features from this table
      JsonArray result = queryFeatures_Box_Lat_Lng(dataStore, "point_loc", lat_min, lng_min, lat_max, lng_max);

      return result;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;

  }// end function

  private JsonArray Query_Box_Lat_Lng_Time_Range(Double lat_min, Double lat_max, Double lng_min, Double lng_max,
      long timestamp_min, long timestamp_max, List<String> uuids) {
    try {

      if (dataStore == null) {
        geomesa_initialize();
      }

      // query a few Features from this table
      // System.out.println("Submitting query in Query_Box_Lat_Lng_Time_Range
      // GeomesaHbase ");
      // the point_loc and date should be part of the config
      //JsonArray result = queryFeatures_Box_Lat_Lng_Time_Range("point_loc", "date", lat_min, lng_min, lat_max,
      JsonArray result = queryFeatures_Box_Lat_Lng_Time_Range("loc", "date", lat_min, lng_min, lat_max, //TODO: Just for testing. Roll back!!!
          lng_max, timestamp_min, timestamp_max, uuids);

      return result;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }// end function
	
  public void geomesa_insertData(JsonArray data, Handler<AsyncResult<Void>> rh) {
    try {
      if (dataStore == null) {
        geomesa_initialize();
      }

      // create new features locally, and add them to this table
      //FeatureCollection featureCollection = createNewFeatures(simpleFeatureType, data);
      createNewFeatures(data);
      //insertFeatures_new(dataStore, featureCollection);
      
      rh.handle(Future.succeededFuture());
    } catch (Exception e) {
      rh.handle(Future.failedFuture(e));
    }

  }// end function


  public void Query_Box_Lat_Lng(double lat_min, double lat_max, double lng_min, double lng_max,
      Handler<AsyncResult<JsonArray>> resultHandler) {
    JsonArray result = new JsonArray();
    try {
      result = Query_Box_Lat_Lng(lat_min, lat_max, lng_min, lng_max);
      resultHandler.handle(Future.succeededFuture(result));
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));// in this case the result is empty jsonarray
    }
  }// end function

  public void Query_Box_Lat_Lng_Time_Range(Double lat_min, Double lat_max, Double lng_min, Double lng_max,
      long timestamp_min, long timestamp_max, List<String> uuids, Handler<AsyncResult<JsonArray>> resultHandler) {
    JsonArray result = null;
    try {
      result = Query_Box_Lat_Lng_Time_Range(lat_min, lat_max, lng_min, lng_max, timestamp_min, timestamp_max, uuids);
      resultHandler.handle(Future.succeededFuture(result));
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));// in this case the result is empty jsonarray
    }
  }

  static void insertFeatures_new(DataStore dataStore, FeatureCollection featureCollection) throws IOException {
    FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(simpleFeatureTypeName, Transaction.AUTO_COMMIT);
   // copy new features in
    SimpleFeatureIterator iterator = (SimpleFeatureIterator) featureCollection.features();
    while (iterator.hasNext()) {
        SimpleFeature feature = iterator.next();
        SimpleFeature newFeature = writer.next(); // new blank feature
        newFeature.setAttributes(feature.getAttributes());
        writer.write();
    }
  }
  
  static void insertFeatures(DataStore dataStore, FeatureCollection featureCollection)
      throws IOException {
    FeatureStore featureStore = (FeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);
    featureStore.addFeatures(featureCollection);
  }

}// end GeomesaHbase class
