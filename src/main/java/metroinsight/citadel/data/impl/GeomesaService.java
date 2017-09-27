package metroinsight.citadel.data.impl;

import java.util.ArrayList;
import java.util.Random;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.ServiceDiscovery;
import metroinsight.citadel.data.DataService;

public class GeomesaService implements DataService {
  
  private final Vertx vertx;
  private final ServiceDiscovery discovery;
 
  static GeomesaHbase gmh;

  public GeomesaService(Vertx vertx) {
    this.vertx = vertx;
    this.discovery = null;
    if (gmh==null) {
      gmh = new GeomesaHbase(vertx);
      gmh.geomesa_initialize();
    }
  }
  
  public GeomesaService(Vertx vertx, ServiceDiscovery discovery) {
    this.vertx = vertx;
    this.discovery = discovery;
    if (gmh==null) {
      gmh = new GeomesaHbase(vertx);
      gmh.geomesa_initialize();
    }
  }

  /*
  public GeomesaService() {
	  //initialize the geomesa database
    if(gmh==null) 
    {
 	   gmh = new GeomesaHbase();
 	   gmh.geomesa_initialize();
    }
  }
  */
  
  @Override
  public void insertData(JsonArray data, Handler<AsyncResult<Void>> resultHandler) {
	   // Validate if it complies to the schema. No actual usage
	    // TODO: Need to change this to proper validation instead.
	   // DataPoint dataPoint = data.mapTo(DataPoint.class); 
	    
    // TODO: data schema validation
    gmh.geomesa_insertData(data, resultHandler);
  } 
	  
  @Override
  public void queryDataBox(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
  	
	  try{	  
      double lat_min;
      if(query.containsKey("lat_min"))
        lat_min=query.getDouble("lat_min");
      else
        lat_min = 0;
      double lat_max=query.getDouble("lat_max");
      double lng_min=query.getDouble("lng_min");
      double lng_max=query.getDouble("lng_max");
      gmh.Query_Box_Lat_Lng(lat_min, lat_max, lng_min, lng_max, res -> {
          if (res.succeeded()) {
            JsonArray resultJson = res.result();
            resultHandler.handle(Future.succeededFuture(resultJson));
          } else {
            res.cause().printStackTrace();
          }
        });
	  }
	  catch(Exception e)  {
		  e.printStackTrace();
	  }
  }
  
  @Override
  public void queryData(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
  
    try{
      Double lat_max;
      Double lat_min;
      Double lng_min;
      Double lng_max;
      long timestamp_min;
      long timestamp_max;
      if (query.containsKey("lat_max")) {
        lat_max = query.getDouble("lat_max");
      } else {
        lat_max = lat_default_max;
      }
      if (query.containsKey("lat_min")) {
        lat_min = query.getDouble("lat_min");
      } else {
        lat_min = lat_default_min;
      }
      if (query.containsKey("lng_min")) {
        lng_min = query.getDouble("lng_min");
      } else {
        lng_min = lng_default_min;
      }
      if (query.containsKey("lng_max")) {
        lng_max = query.getDouble("lng_max");
      } else {
        lng_max = lng_default_max;
      }
      
      if (query.containsKey("timestamp_min")) {
        timestamp_min = query.getLong("timestamp_min");
      } else {
        timestamp_min = timestamp_default_min;
      }
      if (query.containsKey("timestamp_max")) {
        timestamp_max = query.getLong("timestamp_max");
      } else {
        timestamp_max = System.currentTimeMillis();
      }
      
      ArrayList<String> uuids = new ArrayList<String>();
      if (query.containsKey("uuids")) {
        JsonArray uuidJsonArray = query.getJsonArray("uuids");
        for (int i=0; i<uuidJsonArray.size(); i++) {
          uuids.add(uuidJsonArray.getString(i));
        }
      }
      
      //query is box and range both, other cases need to be implemented too
      gmh.Query_Box_Lat_Lng_Time_Range(lat_min, lat_max, lng_min, lng_max, timestamp_min, timestamp_max, uuids, res -> {
        if (res.succeeded()) {
          JsonArray resultJson = res.result();
          resultHandler.handle(Future.succeededFuture(resultJson));
          } else {
            res.cause().printStackTrace();
            }
        });
      }//end if
      catch(Exception e){
        resultHandler.handle(Future.failedFuture(e));
      }
    }
  
  /*
   * Geomesa query performs well when the time range is restricted to the range
   * actually present in the data sets, else it is very badly affected with arbitrary time ranges
   * 
   */
  public static void main(String[] args) {

    // testing the fucntionality of Geomesa service:
    // System.setProperty("hadoop.home.dir",
    // "/home/sandeep/metroinsight/installations/hadoop/hadoop-2.8.0");

    GeomesaService GS = new GeomesaService(null);

    // inserting the data points
    int count = 0;// 0*729000;
    String uuid = "uuid1";
    double value_min = 10.0;
    double value_max = 20.0;
    double lat_min = 30.0;
    double lng_min = 60.0;
    double diff_loc = 2.0;
    String geometryType = "point";
    Random random = new Random();
    DateTime MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"));
    Long SECONDS_PER_YEAR = 365L;// 365L * 24L * 60L * 60L;

    for (int i = 0; i < count; i++) {
      double value = value_min + random.nextDouble() * (value_max - value_min);
      // long millis = System.currentTimeMillis();
      DateTime dateTime = MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR));

      long timestamp = dateTime.getMillis();// 1388534500000L;//millis;//note time is used in millisec in the System
      double lat = lat_min + random.nextDouble() * diff_loc;
      double lng = lng_min + random.nextDouble() * diff_loc;
      JsonArray data = new JsonArray();
      JsonObject datum = new JsonObject();
      datum.put("uuid", uuid);
      datum.put("timestamp", timestamp);
      datum.put("value", value);
      ArrayList<ArrayList<Double>> coordinates = new ArrayList<ArrayList<Double>>();
      ArrayList<Double> coordinate = new ArrayList<Double>();
      coordinate.add(lng);
      coordinate.add(lat);
      coordinates.add(coordinate);
      datum.put("geometryType", geometryType);
      datum.put("coordinates", coordinates);
      data.add(datum);

      System.out.println(i + ": Data Point to Insert is:" + data.toString());
      GS.insertData(data, ar -> {
        if (ar.failed()) {
          System.out.println(ar.cause().getMessage());
        } else {
          System.out.println("Insertion done in Main");
        }
      });

    } // end for

    for (int k = 0; k < 2; k++) {
      // query the points just inserted:

      double lat_minq = 31.0;
      double lat_maxq = 32.0;
      double lng_minq = 60.0;
      double lng_maxq = 62.0;

      // long
      // timestamp_min=1388534400000L,timestamp_max=1389312000000L;//1504059232123L;//1389312000000L;
      // 1388534400000
      DateTime dateTime1 = MIN_DATE;// .plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR));
      DateTime dateTime2 = dateTime1.plusSeconds((int) Math.round((SECONDS_PER_YEAR)));
      long timestamp_min = dateTime1.getMillis();
      long timestamp_max = dateTime2.getMillis();// 1420070400000L;//

      JsonObject query = new JsonObject();
      query.put("lat_min", lat_minq);
      query.put("lat_max", lat_maxq);
      query.put("lng_min", lng_minq);
      query.put("lng_max", lng_maxq);
      query.put("timestamp_min", timestamp_min);
      query.put("timestamp_max", timestamp_max);
      long millistart;
      long milliend;

      // millistart = System.currentTimeMillis();

      System.out.println(k + " : Query is:" + query);
      millistart = System.currentTimeMillis();
      GS.queryData(query, ar -> {
        if (ar.failed()) {
          System.out.println(ar.cause().getMessage());
        } else {

          String result = ar.result().toString();

          // System.out.println("Query 2 Results are:"+result);
          JsonArray datarec = new JsonArray(result);
          System.out.println("Result size is: " + datarec.size());

        }
      });
      milliend = System.currentTimeMillis();
      System.out.println(k + " : Query 2 done in Main");

      System.out.println("Time taken is:" + (milliend - millistart));
      System.out.println();

    } // end for loop on query

  }


}
