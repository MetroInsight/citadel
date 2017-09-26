package metroinsight.citadel;

import java.util.ArrayList;
import java.util.Date;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.data.impl.GeomesaService;

public class GeomesaDebug {

  static GeomesaService gs;
  static Vertx vertx;
  
  static void queryTest() {
  	JsonObject query = new JsonObject();
  	query.put("lat_min", 32.868623);
    query.put("lat_max", 32.893202);
    query.put("lng_min", -117.244438);
    query.put("lng_max", -117.214398);
  	query.put("timestamp_min", 1388534400000L);
  	query.put("timestamp_max", 1389312000000L);
  	System.out.println("0");
  	gs.queryData(query, rh->{
  	  if (rh.succeeded()) {
  	    JsonArray res = rh.result();
  	    System.out.println(res);
  	    System.out.println("Y");
  	  } else {
  	    rh.cause().printStackTrace();
  	    System.out.println("X");
  	  }
  	});
  }
  
  static void insertTest() {
    String uuid = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
  	JsonObject query = new JsonObject();

  	JsonArray data = new JsonArray();

  	JsonObject datum1 = new JsonObject();
  	Double lng = -117.231221;
  	Double lat = 32.881454;
  	datum1.put("uuid", uuid);
  	Long timestamp = 1499813708623L;
  	System.out.println(timestamp);
  	System.out.println(new Date(timestamp));
  	datum1.put("timestamp", timestamp);
  	datum1.put("value", 15);
  	datum1.put("geometryType", "point");
  	ArrayList<ArrayList<Double>> coordinates = new ArrayList<ArrayList<Double>>();
  	ArrayList<Double> coordinate = new ArrayList<Double>();
  	coordinate.add(lng);
  	coordinate.add(lat);
  	coordinates.add(coordinate);
  	datum1.put("coordinates", coordinates);
  	data.add(datum1);
  	gs.insertData(data, rh -> {
  	  if (rh.succeeded()) {
  	    System.out.println("Insertion success");
  	  } else {
  	    rh.cause().printStackTrace();
  	  }
  	  
  	});
  }
  
  public static void main(String[] args) {	
    System.out.println("Set up");
    vertx = Vertx.vertx();
    gs = new GeomesaService(vertx);
    insertTest();
    //queryTest();
    
  }

}
