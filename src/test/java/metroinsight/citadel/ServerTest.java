package metroinsight.citadel;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;;

@RunWith(VertxUnitRunner.class)
public class ServerTest {
  
  private Vertx vertx;
  private Integer port;
  String testUuid;
  String serverip="localhost";
  
  @Before
  public void setUp(TestContext context) throws IOException{
    ServerSocket socket = null;
    socket = new ServerSocket(0);
    port =8080; //socket.getLocalPort();
    socket.close();
    
    DeploymentOptions options = new DeploymentOptions()
        .setConfig(new JsonObject().put("http.port", port)
            );
    vertx = Vertx.vertx();
    vertx.deployVerticle(RestApiVerticle.class.getName(),
        options,
        context.asyncAssertSuccess());
    
    }
  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  //@Test
  public void testMyApplication(TestContext context){
    final Async async = context.async();
    
    vertx.createHttpClient().getNow(port,  serverip, "/",
        response -> {
          response.handler(body -> {
            context.assertTrue(body.toString().contains("Hello"));
            async.complete();
          });
        });
  }
  
  @Test
  public void testQueryPoint(TestContext context) {
    final Async async = context.async();
  	JsonObject query = new JsonObject();
  	query.put("query", (new JsonObject()).put("pointType", "temp"));
    String queryStr = Json.encodePrettily(query);
    String length = Integer.toString(queryStr.length());
    vertx.createHttpClient().post(port, "localhost", "/api/query")
    	.putHeader("content-type", "application/json")
    	.putHeader("content-length",  length)
    	.handler(response -> {
    		context.assertEquals(response.statusCode(), 200);
    		response.bodyHandler(body -> {
    			context.assertTrue(body.toJsonObject().getJsonArray("results").size() > 0);
    			async.complete();
    		});
    	})
    	.write(queryStr)
    	.end();
  }
  
  @Test
  public void testCreateSensor(TestContext context){
    final Async async = context.async();
    JsonObject metadataJo = new JsonObject();
    metadataJo.put("pointType",  "temp");
    metadataJo.put("unit",  "F");
    final String json = Json.encodePrettily(metadataJo);
    final String length = Integer.toString(json.length());
    vertx.createHttpClient().post(port, serverip, "/api/point")
    	.putHeader("content-type", "application/json")
    	.putHeader("content-length",  length)
    	.handler(response -> {
    		context.assertEquals(response.statusCode(), 201);
    		response.bodyHandler(body -> {
    			context.assertTrue(body.toJsonObject().getBoolean("success"));
    			async.complete();
    		});
    	})
    	.write(json)
    	.end();
  }
/*
  @Test
  public void testGetSensor(TestContext context) {
    final Async async = context.async();
    String uuid = "90fb26f6-4449-482b-87be-83e5741d213e"; //TODO: This needs to be auto-gen later.
  	JsonObject query = new JsonObject();
  	query.put("query", (new JsonObject()).put("uuid", uuid));
    String queryStr = Json.encodePrettily(query);
    String length = Integer.toString(queryStr.length());
    vertx.createHttpClient().post(port, "localhost", "/api/query")
    	.putHeader("content-type", "application/json")
    	.putHeader("content-length",  length)
    	.handler(response -> {
    		context.assertEquals(response.statusCode(), 200);
    		response.bodyHandler(body -> {
    			System.out.println("response is:"+body);
    			context.assertTrue(body.toJsonArray().size() > 0);
    			async.complete();
    		});
    	})
    	.write(queryStr)
    	.end();
  }
*/
  @Test
  public void testInsertData(TestContext context) {
    final Async async = context.async();
    String uuid = "90fb26f6-4449-482b-87be-83e5741d213e"; 
    String uuid2 = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
  	JsonObject query = new JsonObject();

  	JsonArray data = new JsonArray();

  	JsonObject datum1 = new JsonObject();
  	Double lng = -117.231221;
  	Double lat = 32.881454;
  	datum1.put("uuid", uuid);
  	datum1.put("timestamp", 1499813708623L);
  	datum1.put("value", 15);
  	datum1.put("geometryType", "point");
  	ArrayList<ArrayList<Double>> coordinates = new ArrayList<ArrayList<Double>>();
  	ArrayList<Double> coordinate = new ArrayList<Double>();
  	coordinate.add(lng);
  	coordinate.add(lat);
  	coordinates.add(coordinate);
  	datum1.put("coordinates", coordinates);
  	data.add(datum1);

  	JsonObject datum2 = new JsonObject();
  	lng = -117.231230;
  	lat = 32.881450;
  	datum2.put("uuid", uuid2);
  	datum2.put("timestamp", 1499813708600L);
  	datum2.put("value", 20);
  	datum2.put("geometryType", "point");
  	ArrayList<ArrayList<Double>> coordinates2 = new ArrayList<ArrayList<Double>>();
  	ArrayList<Double> coordinate2 = new ArrayList<Double>();
  	coordinate2.add(lng);
  	coordinate2.add(lat);
  	coordinates2.add(coordinate2);
  	datum2.put("coordinates", coordinates2);
  	data.add(datum2);
  	
  	query.put("data",data);
    String queryStr = Json.encodePrettily(query);
    String length = Integer.toString(queryStr.length());
    vertx.createHttpClient().post(port, serverip, "/api/data")
    	.putHeader("content-type", "application/json")
    	.putHeader("content-length",  length)
    	.handler(response -> {
    		context.assertEquals(response.statusCode(), 201);
    			async.complete();
    	})
    	.write(queryStr)
    	.end();
  }
  
  @Test
  public void testQueryData(TestContext context) {
    final Async async = context.async();
  	JsonObject query = new JsonObject();
  	JsonObject data = new JsonObject();
  	data.put("lat_min", 32.868623);
  	data.put("lat_max", 32.893202);
  	data.put("lng_min", -117.244438);
  	data.put("lng_max", -117.214398);
  	data.put("timestamp_min", 1499813707623L);
  	data.put("timestamp_max", 1499813709623L);
  	query.put("query",data);
    String queryStr = Json.encodePrettily(query);
    String length = Integer.toString(queryStr.length());
    vertx.createHttpClient().post(port, serverip, "/api/querydata")
      .putHeader("content-type", "application/json")
      .putHeader("content-length",  length)
      .handler(response -> {
        context.assertEquals(response.statusCode(), 200);
        response.bodyHandler(body -> {
          System.out.println("Data Query response is:"+body);
          context.assertTrue(body.toJsonObject().getJsonArray("results").size() > 0);
          async.complete();
          });
    	})
      .write(queryStr)
      .end();
  }
  
  private Boolean check_only_one_uuid(JsonArray data, String uuid) {
    JsonObject datum;
    System.out.println(data);
    for (int i=0; i<data.size(); i++) {
      datum = data.getJsonObject(i);
      if (!datum.getString("uuid").equals(uuid)) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testQueryDataOnlyUUID(TestContext context) {
    final Async async = context.async();
  	JsonObject query = new JsonObject();
  	JsonObject data = new JsonObject();
    String uuid = "90fb26f6-4449-482b-87be-83e5741d213e"; 
    JsonArray uuids = new JsonArray();
    uuids.add(uuid);
  	data.put("uuids", uuids);
  	query.put("query",data);
    String queryStr = Json.encodePrettily(query);
    String length = Integer.toString(queryStr.length());
    vertx.createHttpClient().post(port, serverip, "/api/querydata")
      .putHeader("content-type", "application/json")
      .putHeader("content-length",  length)
      .handler(response -> {
        context.assertEquals(response.statusCode(), 200);
        response.bodyHandler(body -> {
          System.out.println("Data Query response is:"+body);
          JsonArray results = body.toJsonObject().getJsonArray("results");
          context.assertTrue(results.size() > 0);
          context.assertTrue(check_only_one_uuid(results, uuid));
          async.complete();
          });
    	})
      .write(queryStr)
      .end();
  }
  
}
