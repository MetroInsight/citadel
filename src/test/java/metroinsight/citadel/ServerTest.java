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
  	JsonObject query = new JsonObject();
  	JsonObject datum= new JsonObject();
  	JsonArray data = new JsonArray();
  	Double lng = -117.231221;
  	Double lat = 32.881454;
  	datum.put("uuid", uuid);
  	datum.put("timestamp", 1499813708623L);
  	datum.put("value", 15);
  	datum.put("geometryType", "point");
  	ArrayList<ArrayList<Double>> coordinates = new ArrayList<ArrayList<Double>>();
  	ArrayList<Double> coordinate = new ArrayList<Double>();
  	coordinate.add(lng);
  	coordinate.add(lat);
  	coordinates.add(coordinate);
  	datum.put("coordinates", coordinates);
  	data.add(datum);
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
          context.assertTrue(body.toJsonObject().getJsonArray("results").size() > 0);
          async.complete();
          });
    	})
      .write(queryStr)
      .end();
  }
  
}
