package metroinsight.citadel;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;
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

//sudo mongod --dbpath=/home/sandeep/MetroInsight/installation/mongo-data
//sudo mongod --dbpath=/media/sandeep/2Tb/sandeep/MetroInsight/Codes/Citadel-Sandeep/data

@RunWith(VertxUnitRunner.class)
public class ServerTest {
  
  private Vertx vertx;
  private Integer port;
  String testSrcid;
  String serverip="localhost";
  
  @Before
  public void setUp(TestContext context) throws IOException{

	  ClassLoader cl = ClassLoader.getSystemClassLoader();
	  URL[] urls = ((URLClassLoader)cl).getURLs();
	  for(URL url: urls) {
		  System.out.println(url.getFile());
	  }
    ServerSocket socket = null;
    socket = new ServerSocket(0);
    port = 8080; //socket.getLocalPort();
    socket.close();
    
    DeploymentOptions options = new DeploymentOptions()
        .setConfig(new JsonObject().put("http.port", port)
            );
    System.out.println("Deploy options: ");
    System.out.println(options.toJson());
    vertx = Vertx.vertx();
    vertx.deployVerticle(RestApiVerticle.class.getName(),
    //vertx.deployVerticle(MainVerticle.class.getName(),
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
    			context.assertTrue(body.toJsonArray().size() > 0);
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
    JsonObject jo = new JsonObject();
    jo.put("query", metadataJo);
    final String json = Json.encodePrettily(jo);
    final String length = Integer.toString(json.length());
    vertx.createHttpClient().post(port, serverip, "/api/point")
    	.putHeader("content-type", "application/json")
    	.putHeader("content-length",  length)
    	.handler(response -> {
    		context.assertEquals(response.statusCode(), 201);
    		async.complete();
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
  //@Test
  public void testInsertData(TestContext context) {
    final Async async = context.async();
    String uuid = "90fb26f6-4449-482b-87be-83e5741d213e"; 
  	JsonObject query = new JsonObject();
  	JsonObject datum= new JsonObject();
  	JsonArray data = new JsonArray();
  	Double lng = 65.345;
  	Double lat = 30.345;
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
  	data.put("lat_min", 31);
  	data.put("lat_max", 31.5);
  	data.put("lng_min", 60.0);
  	data.put("lng_max", 60.4);
  	data.put("timestamp_min", 1388534400000L);
  	data.put("timestamp_max", 1389312000000L);
  	query.put("query",data);
    String queryStr = Json.encodePrettily(query);
    String length = Integer.toString(queryStr.length());
    vertx.createHttpClient().post(port, serverip, "/api/querydata")
    	.putHeader("content-type", "application/json")
    	.putHeader("content-length",  length)
    	.handler(response -> {
    		context.assertEquals(response.statusCode(), 200);
    		response.bodyHandler(body -> {
    			//System.out.println("Data Query response is:"+body);
    			int dataNum = body.toJsonArray().size();
    			System.out.println("# of data found: " + Integer.toString(dataNum));
    			context.assertTrue(dataNum > 0);
    			async.complete();
    		});
    		
    	})
    	.write(queryStr)
    	.end();
    
	
	 
  }
  
}
