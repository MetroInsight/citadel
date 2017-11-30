package metroinsight.citadel.metadata;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import metroinsight.citadel.metadata.impl.MongoService;
import metroinsight.citadel.model.Metadata;

public class MetadataRestApi {

  private MongoClient mongoClient;
  private MetadataService metadataService;
  
  public MetadataRestApi (Vertx vertx) {
    metadataService = new MongoService (vertx);
  }
  
  public void queryPoint(RoutingContext rc) {
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
    metadataService.queryPoint(q, ar -> {
    	if (ar.failed()) {
      	System.out.println(ar.cause().getMessage());
    	} else {
    		String resultStr = ar.result().toString();
    		String length = Integer.toString(resultStr.length());
    		rc.response()
    		.putHeader("content-TYPE", "application/json; charset=utf=8")
    		.putHeader("content-length",  length)
      	.setStatusCode(200)
      	.write(resultStr)
    		.end();
    	}
    	});
  }
  
  public void getPoint(RoutingContext rc) {
  	String uuid = rc.request().getParam("uuid");
  	if (uuid == null) {
  		rc.response().setStatusCode(400).end();
  	} else {
  		metadataService.getPoint(uuid, ar -> {
  		if (ar.failed()) {
  			System.out.println(ar.cause().getMessage());
  		} else {
  			rc.response()
  			.putHeader("content-TYPE", "application/json; charset=utf=8")
  			.setStatusCode(200)
  			.end(ar.result().toString());
  		}
  		});
  	}
  }
  
  public void createPoint(RoutingContext rc) {
	
	System.out.println("In createPoint: MetadataRestApi");
    JsonObject body = rc.getBodyAsJson();
    // Get the query as JSON.
    JsonObject q = (JsonObject)(body.getValue("query"));
    System.out.println("Sensor is:"+q);
    // Call createPoint in metadataService asynchronously.
    metadataService.createPoint(q, ar -> { 
      // ar is a result object created in metadataService.createPoint
      // We pass what to do with the result in this format.
    	if (ar.failed()) {
    	  // if the service is failed
    	  // TODO: add response here.
      	System.out.println(ar.cause().getMessage());
    	} else {
    	  // Construct response object and complete with "end".
    	  JsonObject result = new JsonObject();
    	  result.put("result", "SUCCESS");
    	  result.put("uuid", ar.result().toString());
    		rc.response()
    		  .putHeader("content-TYPE", "application/text; charset=utf=8")
    		  .setStatusCode(201)
    		  .end(result.toString());
    	}
    	});
  }

}
