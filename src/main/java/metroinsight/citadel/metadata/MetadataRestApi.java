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
  	String srcid = rc.request().getParam("srcid");
  	if (srcid == null) {
  		rc.response().setStatusCode(400).end();
  	} else {
  		metadataService.getPoint(srcid, ar -> {
  		if (ar.failed()) {
  			System.out.println(ar.cause().getMessage());
  		} else {
  			Metadata resultMetadata = ar.result();
  			rc.response()
  			.putHeader("content-TYPE", "application/json; charset=utf=8")
  			.setStatusCode(200)
  			.end(ar.result().toString());
  		}
  		});
  	}
  }
  
  public void createPoint(RoutingContext rc) {
    JsonObject body = rc.getBodyAsJson();
    JsonObject q = (JsonObject)(body.getValue("query")); // TODO: Validate if this is working
    metadataService.createPoint(q, ar -> {
    	if (ar.failed()) {
      	System.out.println(ar.cause().getMessage());
    	} else {
    	  JsonObject result = rc.getBodyAsJson();
    	  result.put("result", "SUCCESS");
    	  result.put("srcid", ar.result().toString());
    		rc.response()
    		.putHeader("content-TYPE", "application/text; charset=utf=8")
      	.setStatusCode(201)
    		.end(result.toString());
    	}
    	});
  }

}
