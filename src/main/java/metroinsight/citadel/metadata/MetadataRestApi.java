package metroinsight.citadel.metadata;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import metroinsight.citadel.common.RestApiTemplate;
import metroinsight.citadel.metadata.impl.MongoService;
import metroinsight.citadel.model.BaseContent;

public class MetadataRestApi extends RestApiTemplate {

  private MetadataService metadataService;
  
  public MetadataRestApi (Vertx vertx) {
    metadataService = new MongoService (vertx);
  }
  
  public void queryPoint(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
    metadataService.queryPoint(q, ar -> {
    	if (ar.failed()) {
    	  content.setReason(ar.cause().getMessage());
    	  resp.setStatusCode(400);
    	} else {
    	  content.setSucceess(true);;
    	  content.setResults(ar.result());
    	  resp.setStatusCode(200);
    	}
    	String cStr = content.toString();
    	String cLen = Integer.toString(cStr.length());
    	resp
    	  .putHeader("content-length", cLen)
    	  .write(cStr);
    	});
  }
  
  public void getPoint(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
  	String uuid = rc.request().getParam("uuid");
  	if (uuid == null) {
  	  content.setReason(sensorNotFound);
  	  String cStr = content.toString();
  	  String cLen = Integer.toString(cStr.length());
  	  resp.setStatusCode(400)
    	  .putHeader("content-length", cLen)
    	  .write(cStr);
  	} else {
  		metadataService.getPoint(uuid, ar -> {
  		if (ar.failed()) {
  		  content.setReason(ar.cause().getMessage());
  		  resp.setStatusCode(400);
  		} else {
  		  JsonArray pointResult = new JsonArray();
  		  pointResult.add(ar.result());
  		  resp.setStatusCode(200);
  		  content.setSucceess(true);
  		  content.setResults(pointResult);
  		}
  	  String cStr = content.toString();
  	  String cLen = Integer.toString(cStr.length());
  	  resp.putHeader("content-length", cLen)
    	  .write(cStr);
  		});
  	}
  }
  
  public void createPoint(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    JsonObject point = rc.getBodyAsJson();
    // Get the query as JSON.
    // Call createPoint in metadataService asynchronously.
    metadataService.createPoint(point, ar -> { 
      // ar is a result object created in metadataService.createPoint
      // We pass what to do with the result in this format.
      String cStr;
      String cLen;
    	if (ar.failed()) {
    	  // if the service is failed
    	  resp.setStatusCode(400);
    	  content.setReason(ar.cause().getMessage());
    	  cStr = content.toString();
    	} else {
    	  // Construct response object.
    	  resp.setStatusCode(201);
    	  JsonObject pointCreateContent = new JsonObject();
    	  pointCreateContent.put("success", true);
    	  pointCreateContent.put("uuid", ar.result().toString());
    	  cStr = pointCreateContent.toString();
    	}
    	cLen = Integer.toString(cStr.length());
  	  resp.putHeader("content-length", cLen)
  	    .write(cStr);
    	});
  }

}
