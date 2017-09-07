package metroinsight.citadel.data;

import static metroinsight.citadel.common.RestApiTemplate.getDefaultResponse;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import metroinsight.citadel.data.impl.GeomesaService;
import metroinsight.citadel.model.BaseContent;


public class DataRestApi {

  private DataService dataService;
  Vertx vertx;
  
  public DataRestApi (Vertx vertx) {
    dataService = new GeomesaService(vertx);
    this.vertx = vertx;
  }
  
  public DataRestApi () {
    dataService = new GeomesaService();
  }
  
  /*
  
  public void queryData(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
    dataService.queryData(q, ar -> {
      String cStr;
      String cLen;
    	if (ar.failed()) {
    	  content.setReason(ar.cause().getMessage());
    	  resp.setStatusCode(400);
    	} else {
    		System.out.println("Suceeded in DataRestAPI Query Data");
    		//String resultStr = ar.result().toString();
    		System.out.println("Query Results are:" + ar.result().toString());
    		//String length = Integer.toString(resultStr.length());
    		resp.setStatusCode(200);
    		content.setSucceess(true);
    		content.setResults(ar.result());
    	}
    	cStr = content.toString();
    	cLen = Integer.toBinaryString(cStr.length());
    	resp
    	  .putHeader("content-length", cLen)
    	  .write(cStr)
    	  .end();
    	});
  }
   */
  public void queryData(RoutingContext rc) {
    JsonObject q = rc.getBodyAsJson().getJsonObject("query");
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    dataService.queryData(q, ar -> {
      String cStr;
      String cLen;
      if (ar.failed()) {
        content.setReason(ar.cause().getMessage());
        cStr = content.toString();
        cLen = Integer.toString(cStr.length());
        resp.setStatusCode(400);
      } else {
        content.setSucceess(true);
        content.setResults(ar.result());
        cStr = content.toString();
        cLen = Integer.toString(cStr.length());
        resp
        .setStatusCode(200);
      }
      resp
        .putHeader("content-length", cLen)
        .write(cStr);
      });
  }
  
  public void getData(RoutingContext rc) {
    
  }
  
  
  public void insertData(RoutingContext rc) {
    JsonObject body = rc.getBodyAsJson();
    // Get the query as JSON.
    JsonArray q = body.getJsonArray("data");
    // Call createPoint in metadataService asynchronously.
    HttpServerResponse resp = getDefaultResponse(rc);
    dataService.insertData(q, ar -> { 
      BaseContent content = new BaseContent();
      String cStr;
      String cLen;
      if (ar.failed()) {
        content.setReason(ar.cause().getMessage());
        resp.setStatusCode(400);
      } else {
        resp.setStatusCode(201);
        content.setSucceess(true);
      }
      cStr = content.toString();
      cLen = Integer.toString(cStr.length());
      resp
        .putHeader("content-length", cLen)
        .write(cStr);
      });
    return;
  }

  public void querySimpleBbox(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    JsonObject bbox = rc.getBodyAsJson().getJsonObject("query");
    dataService.querySimpleBbox(bbox.getDouble("min_lng"), bbox.getDouble("max_lng"), bbox.getDouble("min_lat"), bbox.getDouble("max_lat"), rh -> {
      if (rh.succeeded()) {
        content.setResults(rh.result());
        resp.setStatusCode(200);
      } else {
        content.setReason(rh.cause().getMessage());
        resp.setStatusCode(400);
      }
      String cStr = content.toString();
      String cLen = Integer.toString(cStr.length());
      resp
        .putHeader("content-length", cLen)
        .write(cStr);
    });
    
  }

}
