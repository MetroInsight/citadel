package metroinsight.citadel.data;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import metroinsight.citadel.common.RestApiTemplate;
import metroinsight.citadel.data.impl.GeomesaService;
import metroinsight.citadel.model.BaseContent;


public class DataRestApi extends RestApiTemplate {

  private DataService dataService;
  
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
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
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
        .write(cStr)
        .end();
      });
  }
  
  
  public void insertData(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    JsonObject body = rc.getBodyAsJson();
    // Get the query as JSON.
    JsonArray q = body.getJsonArray("data");
    // Call createPoint in metadataService asynchronously.
    dataService.insertData(q, ar -> { 
      // ar is a result object created in metadataService.createPoint
      // We pass what to do with the result in this format.
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
        .write(cStr)
        .end();

    	
    	});
  }

}
