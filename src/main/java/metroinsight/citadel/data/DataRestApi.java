package metroinsight.citadel.data;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import metroinsight.citadel.data.impl.GeomesaService;


public class DataRestApi {

	 private DataService dataService;
  
  public DataRestApi () {
	  dataService=new GeomesaService();
  }
  
  public void queryData(RoutingContext rc) {
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
    dataService.queryData(q, ar -> {
    	if (ar.failed()) {
      	System.out.println(ar.cause().getMessage());
    	} else {
    		System.out.println("Suceeded in DataRestAPI Query Data");
    		String resultStr = ar.result().toString();
    		System.out.println("Query Results are:"+resultStr);
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
  
  
  public void insertData(RoutingContext rc) {
    JsonObject body = rc.getBodyAsJson();
    // Get the query as JSON.
    JsonArray q = body.getJsonArray("data");
    // Call createPoint in metadataService asynchronously.
    dataService.insertData(q, ar -> { 
      // ar is a result object created in metadataService.createPoint
      // We pass what to do with the result in this format.
    	if (ar.failed()) {
    	  // if the service is failed
    	  // TODO: add response here.
      	System.out.println(ar.cause().getMessage());
    	} else {
    	  System.out.println("Suceeded in DataRestAPI insertData");
    	  JsonObject result = new JsonObject();
    	  result.put("result", "SUCCESS");
    		rc.response()
    		  .putHeader("content-TYPE", "application/text; charset=utf=8")
    		  .setStatusCode(201)
    		  .end(result.toString());
    	}
    	});
  }

}
