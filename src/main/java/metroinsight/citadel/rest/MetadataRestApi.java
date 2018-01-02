package metroinsight.citadel.rest;

import java.util.UUID;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.serviceproxy.ProxyHelper;
import metroinsight.citadel.authorization.Authorization_MetaData;
import metroinsight.citadel.common.ErrorMessages;
import metroinsight.citadel.common.RestApiTemplate;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.model.BaseContent;


public class MetadataRestApi extends RestApiTemplate {

  private MetadataService metadataService;
  Vertx vertx;
  
  /*
   * Used to validate user token is valid in every operation on MetaData
   */
  Authorization_MetaData Auth_meta;
  
  public MetadataRestApi (Vertx vertx) {
    metadataService = ProxyHelper.createProxy(MetadataService.class, vertx, MetadataService.ADDRESS);
    this.vertx = vertx;
    
    /*
     * Initializing Auth Metadata
     */
    Auth_meta=new Authorization_MetaData();
    
  }
  
  public void queryPoint(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
   // System.out.println("In query point:"+q);
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
  	  content.setReason(ErrorMessages.SENSOR_NOT_FOUND);
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
  		  pointResult.add(ar.result().toJson());
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
    
    
    JsonObject body = new JsonObject();
    JsonObject point = new JsonObject();
    /*
     * Needed in Case Body is not in json format
     */
    try {
    	body=rc.getBodyAsJson();
    }
    catch(Exception e){
    	e.printStackTrace();
    }
    		 
   
    /*
     * Verifying the Token is present and is valid
     */
    
    try {
    	System.out.println("body is:"+body);
    	
    	//check token and sensor is present
    	if(body.containsKey(Auth_meta.userToken)&&body.containsKey("sensor"))
    	{
    		String userToken = body.getString(Auth_meta.userToken);
    		//check if this token exists in the HBase, and if it exists, what is the userID
       		String userId=Auth_meta.get_userID(userToken);
       		
       		if(!userId.equals(""))//user is present in the system
       		{
       			point =body.getJsonObject("sensor");
       			
       		    //token exists and is linked to the valid userId
      			 String uuid = UUID.randomUUID().toString();
      			 point.put("uuid", uuid);//This is later used by metadataService.createPoint
      			 point.put("userId", userId);//This can be later used by metadataService.createPoint to link a point to userID
      			 
      			
       			 
       			 //original function to insert Point
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
       		    		
       		    		//we succeeded
       		    	     //insert the policy for Owner to default "true", no-space-time constraints
              			 Auth_meta.insert_policy(uuid, userId, "true"); 
              			 
       		    		
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
       			 
       			 
      			 
       		}//end if(!userId.equals(""))
       		else
       		{
       			System.out.println("Token is not Valid");	
       	   		sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
       	   		
       		}//end else
       		
    	
    	}//end if(body.containsKey(Auth_meta.userToken)&&body.containsKey("sensor"))
    	
    	else
    	{
    		System.out.println("In MetadataRestApi: Insert data parameters are missing");
        	sendErrorResponse(resp, 400, "Query parameters are missing");	
    	}
    	
    	
   		
   		
    }//end try
    catch (Exception e)
    {
    	e.printStackTrace();
    }
    
    /*
     * end Verifying the Token is present in the point and is valid
     */
   
    
  }

}
