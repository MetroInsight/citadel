package metroinsight.citadel.data;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import metroinsight.citadel.authorization.Authorization_MetaData;
import metroinsight.citadel.data.impl.GeomesaService;


public class DataRestApi {

	private DataService dataService;
	Authorization_MetaData Auth_meta_data;
	
  public DataRestApi () {
	  dataService=new GeomesaService();
	  Auth_meta_data=new Authorization_MetaData();
  }
  
  public void queryData(RoutingContext rc) {
	  
	System.out.println("In queryData DataRestApi.java");  
	JsonObject body = rc.getBodyAsJson();
	
	/*
     * verify the user Token is valid and used has authority to query Data on provided uuid into the System
     */
    //get_ds_owner_token(String dsId), we also need to check query parameters
    if(body.containsKey("userToken")&&body.containsKey("uuid")&&body.containsKey("query"))
    {
    	String token=body.getString("userToken");
    	String uuid=body.getString("uuid");//uuid is the ds_id
    	String token_owner=Auth_meta_data.get_ds_owner_token(uuid);//this means uuid exists and token also exists
    	
    	if(token_owner.equals(token)) {//verify that owner token is same as token of person querying the data
    		
    		JsonObject q = body.getJsonObject("query");
            System.out.println("Query is:"+q);
            dataService.queryData(q, ar -> {
            	if (ar.failed()) {
              	System.out.println(ar.cause().getMessage());
            	} else {
            		System.out.println("Suceeded in DataRestAPI Query Data");
            		String resultStr = ar.result().toString();
            		//System.out.println("Query Results are:"+resultStr);
            		String length = Integer.toString(resultStr.length());
            		rc.response()
            		.putHeader("content-TYPE", "application/json; charset=utf=8")
            		.putHeader("content-length",  length)
              	.setStatusCode(200)
              	.write(resultStr);
            	}
            	});
        	
    		
    	}//end if(token_owner.equals(token))
    	else
    	{
    		System.out.println("In DataRestApi: Token doesn't have required priveleges");
    	}
    	
    	
    }//end  if(body.containsKey("userToken")&&body.containsKey("uuid"))
    else
    {
    	System.out.println("In DataRestApi: Query parameters are missing");
    }
	
    
  }//end queryData(RoutingContext rc)
  
  
  public void insertData(RoutingContext rc) {
	 
	System.out.println("In insertData DataRestApi.java");
    JsonObject body = rc.getBodyAsJson();
    // Get the query as JSON.
    
    /*
     * verify the user Token is valid and he as authority to insert Data into the System
     */
    //get_ds_owner_token(String dsId)
    if(body.containsKey("userToken")&&body.containsKey("uuid")&&body.containsKey("data"))
    {
    
    	String token=body.getString("userToken");
    	String uuid=body.getString("uuid");
    	String token_owner=Auth_meta_data.get_ds_owner_token(uuid);//this means uuid exists and token also exists
    	
    	if(token_owner.equals(token)) {
    		
		    	JsonArray q = body.getJsonArray("data");
		   	   //JsonObject body = (JsonObject) rc.getBodyAsJson().getValue("query");
		   	    System.out.println("body is:"+body);
		   	   //  JsonArray q = body.getJsonArray("data");
		   	  
		   	    //have to add UUID to the dataService.insertData function
		        // Call createPoint in metadataService asynchronously.
		        dataService.insertData(uuid, q, ar -> { 
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
		       	  String length = Integer.toString(result.toString().length());
		       		rc.response()
		       		  .putHeader("content-TYPE", "application/text; charset=utf=8")
		       		  .putHeader("content-length",  length)
		       		  .setStatusCode(201)
		       		  .write(result.toString());
		       	}
		       	});
       
    	}//if(token_owner.equals(token))
    	else{
    		System.out.println("Token doesn't have required preveleges");	
    	}
    		
    }//end  if(body.containsKey("userToken"))
    else
    {
    	System.out.println("parameters are missing");	
		return;
    }
    
    /*
     * 
     */
    
  
  }//end insertData(RoutingContext rc) 
  

}
