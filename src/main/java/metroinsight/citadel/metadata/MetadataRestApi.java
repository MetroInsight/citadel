package metroinsight.citadel.metadata;

import java.util.UUID;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import metroinsight.citadel.authorization.Authorization_MetaData;
import metroinsight.citadel.common.RestApiTemplate;
import metroinsight.citadel.metadata.impl.MongoService;
import metroinsight.citadel.model.BaseContent;
import metroinsight.citadel.model.Metadata;

public class MetadataRestApi extends RestApiTemplate{

  private MongoClient mongoClient;
  private MetadataService metadataService;
  Authorization_MetaData Auth_meta;
  
  public MetadataRestApi (Vertx vertx) {
    metadataService = new MongoService (vertx);
    Auth_meta=new Authorization_MetaData();
    
  }
  
 
  
  public void queryPoint(RoutingContext rc) {
	HttpServerResponse resp = getDefaultResponse(rc);
	BaseContent content = new BaseContent();
	JsonObject body =(JsonObject) rc.getBodyAsJson();
    
    try{
		  
		  if(body.containsKey("userToken")&&body.containsKey("query"))
	       {
			  JsonObject query =  body.getJsonObject("query");
			  String userToken = body.getString("userToken");
			  
			  if(userToken.equals(""))//TODO: also check that query is not empty here!!
		    	{
		    		//System.out.println("The parameters are missing");
		    		//return;
		    		System.out.println("In MetadataRestApi: Query parameters are missing");
		        	sendErrorResponse(resp, 400, "Parameters are missing");
		    		
		    	}
			  
	    	 //check if this token exists in the HBase, and if it exists, what is the userID
	    	  String userId=Auth_meta.get_userID(userToken);
	    	  
	    	  //In MongoDb check if this userID matches the user who created this Sensor
	    	  //we store the userID in the Sensor Metadata, fetch sensor metadata, and confirm, it and then return it   	  
	    	  if(!userId.equals(""))
	    		{
		    	//next check if for this userID the policy exists
	    		//for this userId extract the policy
	    	    //String policy = Auth_meta.get_policy(uuid, userId);
	    	    
	    	   // if(policy.equals("true")){
	    		  
	    	    	//return the metadata for the given query and userId
	    		  metadataService.queryPoint(query,userId, ar -> {
	    		    	if (ar.failed()) {
	    		      	System.out.println(ar.cause().getMessage());
	    		      	content.setReason(ar.cause().getMessage());
	    				resp.setStatusCode(400);
	    		    	} else {
	    		    		
	    		    		  JsonArray pointResult = ar.result();
	    		    		  resp.setStatusCode(200);
	    		    		  content.setSucceess(true);
	    		    		  content.setResults(pointResult);
	    		    	}
	    		    	
	    		    	  String cStr = content.toString();
	    		    	  String cLen = Integer.toString(cStr.length());
	    		    	  resp.putHeader("content-length", cLen)
	    		      	  .write(cStr);
	    		    	
	    		    	});
	    		  
	    	   // }//if(policy.equals("true"))
	    	    
	    	   /*
	    		else
	        	{
	        		System.out.println("In MetadataRestApi: Policy for user doesn't exist");
	        		sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
	        	}
	    	    */
	    		  
	    		}//end if(!userId.equals(""))
	    	  else
	    		{	
	    		System.out.println("Token is not Valid");	
	    		//return;
	    		sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
	    
	    		}
	    	  
	    	  
	    	}//end if(query.containsKey("userToken"))
		  else
			{	
			System.out.println("Token is missing or uuid is missing");	
			//return;
			System.out.println("In MetadataRestApi: Parameters are missing");
        	sendErrorResponse(resp, 400, "Parameters are missing");
        	
			}//end else
		  
		  
		  
	  }//end try
	  catch(Exception e)
	  {
		  e.printStackTrace();
		  sendErrorResponse(resp, 400, "Internal server error occured, please contact developers.");	
	  }
	  
    
    
    
    
  }//end queryPoint(RoutingContext rc)
  
  
  public void getPoint(RoutingContext rc) {
	
	  HttpServerResponse resp = getDefaultResponse(rc);
		BaseContent content = new BaseContent();
		    
	    JsonObject body = (JsonObject) rc.getBodyAsJson();
	    
	    try{
			  
			  if(body.containsKey(Auth_meta.userToken)&&body.containsKey("uuid"))
		       {
				  String userToken = body.getString(Auth_meta.userToken);
				  String uuid=body.getString("uuid");
				  if(userToken.equals("")||uuid.equals(""))//TODO: also check that query is not empty here!!
			    	{
			    		//System.out.println("The parameters are missing");
			    		//return;
			    		System.out.println("In MetadataRestApi: Query parameters are missing");
			        	sendErrorResponse(resp, 400, "Parameters are missing");
			    		
			    	}
				  
		    	 //check if this token exists in the HBase, and if it exists, what is the userID
		    	  String userId=Auth_meta.get_userID(userToken);
		    	  
		    	  //next check if this userID matches the user who created this Sensor
		    	  //we store the userID in the Sensor Metadata, fetch sensor metadata, and confirm, it and then return it   	  
		    	  if(!userId.equals(""))
		    		{
			    	//next check if for this userID the policy exists
		    		//for this userId extract the policy
		    	    String policy = Auth_meta.get_policy(uuid, userId);
		    	    
		    	    if(policy.equals("true"))
		    	    {
		    	    	//return the metadata for the given query and userId
		    		  metadataService.getPoint(uuid, ar -> {
		    		    	if (ar.failed()) {
		    		      	System.out.println(ar.cause().getMessage());
		    		      	content.setReason(ar.cause().getMessage());
		    				resp.setStatusCode(400);
		    		    	} else {
		    		    		
		    		    		  JsonArray pointResult = ar.result();
		    		    		  resp.setStatusCode(200);
		    		    		  content.setSucceess(true);
		    		    		  content.setResults(pointResult);
		    		    	}
		    		    	
		    		    	  String cStr = content.toString();
		    		    	  String cLen = Integer.toString(cStr.length());
		    		    	  resp.putHeader("content-length", cLen)
		    		      	  .write(cStr);
		    		    	
		    		    	});
		    		  
		    	    }//if(policy.equals("true"))
		    	    
		    	    else
		        	{
		        		System.out.println("In MetadataRestApi: Policy for user doesn't exist");
		        		sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
		        	}
		    	    
		    		}//end if(!userId.equals(""))
		    	  else
		    		{	
		    		System.out.println("Token is not Valid");	
		    		//return;
		    		sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
		    
		    		}
		    	  
		    	  
		    	}//end if(query.containsKey("userToken"))
			  else
				{	
				System.out.println("Token is missing or uuid is missing");	
				//return;
				System.out.println("In MetadataRestApi: Query parameters are missing");
	        	sendErrorResponse(resp, 400, "Query parameters are missing");
	        	
				}//end else
			  
			  
			  
		  }//end try
		  catch(Exception e)
		  {
			  e.printStackTrace();
			  sendErrorResponse(resp, 400, "Internal server error occured, please contact developers.");	
		  }
		 
  }//end fxn
  
  
  public void createPoint(RoutingContext rc) {
	
	System.out.println("In createPoint: MetadataRestApi");
    JsonObject body = rc.getBodyAsJson();
    HttpServerResponse resp = getDefaultResponse(rc);
	BaseContent content = new BaseContent();
	
    try 
    {
    	//check token and sensor is present
    	if(!body.containsKey(Auth_meta.userToken)&&!body.containsKey("sensor"))
    	{
    		System.out.println("In MetadataRestApi: Insert data parameters are missing");
        	sendErrorResponse(resp, 400, "Query parameters are missing");	
    	}
    	// Get the query as JSON.
        JsonObject jsonMetadata = (JsonObject)(body.getValue("sensor"));
        System.out.println("Sensor is:"+jsonMetadata);
        
   		String userToken = body.getString(Auth_meta.userToken);
   		
   		//check if this token exists in the HBase, and if it exists, what is the userID
   		String userId=Auth_meta.get_userID(userToken);
   		
   		if(!userId.equals(""))
   		{
   			 String uuid = UUID.randomUUID().toString();
   			//token exists and is linked to the valid userId
   			
   			 
   			 //inserts the owner token, userId and ds_ID into the hbase metadata table
   			 Auth_meta.insert_ds_owner(uuid,userToken,userId);
   		
   			 //insert the policy for Owner to default "true", no-space-time constraints
   			 Auth_meta.insert_policy(uuid, userId, "true"); 
	    		 jsonMetadata.put("uuid", uuid);
	    		 jsonMetadata.put("userId", userId);
	    		 //Metadata metadata =new Metadata(jsonMetadata);
	    		 //Call createPoint in metadataService asynchronously.
	    		    metadataService.createPoint(jsonMetadata, ar -> { 
	    		      // ar is a result object created in metadataService.createPoint
	    		      // We pass what to do with the result in this format.
	    		    	if (ar.failed()) {
	    		    	  // if the service is failed
	    		    	  // TODO: add response here.
	    		      	System.out.println(ar.cause().getMessage());
	    		      	sendErrorResponse(resp, 400, "Internal server error occured, please contact developers.");	
	    		        
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
   		}//end if(!userId.equals(""))
   		else
   		{	
   		System.out.println("Token is not Valid");	
   		//return;
   		sendErrorResponse(resp, 400, "Api-Token doesn't exist or it doesn't have required priveleges");	
   		}
   		
    }//end try
   catch(Exception e)
   {
   	e.printStackTrace();
   	sendErrorResponse(resp, 400, "Internal server error occured, please contact developers.");	
   }
      
  }//createPoint

}
