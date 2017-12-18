package metroinsight.citadel.metadata.impl;

import java.util.UUID;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import metroinsight.citadel.authorization.Authorization_MetaData;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.model.Metadata;

public class MongoService implements MetadataService {
  
  public static MongoClient mongoClient ;
  private final Vertx vertx;
  String collName;
  
  Authorization_MetaData Auth_meta;
  
  public MongoService(Vertx vertx) {
  	/*//
    String uri = vertx.config().getString("mongo_uri");
    if (uri == null) {
      uri = "mongodb://localhost:27017";
    }
    String db = config().getString("mongo_db");
    if (db == null) {
      db = "citadel";
    }
    */
  	String uri = "mongodb://localhost:27017";
  	String db = "citadel";
    JsonObject mongoConfig = new JsonObject()
        .put("connection_string", uri)
        .put("db_name", db);
    mongoClient = MongoClient.createNonShared(vertx, mongoConfig);
    collName = "metadata";
    this.vertx = vertx;
    Auth_meta=new Authorization_MetaData();
  }
  
  /*
   * Not used in API:
   * The used oneis: public void queryPoint(JsonObject query, String userId, Handler<AsyncResult<JsonArray>> resultHandler)
   */
 /*
  @Override
  public void queryPoint(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
	  
	  try{
		  
		  if(query.containsKey("userToken"))
	       {
			  String userToken = query.getString("userToken");
	    		
	    	 //check if this token exists in the HBase, and if it exists, what is the userID
	    	  String userId=Auth_meta.get_userID(userToken);
	    	
	    	  //next check if this userID matches the user who created this Sensor
	    	  //we store the userID in the Sensor Metadata, fetch sensor metadata, and confirm, it and then return it
	    	  if(!userId.equals(""))
	    		{
			  mongoClient.find(collName, query, res -> {
			  		if (res.succeeded()) {
			  			JsonArray ja = new JsonArray();
			  			for (JsonObject json: res.result()) {
			  				
			  				if(json.containsKey("userId"))
			  				{
			  					//match the userID
			  				  if(json.getString("userId").equals(userId))
			  				  {
			  				    json.remove("_id");
				  			    json.remove("userId");
				  			    json.remove("userToken");
				  			    json.remove("uuid");
			  				    ja.add(json);    
			  				  }//end if
			  				  
			  				}//end if
			  				
			  			}//end for
			  			resultHandler.handle(Future.succeededFuture(ja));
			  		} else {
			      	res.cause().printStackTrace();
			      	Exception e= new Exception(res.cause());
			      	resultHandler.handle(Future.failedFuture(e));
			  		}
			  	});
	    		}//end if(!userId.equals(""))
	    	  else
	    		{	
	    		System.out.println("Token is not Valid");	
	    		return;
	    		}
	    	  
	    	  
	    	}//end if(query.containsKey("userToken"))
		  else
			{	
			System.out.println("Token is missing");	
			return;
			}//end else
		  
		  
		  
	  }//end try
	  catch(Exception e)
	  {
		  e.printStackTrace();
		  return;
	  }
	  
  	
  	
  }//end queryPoint
*/
  
  
 public void queryPoint(JsonObject query, String userId, Handler<AsyncResult<JsonArray>> resultHandler) {
	  
	  try{
	    	  //next check if this userID matches the user who created this Sensor
	    	  //we store the userID in the Sensor Metadata, fetch sensor metadata, and confirm, it and then return it
		  query.remove("_id");
		  query.remove("userId");
		  
		  
		  
		  query.put("userId", userId);//restrict by the userId
		  //System.out.println("in queryPoint MongoService Query is:"+query);
		  
		   mongoClient.find(collName, query, res -> {
			  		if (res.succeeded()) {
			  			JsonArray ja = new JsonArray();
			  			for (JsonObject json: res.result()) {
			  			
			  				String owner=json.getString("userId");
			  				    json.remove("_id");
				  			    json.remove("userId");
				  			    json.remove("userToken");
				  			    //json.remove("uuid");
				  			    
				  			 json.put("owner", owner);//adding the owner details
			  				    ja.add(json);    
			  				 
			  				
			  			}//end for
			  			resultHandler.handle(Future.succeededFuture(ja));
			  		} else {
			      	res.cause().printStackTrace();
			      	Exception e= new Exception(res.cause());
			      	resultHandler.handle(Future.failedFuture(e));
			  		}
			  	});
	    		  
	  }//end try
	  catch(Exception e)
	  {
		  e.printStackTrace();
		  resultHandler.handle(Future.failedFuture(e));
	  }
	  
  	
  	
  }//end queryPoint

 
 public void queryPoint2(JsonObject query, String userId, Handler<AsyncResult<JsonArray>> resultHandler) {
	  
	  try{
	    	  //next check if this userID matches the user who created this Sensor
	    	  //we store the userID in the Sensor Metadata, fetch sensor metadata, and confirm, it and then return it
		  //query.remove("_id");
		  //query.remove("userId");
		  
		  
		  
		  //query.put("userId", userId);//restrict by the userId
		  //System.out.println("in queryPoint MongoService Query is:"+query);
		  query=new JsonObject();
		  
		   mongoClient.find(collName, query, res -> {
			  		if (res.succeeded()) {
			  			JsonArray ja = new JsonArray();
			  			for (JsonObject json: res.result()) {
			  			
			  				System.out.println("In query2 mongoservice:"+json);
			  				
			  				boolean add=false;
			  				
			  				String uuid="";
			  				if(json.containsKey("uuid"))
			  				{
			  				uuid=json.getString("uuid");
			  				System.out.println("uuid is:"+uuid);
			  				String policy=Auth_meta.get_policy(uuid, userId);
			  				if(policy.length()>0)
			  					add=true;
			  				else
			  				{
			  				 String ownerid=Auth_meta.get_ds_owner_id(uuid);
			  				 System.out.println("OwnerID is:"+ownerid);
			  				 if(ownerid.equals(userId))
			  					 add=true;
			  				}
			  				
			  				}
			  				
			  				if(add)
			  				{
			  				    String owner=json.getString("userId");
			  				    json.remove("_id");
				  			    json.remove("userId");
				  			    json.remove("userToken");
				  			    //json.remove("uuid");
				  			    
				  			   json.put("owner", owner);//adding the owner details
			  				   ja.add(json);    
			  				}
			  				
			  			}//end for
			  			resultHandler.handle(Future.succeededFuture(ja));
			  		} else {
			      	res.cause().printStackTrace();
			      	Exception e= new Exception(res.cause());
			      	resultHandler.handle(Future.failedFuture(e));
			  		}
			  	});
	    		  
	  }//end try
	  catch(Exception e)
	  {
		  e.printStackTrace();
		  resultHandler.handle(Future.failedFuture(e));
	  }
	  
 	
 	
 }//end queryPoint

 
 
  @Override
  public void getPoint(String uuid, Handler<AsyncResult<JsonArray>> resultHandler){
    JsonObject query = new JsonObject();
    query.put("uuid", uuid);
    mongoClient.findOne(collName, query, null, res -> {
    	if (res.succeeded()) {
    		JsonObject resultJson = res.result();
    		resultJson.remove("_id");
    		String userId=resultJson.getString("userId");
    		resultJson.remove("userId");
    		resultJson.put("owner", userId);
    		resultJson.remove("userToken");
    		resultJson.remove("uuid");
    		JsonArray ja = new JsonArray();
    		ja.add(resultJson);
    		resultHandler.handle(Future.succeededFuture(ja));
      } else {
      	res.cause().printStackTrace();
      }
    });
  }
  
  @Override
  public void createPoint(JsonObject jsonMetadata, Handler<AsyncResult<String>> resultHandler) {
   
    // Validate if it complies to the schema. No actual usage
    // TODO: Need to change this to proper validation instead.
    //changed by sandeep
    //Metadata metadata = jsonMetadata.mapTo(Metadata.class); 
    try 
    {
    	 String uuid = jsonMetadata.getString("uuid");
    	 
	    		 mongoClient.insert(collName, jsonMetadata, res -> {
	    		      if (res.succeeded()) {
	    		        // Load result to future if success.
	    		        resultHandler.handle(Future.succeededFuture(uuid));
	    		      } else {
	    		        // TODO: Need to add failure behavior.
	    		      }
	    		    });
     }//end try
    catch(Exception e)
    {
    	e.printStackTrace();
    	resultHandler.handle(Future.failedFuture(e));
    }
    
    
  }//end createPoint

}
