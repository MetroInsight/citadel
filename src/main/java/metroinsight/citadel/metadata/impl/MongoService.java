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
  
  private static MongoClient mongoClient ;
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
  
  @Override
  public void queryPoint(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
  	mongoClient.find(collName, query, res -> {
  		if (res.succeeded()) {
  			JsonArray ja = new JsonArray();
  			for (JsonObject json: res.result()) {
  				json.remove("_id");
  				ja.add(json);
  			}
  			resultHandler.handle(Future.succeededFuture(ja));
  		} else {
      	res.cause().printStackTrace();
  		}
  	});
  }

  @Override
  public void getPoint(String uuid, Handler<AsyncResult<Metadata>> resultHandler){
    JsonObject query = new JsonObject();
    query.put("uuid", uuid);
    mongoClient.findOne(collName, query, null, res -> {
    	if (res.succeeded()) {
    		JsonObject resultJson = res.result();
    		resultJson.remove("_id");
    		Metadata resMetadata = resultJson.mapTo(Metadata.class);
    		resultHandler.handle(Future.succeededFuture(resMetadata));
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
    	//check token is present in the jsonMetadata
    	if(jsonMetadata.containsKey("userToken"))
    	{
    		
    		String userToken = jsonMetadata.getString("userToken");
    		
    		//check if this token exists in the HBase, and if it exists, what is the userID
    		String userId=Auth_meta.get_userID(userToken);
    		
    		if(!userId.equals(""))
    		{
    		 String uuid = UUID.randomUUID().toString();
    		 jsonMetadata.put("uuid", uuid);
    		 jsonMetadata.put("userId", userId);
    		//Metadata metadata =new Metadata(jsonMetadata);
    		mongoClient.insert(collName, jsonMetadata, res -> {
    		      if (res.succeeded()) {
    		        // Load result to future if success.
    		        resultHandler.handle(Future.succeededFuture(uuid));
    		      } else {
    		        // TODO: Need to add failure behavior.
    		      }
    		    });
    		}//end if(!userId.equals(""))
    		else
    		{	
    		System.out.println("Token is not Valid");	
    		return;
    		}
    		
    	}//end if(jsonMetadata.containsKey("userToken"))
    	else
		{	
		System.out.println("Token is missing");	
		return;
		}
     
     }//end try
    catch(Exception e)
    {
    	e.printStackTrace();
    }
    
    
  }//end createPoint

}
