package metroinsight.citadel.metadata.impl;

import java.util.UUID;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.model.Metadata;

public class MongoService implements MetadataService {
  
  private static MongoClient mongoClient ;
  private final Vertx vertx;
  String collName;
  
  public MongoService(Vertx vertx) {
  	/*
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
  public void getPoint(String srcid, Handler<AsyncResult<Metadata>> resultHandler){
    JsonObject query = new JsonObject();
    query.put("srcid", srcid);
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
    //Metadata metadata = Json.decodeValue(jsonMetadata, Metadata.class);
    UUID uuid = UUID.randomUUID();
    jsonMetadata.put("srcid", uuid.toString());
    Metadata metadata = jsonMetadata.mapTo(Metadata.class); // Schema validation
    mongoClient.insert(collName, jsonMetadata, res -> {
      if (res.succeeded()) {
        resultHandler.handle(Future.succeededFuture(uuid.toString()));
      } else {
      }
      }
    );
  }

  @Override
  public void processQuery(String qStr, Handler<AsyncResult<Metadata>> resultHandler) {
    
  }
  /*
  MongoClient mongoClient;

  private void initMongoClient(){
    String uri = config().getString("mongo_uri");
    if (uri == null) {
      uri = "mongodb://localhost:27017";
    }
    String db = config().getString("mongo_db");
    if (db == null) {
      db = "citadel";
    }
    JsonObject mongoconfig = new JsonObject()
        .put("connection_string", uri)
        .put("db_name", db);
    mongoClient = MongoClient.createShared(vertx, mongoconfig);
  }
  
  public void start() {
  }
  */
}
