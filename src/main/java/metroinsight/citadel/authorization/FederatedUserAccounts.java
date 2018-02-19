package metroinsight.citadel.authorization;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.UpdateOptions;
import io.vertx.servicediscovery.ServiceDiscovery;

public class FederatedUserAccounts {
  Vertx vertx;
  ServiceDiscovery discovery;
  String DB_NAME;

  protected static MongoClient client;
  protected String COLL_NAME;
  protected UpdateOptions upsertOptions;
  
  public FederatedUserAccounts(Vertx vertx) {
    upsertOptions = new UpdateOptions().setUpsert(true);
    this.vertx = vertx;
    Buffer confBuffer = vertx.fileSystem().readFileBlocking("./src/main/resources/conf/citadel-conf.json");
    JsonObject configs = new JsonObject(confBuffer);
  	String uri = String.format("mongodb://%s:%d", configs.getString("auth.mongodb.hostname"), configs.getInteger("auth.mongodb.port"));
  	String db = "citadel";
    JsonObject mongoConfig = new JsonObject()
        .put("connection_string", uri)
        .put("db_name", configs.getString("auth.mongodb.dbname"));
    client = MongoClient.createNonShared(vertx, mongoConfig);
    COLL_NAME= configs.getString("auth.mongodb.collectionname");
    createTable(res -> {
      if (res.succeeded()) {
        System.out.println("Mongodb init success");
      } else {
        System.out.println("Mongodb init failed due to" + res.cause().getMessage());
      }
    }); 
  }

  private void createTable(Handler<AsyncResult<Void>> rh) {
    JsonObject keys = new JsonObject().put("user_id", 1);
    client.createIndex(COLL_NAME, keys, res -> {
      if (res.succeeded()) {
        System.out.println("Index created for mongodb");
      } else {
        rh.handle(Future.failedFuture(res.cause().getMessage()));
      }
    });
  }
  
}