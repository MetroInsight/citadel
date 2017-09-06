package metroinsight.citadel.metadata;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.metadata.impl.VirtuosoService;
import metroinsight.citadel.model.Metadata;

@ProxyGen
@VertxGen
public interface MetadataService {

  String ADDRESS = "service.metadata";
  String EVENT_ADDRESS = "metadata";
  
/*  static MetadataService create(Vertx vertx) {
    return new VirtuosoService(vertx);
  }
  */
  
  static MetadataService createProxy(Vertx vertx, String address) {
    return new MetadataServiceVertxEBProxy(vertx, address);
  }
  
  void getPoint(String uuid, Handler<AsyncResult<Metadata>> resultHandler);
  // Returns all metadata corresponding to the uuid.

  void createPoint(JsonObject jsonMetadata, Handler<AsyncResult<String>> resultHandler);
  // Create Point with given metadata

  void queryPoint(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler);
}
