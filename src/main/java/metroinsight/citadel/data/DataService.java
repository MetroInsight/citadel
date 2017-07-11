package metroinsight.citadel.data;


import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.model.Metadata;

//@ProxyGen
//@VertxGen
public interface DataService {


  void insertPoint(JsonObject data, Handler<AsyncResult<Metadata>> resultHandler);
  // insert the data described by the JsonObject data

  void queryPoint(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler);
  //query the data described by the JsonObject data
}
