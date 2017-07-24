package metroinsight.citadel.data;


import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.model.Metadata;

//@ProxyGen
//@VertxGen
public interface DataService {

  long timestamp_default_min = 1483228800000L;
  Double lat_default_max = 33.459938;
  Double lat_default_min = 32.576754;
  Double lng_default_min = -117.394428;
  Double lng_default_max = -116.725635;
  
  void insertData(JsonArray data, Handler<AsyncResult<Void>> resultHandler);
  //insert the data described by the JsonObject data

  void queryDataBox(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler);
  //box query on the data described by the JsonObject query
  
  void queryData(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler);
  //query on the data described by the JsonObject query
  
}
