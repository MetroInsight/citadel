package metroinsight.citadel.datacache;

import java.util.List;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public interface DataCacheService {
  String ADDRESS = "service.datacache";
  String EVENT_ADDRESS = "datacache";

  void upsertData(String uuid, JsonObject data, List<String> indexKeys, Handler<AsyncResult<Void>> rh);

  void getData(String uuid, List<String> fields, Handler<AsyncResult<JsonObject>> rh);

  void bboxQuery(Double minLng, Double maxLng, Double minLat, Double maxLat,
      Handler<AsyncResult<JsonArray>> rh);
  
  void rangeQuery(String key, Double minVal, Double maxVal, 
      Handler<AsyncResult<JsonArray>> rh);
  
  void upsertIndex(String uuid, JsonObject data, Handler<AsyncResult<Void>> rh);
  
}
