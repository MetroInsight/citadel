package metroinsight.citadel.federation;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public interface SourceConnector {
  // TODO: Add the model of source metadata. (URL, ownwer, contained data, etc.)
  
  void queryData(JsonObject query, String identity, Handler<AsyncResult<JsonArray>> rh);
  
}
