package metroinsight.citadel.rest;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import metroinsight.citadel.common.RestApiTemplate;
import metroinsight.citadel.datacache.DataCacheService;
import metroinsight.citadel.datacache.impl.RedisDataCacheService;
import metroinsight.citadel.model.BaseContent;

public class DataCacheRestApi extends RestApiTemplate{
  Vertx vertx = null;
  DataCacheService cacheService = null;
  
  DataCacheRestApi(Vertx vertx) {
    this.vertx = vertx;
    cacheService = (DataCacheService) new RedisDataCacheService(vertx);
  }

  public void querySimpleBbox(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    JsonObject bbox = rc.getBodyAsJson().getJsonObject("query");
    cacheService.bboxQuery(
        bbox.getDouble("min_lng"),
        bbox.getDouble("max_lng"),
        bbox.getDouble("min_lat"),
        bbox.getDouble("max_lat"), ar -> {
          if (ar.succeeded()) {
            content.setResults(ar.result());
            resp.setStatusCode(200);
          } else {
            content.setReason(ar.cause().getMessage());
            resp.setStatusCode(400);
          }
          String cStr = content.toString();
          String cLen = Integer.toString(cStr.length());
          resp
            .putHeader("content-length", cLen)
            .write(cStr);
          });
  }

}
