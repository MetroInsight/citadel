package metroinsight.citadel.common;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import metroinsight.citadel.model.BaseContent;

public class RestApiTemplate {
  
  protected JsonObject configs = null;
  
  public HttpServerResponse getDefaultResponse(RoutingContext rc) {
    return rc.response().putHeader("content-TYPE", "application/json; charset=utf=8");
  }

  public void sendSuccesResponse(HttpServerResponse resp, int statusCode, JsonArray res) {
      BaseContent content = new BaseContent();
        // If any of uuid does not exist.
      content.setResults(res);
      content.setSucceess(true);
      String cStr = content.toString();
      String cLen = Integer.toString(cStr.length());
      resp
        .setStatusCode(statusCode)
        .putHeader("content-length", cLen)
        .write(cStr)
        .end();
  }
  
  public void sendErrorResponse(HttpServerResponse resp, int statusCode, String msg) {
      BaseContent content = new BaseContent();
        // If any of uuid does not exist.
      content.setReason(msg);
      content.setSucceess(false);
      String cStr = content.toString();
      String cLen = Integer.toString(cStr.length());
      resp
        .setStatusCode(statusCode)
        .putHeader("content-length", cLen)
        .write(cStr)
        .end();
  }

}
