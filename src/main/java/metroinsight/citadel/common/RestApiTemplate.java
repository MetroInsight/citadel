package metroinsight.citadel.common;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import metroinsight.citadel.model.BaseContent;

public class RestApiTemplate {
  
  public HttpServerResponse getDefaultResponse(RoutingContext rc) {
    return rc.response().putHeader("content-TYPE", "application/json; charset=utf=8");
  }
  
  public void sendErrorResponse(HttpServerResponse resp, int statusCode, String msg) {
      BaseContent content = new BaseContent();
        // If any of uuid does not exist.
      content.setReason(msg);
      String cStr = content.toString();
      String cLen = Integer.toString(cStr.length());
      resp
        .setStatusCode(statusCode)
        .putHeader("content-length", cLen)
        .write(cStr).end();
  }

}
