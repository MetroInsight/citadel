package metroinsight.citadel.common;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

final public class RestApiTemplate {
  
  final static public HttpServerResponse getDefaultResponse(RoutingContext rc) {
    return rc.response().putHeader("content-TYPE", "application/json; charset=utf=8");
    
  }

}
