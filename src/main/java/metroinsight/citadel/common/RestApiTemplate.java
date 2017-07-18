package metroinsight.citadel.common;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;

public class RestApiTemplate {
  
  final protected String sensorNotFound = "Sensor not found";

  protected HttpServerResponse getDefaultResponse(RoutingContext rc) {
    return rc.response().putHeader("content-TYPE", "application/json; charset=utf=8");
    
  }

}
