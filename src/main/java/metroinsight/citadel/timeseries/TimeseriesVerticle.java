package metroinsight.citadel.timeseries;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import metroinsight.citadel.common.MicroServiceVerticle;

public class TimeseriesVerticle extends MicroServiceVerticle {
  
  private void getTimeseries(RoutingContext rc) {
    HttpServerResponse response = rc.response();
    response
        .putHeader("content-type", "text/html")
        .end("<h1>Hello from my first Vert.x 3 application</h1>");
  }

  private void postTimeseries(RoutingContext rc) {
    // TODO
  }
  
  @Override
  public void start(Future<Void> fut) {
    super.start();
    Router router = Router.router(vertx);
    
    router.route("/api/timeseries*").handler(BodyHandler.create());
    router.get("/api/timeseries").handler(this::getTimeseries);
    router.post("/api/timeseries").handler(this::postTimeseries);
    vertx
      .createHttpServer()
      .requestHandler(router::accept)
      .listen(
          config().getInteger("http.port", 8080),
          result -> {
            if (result.succeeded()) {
              fut.complete();
              System.out.println("TIMESERIES VERTICLE STARTED");
            } else {
              fut.fail(result.cause());
            }
          });
  }
}