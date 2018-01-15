package metroinsight.citadel.policy;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import metroinsight.citadel.common.MicroServiceVerticle;

public class PolicyVerticle extends MicroServiceVerticle {

  protected ServiceDiscovery discovery;
  PolicyRestApi pm;

  @Override
  public void start(Future<Void> fut) {
    // Init service discovery. Future purpose
    JsonObject configs = config();
    discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions().setBackendConfiguration(config()));

    pm = new PolicyRestApi(configs);

    Router router = Router.router(vertx);
    HttpServerOptions httpOptions = getBaseHttpOptions();
    httpOptions.setPort(configs.getInteger("policy.http.port", 8089));

    // Main page. TODO
    router.route("/").handler(rc -> {
      HttpServerResponse response = rc.response();
      response.putHeader("content-type", "text/html").end("<h1>Welcome to Citadel Policy Management</h1>");
    });

    router.route("/*").handler(BodyHandler.create());

    router.post("/api/registerPolicy").handler(pm::registerPolicy);

    vertx.createHttpServer(httpOptions).requestHandler(router::accept).listen(
        result -> {
          if (result.succeeded()) {
            fut.complete();
            System.out.println("POLICY_VERTICLE STARTED on " + Integer.toString(result.result().actualPort()));
          } else {
            fut.fail(result.cause());
          }
        });
  }// end Start

}// end class PolicyVerticle
