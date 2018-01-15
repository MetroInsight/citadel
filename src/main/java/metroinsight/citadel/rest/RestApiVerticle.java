package metroinsight.citadel.rest;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import metroinsight.citadel.common.MicroServiceVerticle;

public class RestApiVerticle extends MicroServiceVerticle {

  MetadataRestApi metadataRestApi;
  DataRestApi dataRestApi;
  VirtualSensorRestApi vsRestApi;
  DataCacheRestApi dataCacheRestApi;

  @Override
  public void start(Future<Void> fut) {

    JsonObject configs = config();

    HttpServerOptions httpOptions = getBaseHttpOptions();
    httpOptions.setPort(config().getInteger("rest.http.port", 8080));

    // Init service discovery. Future purpose
    discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions().setBackendConfiguration(config()));

    // REST API modules
    // Jason Note: Propagating configs to all the modules is not a good practice in
    // microservices design.
    // However, as Authoriazation_Metadata is not a microservice but a library,
    // all the dependent modules need to know the configs.
    metadataRestApi = new MetadataRestApi(vertx, configs);
    dataRestApi = new DataRestApi(vertx, configs);
    vsRestApi = new VirtualSensorRestApi(vertx);
    dataCacheRestApi = new DataCacheRestApi(vertx);

    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());

    // Main page. TODO
    router.route("/").handler(rc -> {
      HttpServerResponse response = rc.response();
      response.putHeader("content-type", "text/html").end("<h1>Welcome to Citadel</h1>\n"
          + "<h1><a href=\"http://metroinsight.westus.cloudapp.azure.com/doc/api/\">API Documentation</a></h1>");
    });

    router.route("/*").handler(BodyHandler.create());
    /*
     * // Redirection to API Doc (TODO: Swagger should be tightly integrated.)
     * router.get("/doc/api").handler(rc -> { HttpServerResponse response =
     * rc.response(); response.putHeader("Location",
     * "http://localhost:9090/api/ui/"); //TODO: Need to fill this everytime for
     * now. response.setStatusCode(302); response.end(); });
     */

    // REST API routing for MetaData
    router.post("/api/point").blockingHandler(metadataRestApi::createPoint);
    router.get("/api/point/:uuid").blockingHandler(metadataRestApi::getPoint);
    router.post("/api/query").blockingHandler(metadataRestApi::queryPoint);

    // REST API routing for Data
    router.post("/api/data").blockingHandler(dataRestApi::insertData);
    router.post("/api/querydata").blockingHandler(dataRestApi::queryData);
    router.post("/api/querydata/simplebbox").blockingHandler(dataCacheRestApi::querySimpleBbox);

    HttpServer server = vertx.createHttpServer(httpOptions);
    server = server.requestHandler(router::accept);
    server.listen(
        result -> {
          if (result.succeeded()) {
            fut.complete();
            System.out.println("REST_API_VERTICLE STARTED on " + Integer.toString(result.result().actualPort()));
          } else {
            fut.fail(result.cause());
          }
        });
  }

}
