package metroinsight.citadel;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import metroinsight.citadel.common.MicroServiceVerticle;
import metroinsight.citadel.data.DataRestApi;
import metroinsight.citadel.metadata.MetadataRestApi;

public class RestApiVerticle extends MicroServiceVerticle {

  MetadataRestApi metadataRestApi ;
  DataRestApi dataRestApi;
 
  @Override
  public void start(Future<Void> fut){
    // Init service discovery. Future purpose
    discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions().setBackendConfiguration(config()));

    // REST API modules
    metadataRestApi = new MetadataRestApi(vertx);
    dataRestApi     = new DataRestApi(vertx);
    
    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    
    // Main page. TODO
    router.route("/").handler(rc -> {
      HttpServerResponse response = rc.response();
      response
          .putHeader("content-type", "text/html")
          .end("<h1>Hello from my first Vert.x 3 application</h1>");
    });
    
    // REST API routing for MetaData
    router.post("/api/point").handler(metadataRestApi::createPoint);
    router.get("/api/point/:uuid").handler(metadataRestApi::getPoint);
    router.post("/api/query").blockingHandler(metadataRestApi::queryPoint, false);

    // REST API routing for Data
    router.post("/api/data").blockingHandler(dataRestApi::insertData, false);
    router.post("/api/querydata").blockingHandler(dataRestApi::queryData, false);
    router.post("/api/querydata/simplebbox").blockingHandler(dataRestApi::querySimpleBbox, false);
    
    vertx
        .createHttpServer()
        .requestHandler(router::accept)
        .listen(
            config().getInteger("http.port", 8080),
            result -> {
              if (result.succeeded()) {
                fut.complete();
                System.out.println("REST_API_VERTICLE STARTED");
              } else {
                fut.fail(result.cause());
                }
              }
        );
  }

}
