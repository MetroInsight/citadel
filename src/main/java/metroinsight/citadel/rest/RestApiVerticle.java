package metroinsight.citadel.rest;

import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import metroinsight.citadel.common.MicroServiceVerticle;

public class RestApiVerticle extends MicroServiceVerticle {

  MetadataRestApi metadataRestApi ;
  DataRestApi dataRestApi;
  VirtualSensorRestApi vsRestApi;
  DataCacheRestApi dataCacheRestApi;
  
  @Override
  public void start(Future<Void> fut){
    
    // Init service discovery. Future purpose
    discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions().setBackendConfiguration(config()));
    // Init Metadata Service

    // REST API modules
    metadataRestApi = new MetadataRestApi(vertx);
    dataRestApi     = new DataRestApi(vertx);
    vsRestApi = new VirtualSensorRestApi(vertx);
    dataCacheRestApi = new DataCacheRestApi(vertx);
    
    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    
    // Main page. TODO
    router.route("/").handler(rc -> {
      HttpServerResponse response = rc.response();
      response
          .putHeader("content-type", "text/html")
          .end("<h1>Hello from my first Vert.x 3 application</h1>");
    });
    
    router.route("/*").handler(BodyHandler.create());
    
    // Redirection to API Doc (TODO: Swagger should be tightly integrated.) 
    router.get("/doc/api").handler(rc -> {
      HttpServerResponse response = rc.response();
      response.putHeader("Location", "http://localhost:9090/api/ui/"); //TODO: Need to fill this everytime for now.
      response.setStatusCode(302);
      response.end();
    });
    
    // REST API routing for MetaData
    router.post("/api/point").blockingHandler(metadataRestApi::createPoint);
    router.get("/api/point/:uuid").blockingHandler(metadataRestApi::getPoint);
    router.post("/api/query").blockingHandler(metadataRestApi::queryPoint);

    // REST API routing for Data
    router.post("/api/data").blockingHandler(dataRestApi::insertData);
    router.post("/api/querydata").blockingHandler(dataRestApi::queryData);
    router.post("/api/querydata/simplebbox").blockingHandler(dataCacheRestApi::querySimpleBbox);
    
    Integer port = config().getInteger("http.port", 8080);

    vertx
      .createHttpServer()
      .requestHandler(router::accept)
      .listen(
          port,
          result -> {
            if (result.succeeded()) {
              fut.complete();
              System.out.println("REST_API_VERTICLE STARTED at " + port.toString());
            } else {
              fut.fail(result.cause());
            }
          });
  }

}
