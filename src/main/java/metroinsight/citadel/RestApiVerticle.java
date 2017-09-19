package metroinsight.citadel;

import static metroinsight.citadel.common.RestApiTemplate.getDefaultResponse;

import java.net.URL;
import java.net.URLClassLoader;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.serviceproxy.ProxyHelper;
import metroinsight.citadel.common.MicroServiceVerticle;
import metroinsight.citadel.data.DataRestApi;
import metroinsight.citadel.metadata.MetadataRestApi;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.model.BaseContent;
import metroinsight.citadel.virtualsensor.VirtualSensorService;

public class RestApiVerticle extends MicroServiceVerticle {

  MetadataRestApi metadataRestApi ;
  DataRestApi dataRestApi;
  private MetadataService metadataService;
  private VirtualSensorService vsService;
  
  @Override
  public void start(Future<Void> fut){
    // Init service discovery. Future purpose
    discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions().setBackendConfiguration(config()));
    // Init Metadata Service
    //Future<MetadataService> metadataFuture = Future.future();
    //EventBusService.getProxy(discovery, MetadataService.class, metadataFuture);
    metadataService = ProxyHelper.createProxy(MetadataService.class, vertx, MetadataService.ADDRESS);
    vsService = ProxyHelper.createProxy(VirtualSensorService.class, vertx, VirtualSensorService.ADDRESS);

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
    
    router.route("/*").handler(BodyHandler.create());
    
    // REST API routing for MetaData
    router.post("/api/point").blockingHandler(metadataRestApi::createPoint);
    router.get("/api/point/:uuid").blockingHandler(metadataRestApi::getPoint);
    router.post("/api/query").blockingHandler(this::queryPoint);
//    router.post("/api/query").blockingHandler(metadataRestApi::queryPoint);

    // REST API routing for Data
    router.post("/api/data").blockingHandler(dataRestApi::insertData);
    router.post("/api/querydata").blockingHandler(dataRestApi::queryData);
    router.post("/api/querydata/simplebbox").blockingHandler(dataRestApi::querySimpleBbox);
    
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

  public void queryPoint(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
    metadataService.queryPoint(q, ar -> {
      if (ar.failed()) {
        content.setReason(ar.cause().getMessage());
        resp.setStatusCode(400);
      } else {
        content.setSucceess(true);;
        content.setResults(ar.result());
        resp.setStatusCode(200);
      }
      String cStr = content.toString();
      String cLen = Integer.toString(cStr.length());
      resp
        .putHeader("content-length", cLen)
        .write(cStr);
      });
  }
  
  public void registerVirtualSensor(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    BaseContent content = new BaseContent();
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
    String code = q.getString("code");
    q.remove("code");
    vsService.registerVirtualSensor(code, q, ar -> {
      if (ar.failed()) {
        content.setReason(ar.cause().getMessage());
        resp.setStatusCode(400);
      } else {
        content.setSucceess(true);;
        String uuid = ar.result();
        JsonArray resultArray = new JsonArray();
        resultArray.add(uuid);
        content.setResults(resultArray);
        resp.setStatusCode(200);
      }
      String cStr = content.toString();
      String cLen = Integer.toString(cStr.length());
      resp
        .putHeader("content-length", cLen)
        .write(cStr);
      });
  }

}
