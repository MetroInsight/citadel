package metroinsight.citadel.rest;

import static metroinsight.citadel.common.RestApiTemplate.getDefaultResponse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.vertx.core.CompositeFuture;
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
import metroinsight.citadel.data.DataService;
import metroinsight.citadel.data.impl.GeomesaService;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.model.BaseContent;
import metroinsight.citadel.virtualsensor.VirtualSensorService;

public class RestApiVerticle extends MicroServiceVerticle {

  MetadataRestApi metadataRestApi ;
  DataRestApi dataRestApi;
  private MetadataService metadataService;
  private DataService dataService;
  private VirtualSensorService vsService;
  
  @Override
  public void start(Future<Void> fut){
    
    // Init service discovery. Future purpose
    discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions().setBackendConfiguration(config()));
    // Init Metadata Service
    metadataService = ProxyHelper.createProxy(MetadataService.class, vertx, MetadataService.ADDRESS);
    //dataService = ProxyHelper.createProxy(DataService.class, vertx, DataService.ADDRESS);
    dataService = (DataService) new GeomesaService(vertx);
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

  public void queryData(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    JsonObject q = rc.getBodyAsJson().getJsonObject("query");
    //TODO: Validate if the UUIDs are valid.
    dataService.queryData(q, ar -> {
      BaseContent content = new BaseContent();
      String cStr;
      String cLen;
      if (ar.failed()) {
        content.setReason(ar.cause().getMessage());
        resp.setStatusCode(400);
      } else {
        resp.setStatusCode(200);
        content.setSucceess(true);
      }
      cStr = content.toString();
      cLen = Integer.toString(cStr.length());
      resp
        .putHeader("content-length", cLen)
        .write(cStr);
    });
  }
  
  public void insertData(RoutingContext rc) {
    HttpServerResponse resp = getDefaultResponse(rc);
    JsonArray q = rc.getBodyAsJson().getJsonArray("data");

    //// Validate if the UUIDs are valid.
    // Extract unique uuids in the data.
    Set<String> uuids = new HashSet<String>();
    for (int i=0; i < q.size(); i++) {
      uuids.add(q.getJsonObject(i).getString("uuid"));
    }
    
    // Check if all uuids exist in metadata db.
    List<Future> uuidFutList = new ArrayList<Future>();
    for (String uuid: uuids) {
      Future<Boolean> uuidFut = Future.future();
      metadataService.getPoint(uuid, rh -> {
        if (rh.succeeded()) {
          uuidFut.complete(true);
        } else {
          uuidFut.fail(uuid + "does not exist");
        }
      });
      uuidFutList.add(uuidFut);
    }
    
    // Actual running of uuid checking and then run the insertion.
    CompositeFuture.join(uuidFutList).setHandler(uuidAr -> {
      BaseContent content = new BaseContent();
      if (uuidAr.failed()) {
        String cStr = "";
        String cLen = "";
        // If any of uuid does not exist.
        content.setReason(uuidAr.cause().getMessage());
        cStr = content.toString();
        cLen = Integer.toString(cStr.length());
        resp
          .setStatusCode(400)
          .putHeader("content-length", cLen)
          .write(cStr);
      } else {
        // Try to insert Data
        dataService.insertData(q, dataAr -> {
          String cStr = "";
          String cLen = "";
          if (dataAr.failed()) {
            // If failed to insert data
            content.setReason(dataAr.cause().getMessage());
            resp.setStatusCode(400);
          } else {
            // Succeeded to insert data
            resp.setStatusCode(201);
            content.setSucceess(true);
          }
          cStr = content.toString();
          cLen = Integer.toString(cStr.length());
          resp
            .putHeader("content-length", cLen)
            .write(cStr);
        });
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
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
    String code = q.getString("code");
    q.remove("code");
    vsService.registerVirtualSensor(code, q, ar -> {
      HttpServerResponse resp = getDefaultResponse(rc);
      BaseContent content = new BaseContent();
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
