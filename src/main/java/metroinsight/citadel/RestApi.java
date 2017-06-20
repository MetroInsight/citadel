package metroinsight.citadel;

import java.util.LinkedHashMap;
import java.util.Map;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.serviceproxy.ProxyHelper;
//import metroinsight.citadel.metadata.MetadataService;
//import metroinsight.citadel.model.Timeseries2;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.model.Metadata;
import metroinsight.citadel.model.Timeseries2;

public class RestApi extends AbstractVerticle {

  private Map<Integer, Timeseries2> dataMap = new LinkedHashMap<>();
  private MongoClient mongoClient;
  protected ServiceDiscovery discovery;
  private MetadataService metadataService;
  
  private void getAll(RoutingContext rc) {
    rc.response()
      .putHeader("content-TYPE", "application/json; charset=utf=8")
      .end(Json.encodePrettily(dataMap.values()));
  }
  
  private void querySrcid(RoutingContext rc) {
    JsonObject body = rc.getBodyAsJson();
    String qStr = body.getValue("query").toString();
    rc.response()
      .putHeader("content-TYPE", "application/json; charset=utf=8")
      .end(Json.encodePrettily(dataMap.values()));
  }

  private void addOne(RoutingContext rc) {
    final Timeseries2 ts = Json.decodeValue(rc.getBodyAsString(),  Timeseries2.class);
    dataMap.put(ts.getId(), ts);
    rc.response()
      .setStatusCode(201)
      .putHeader("content-type",  "application/json; charse=utf-8")
      .end(Json.encodePrettily(ts));
  }
  
  private void queryPoint(RoutingContext rc) {
    JsonObject q = (JsonObject) rc.getBodyAsJson().getValue("query");
    metadataService.queryPoint(q, ar -> {
    	if (ar.failed()) {
      	System.out.println(ar.cause().getMessage());
    	} else {
    		String resultStr = ar.result().toString();
    		String length = Integer.toString(resultStr.length());
    		rc.response()
    		.putHeader("content-TYPE", "application/json; charset=utf=8")
    		.putHeader("content-length",  length)
      	.setStatusCode(200)
      	.write(resultStr)
    		.end();
    	}
    	});
  }
  
  private void getPoint(RoutingContext rc) {
  	String srcid = rc.request().getParam("srcid");
  	if (srcid == null) {
  		rc.response().setStatusCode(400).end();
  	} else {
  		metadataService.getPoint(srcid, ar -> {
  		if (ar.failed()) {
  			System.out.println(ar.cause().getMessage());
  		} else {
  			Metadata resultMetadata = ar.result();
  			rc.response()
  			.putHeader("content-TYPE", "application/json; charset=utf=8")
  			.setStatusCode(200)
  			.end(ar.result().toString());
  		}
  		});
  	}
  }
  
  private void createPoint(RoutingContext rc) {
    JsonObject body = rc.getBodyAsJson();
    JsonObject q = (JsonObject)(body.getValue("query")); // TODO: Validate if this is working
    /* // Method 1
    Future<MetadataService> metadataFuture = Future.future();
    EventBusService.getProxy(discovery, MetadataService.class,
        metadataFuture.completer());
    metadataFuture.setHandler(ar -> {           
      if (ar.failed()) {
      	System.out.println(ar.cause().getMessage());
      	System.out.println("AR INIT FAILED");
        // TODO: return proper failed message
      } else {
        MetadataService ms = metadataFuture.result();
        ms.createPoint(q, sub_ar -> {
          rc.response()
            .putHeader("content-TYPE", "application/text; charset=utf=8")
            .setStatusCode(201)
            .end("SUCCESS");
        });
      }
   });
   //*/
   ///* // Method 2 (This is slightly slower but simpler)
    metadataService.createPoint(q, ar -> {
    	if (ar.failed()) {
      	System.out.println(ar.cause().getMessage());
    	} else {
    		rc.response()
    		.putHeader("content-TYPE", "application/text; charset=utf=8")
      	.setStatusCode(201)
    		.end("SUCCESS");
    	}
    	});
    	//*/
  }
 
  @Override
  public void start(Future<Void> fut){
  	System.out.println("REST_API_VERTICLE STARTED");
    discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions().setBackendConfiguration(config()));
    metadataService = ProxyHelper.createProxy(MetadataService.class, vertx, "service.metadata");
    
    Router router = Router.router(vertx);
    router.route("/").handler(rc -> {
      HttpServerResponse response = rc.response();
      response
          .putHeader("content-type", "text/html")
          .end("<h1>Hello from my first Vert.x 3 application</h1>");
    });
    
    router.route("/assets/*").handler(StaticHandler.create("assets"));
    /*
    router.route("/api/timeseries*").handler(BodyHandler.create());
    router.get("/api/timeseries").handler(this::getAll);
    router.post("/api/timeseries").handler(this::addOne);
    */
    router.get("/api/sensor").handler(rc -> {
    	System.out.println(rc.getBodyAsString());
    });
    router.route("/api/sensor*").handler(BodyHandler.create());
    router.post("/api/sensor").handler(this::createPoint);
    router.get("/api/sensor/:srcid").handler(this::getPoint);
    
    router.route("/api/query*").handler(BodyHandler.create());
    router.post("/api/query").handler(this::queryPoint);

    vertx
        .createHttpServer()
        .requestHandler(router::accept)
        .listen(
            config().getInteger("http.port", 8080),
            result -> {
              if (result.succeeded()) {
                fut.complete();
              } else {
                fut.fail(result.cause());
                }
              }
        );
  }

}
