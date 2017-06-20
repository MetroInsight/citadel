package metroinsight.citadel;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult.Series;

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
import metroinsight.citadel.model.Timeseries2;

public class CitadelServer {// extends AbstractVerticle {
  /*
  
  private Map<Integer, Timeseries2> dataMap = new LinkedHashMap<>();
  private MongoClient mongoClient;
  private InfluxDB influxClient;

  private InfluxDB initInfluxDB() {
    InfluxDB influxClient = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
    String dbName = "citadel";
    return influxClient;
  }
  
  private MongoClient initMongoClient(){
    String uri = config().getString("mongo_uri");
    if (uri == null) {
      uri = "mongodb://localhost:27017";
    }
    String db = config().getString("mongo_db");
    if (db == null) {
      db = "citadel";
    }
    JsonObject mongoconfig = new JsonObject()
        .put("connection_string", uri)
        .put("db_name", db);
    return MongoClient.createShared(vertx, mongoconfig);
  }
  
  private void createTimeseriesData(){
    Timeseries2 testData1 = new Timeseries2("Data 1", "1999/06/07");
    dataMap.put(testData1.getId(), testData1);
    Timeseries2 testData2 = new Timeseries2("Data 2", "2017/06/07");
    dataMap.put(testData2.getId(), testData2);
  }
  
  private void getAll(RoutingContext routingContext) {
    routingContext.response()
      .putHeader("content-TYPE", "application/json; charset=utf=8")
      .end(Json.encodePrettily(dataMap.values()));
  }
  
  private void addOne(RoutingContext routingContext) {
    final Timeseries2 ts = Json.decodeValue(routingContext.getBodyAsString(),  Timeseries2.class);
    dataMap.put(ts.getId(), ts);
    routingContext.response()
      .setStatusCode(201)
      .putHeader("content-type",  "application/json; charse=utf-8")
      .end(Json.encodePrettily(ts));
  }
  
  private void deleteOne(RoutingContext rc) {
    String id = rc.request().getParam("id");
    if (id == null) {
      rc.response().setStatusCode(400).end();
    } else {
      Integer idAsInteger = Integer.valueOf(id);
      dataMap.remove(idAsInteger);
    }
    rc.response().setStatusCode(204).end();
  }

  @Override
  public void start(Future<Void> fut) {
    // Initialize MeatdataVerticle
    mongoClient = initMongoClient();
    
    
    createTimeseriesData();
    Router router = Router.router(vertx);
    router.route("/").handler(routingContext -> {
      HttpServerResponse response = routingContext.response();
      response
          .putHeader("content-type", "text/html")
          .end("<h1>Hello from my first Vert.x 3 application</h1>");
    });
    
    router.route("/assets/*").handler(StaticHandler.create("assets"));
    
    router.get("/api/timeseries").handler(this::getAll);
    router.route("/api/timeseries*").handler(BodyHandler.create());
    router.post("/api/timeseries").handler(this::addOne);
    router.delete("/api/timeseries/:id").handler(this::deleteOne);

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
  */
}