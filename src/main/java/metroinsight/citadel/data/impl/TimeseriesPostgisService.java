package metroinsight.citadel.data.impl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.julienviet.pgclient.PgClient;
import com.julienviet.pgclient.PgConnection;
import com.julienviet.pgclient.PgPool;
import com.julienviet.pgclient.PgPoolOptions;
import com.julienviet.pgclient.PgResult;
import com.julienviet.pgclient.Row;
import com.julienviet.pgclient.Tuple;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.ServiceDiscovery;
import metroinsight.citadel.data.DataService;
import metroinsight.citadel.model.Datapoint;;

public class TimeseriesPostgisService implements DataService {
  Vertx vertx;
  ServiceDiscovery discovery;
  String hostname;
  String DB_NAME = "citadel";
  String TABLE_NAME = "MEASUREMENTS";
  PgPool client;
  int paginationSize = 100;
  SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSX");

  public TimeseriesPostgisService(Vertx vertx, ServiceDiscovery discovery, String host, int port, String user,
      String password) {
    this.vertx = vertx;
    this.discovery = discovery;

    PgPoolOptions options = new PgPoolOptions().setPort(5432).setHost(host).setDatabase(DB_NAME).setUsername(user)
        .setPassword(password).setSsl(false).setMaxSize(10);
    client = PgClient.pool(this.vertx, options);
    createTable(tableRes -> {
      if (tableRes.succeeded()) {
      } else {
        System.out.println("Cannot create tabl in PostgreSQL due to:\n" + tableRes.cause().getMessage());
      }
    });
  }

  private void createTable(Handler<AsyncResult<Void>> rh) {
    String tableSql = "CREATE TABLE " + TABLE_NAME + "(\n" + 
                 "time TIMESTAMP NOT NULL,\n" + 
                 "location GEOMETRY NOT NULL\n," + 
                 "uuid UUID NOT NULL\n," + 
                 "value DOUBLE PRECISION NOT NULL\n" + 
                 ");";
    String htSql = String.format(
        "SELECT create_hypertable('%s', 'time');", TABLE_NAME);
    String indexSql = String.format("CREATE UNIQUE INDEX row_idx on %s (time,location,uuid);", TABLE_NAME);
    String uuidIdxSql = String.format("CREATE INDEX uuid_idx on %s (uuid);", TABLE_NAME);
    String geomIdxSql = String.format("CREATE INDEX loc_idx ON %s USING GIST (location);", TABLE_NAME);
    client.getConnection(connRes -> {
      if (connRes.succeeded()) {
        PgConnection conn = connRes.result();
        conn.query(tableSql, res0 -> {
          if (res0.succeeded()) {
            System.out.println("PostgreSQL, created table");
            conn.query(htSql, res1 -> {
              if (res1.succeeded()) {
                System.out.println("PostgreSQL, created hypertable");
                conn.query(indexSql, res2 -> {
                  if (res2.succeeded()) {
                    System.out.println("PostgreSQL, created unique row indices");
                    conn.query(uuidIdxSql, res3 -> {
                      if (res3.succeeded()) {
                        System.out.println("PostgreSQL, created UUID indices");
                        conn.query(geomIdxSql, res4 -> {
                          if (res4.succeeded()) {
                            System.out.println("PostgreSQL, created geometry indices");
                            rh.handle(Future.succeededFuture());
                            conn.close();
                          } else {
                            rh.handle(Future.failedFuture(res4.cause()));
                            conn.close();
                          }
                        });
                      } else {
                        rh.handle(Future.failedFuture(res3.cause()));
                        conn.close();
                      }
                      });
                    } else {
                      Throwable aaa = res2.cause();
                      rh.handle(Future.failedFuture(res2.cause()));
                      conn.close();
                    }
                    });
              } else {
                rh.handle(Future.failedFuture(res1.cause()));
                conn.close();
              }
            });
          } else {
            String msg = res0.cause().getMessage();
            if (msg.contains("already exists")) {
              System.out.println(msg);
              rh.handle(Future.succeededFuture());
              conn.close();
            } else {
              rh.handle(Future.failedFuture(res0.cause()));
              conn.close();
            }
          }
        });
      } else {
        rh.handle(Future.failedFuture(connRes.cause()));
      }
    });
  }
            
            
  String geomlist2ewktstr(String geomType, List<List<Double>> cds) {
    // List<List<Double>> getCoordinates() {
    String ewkt = "";
    geomType = geomType.toLowerCase();
    if (geomType.equals("point")) {
      ewkt += "POINT(";
    } else if (geomType.equals("linestring")) {
      ewkt += "LINESTRING(";
    } else if (geomType.equals("polygon")) {
      ewkt += "POLYGON((";
    }
    // TODO: Currently POLYGON can only consist of a LINE. May need to change later.
    Iterator<List<Double>> topIter = cds.iterator();
    while (topIter.hasNext()) {
      List<Double> point = topIter.next();
      Double lng = point.get(0);
      Double lat = point.get(1);
      ewkt += lng.toString() + " " + lat.toString() + ",";
    }
    ewkt = ewkt.substring(0, ewkt.length() - 1) + ")";
    if (geomType.equals("polygon")) {
      ewkt += ")";
    }
    return ewkt;
  }
  
  public void insertDataBatch(JsonArray data, Handler<AsyncResult<Void>> rh) {
    try {
      client.getConnection(r0 -> {
        if (r0.succeeded()) {
          List<Tuple> batch = new LinkedList<>();
          for (int i=0; i<data.size(); i++) {
            JsonObject datum = data.getJsonObject(i);
            Datapoint dp = datum.mapTo(Datapoint.class);
            batch.add(Tuple.of(dateFormatter.format(new Date(dp.getTimestamp())),
                               geomlist2ewktstr(dp.getGeometryType(), dp.getCoordinates()),
                               dp.getUuid(), 
                               dp.getValue()));
          String sql = String.format("INSERT INTO %s (time, location, uuid, value) VALUES ($1, ST_GeomFromEWKT('SRID=4326;$2'), $3, $4)\n", TABLE_NAME) + 
                       "ON CONFLICT (time,location,uuid) DO UPDATE SET value = excluded.value;";
          PgConnection conn = r0.result();
          conn.preparedBatch(sql, batch, res -> {
            if (res.succeeded()) {
              rh.handle(Future.succeededFuture());
            } else {
              rh.handle(Future.failedFuture(res.cause()));
            }
            conn.close();
          });
          }
        } else {
          rh.handle(Future.failedFuture(r0.cause()));
        }
      });
    } catch (Exception e) {
      rh.handle(Future.failedFuture(e.getMessage()));
    }
  }

  @Override
  public void insertData(JsonArray data, Handler<AsyncResult<Void>> rh) {
    // TODO: Use below
    try {
      String q = "";
      int currIdx = 1;
      for (int i = 0; i < data.size(); i++) {
        if (q.length() == 0) {
          q = "INSERT INTO measurements (time, location, uuid, value) VALUES\n";
        }
        JsonObject datum = data.getJsonObject(i);
        Datapoint dp = datum.mapTo(Datapoint.class);
        String ewkt = geomlist2ewktstr(dp.getGeometryType(), dp.getCoordinates());
        String dateStr = dateFormatter.format(new Date(dp.getTimestamp()));
        q += String.format("('%s', %s, '%s'::uuid, %f),\n", dateStr,
            // String.format("ST_GeogFromText('SRID=4326;%s')", ewkt),
            String.format("ST_GeomFromEWKT('SRID=4326;%s')", ewkt),
            // String.format("ST_GeomFromEWKT('SRID=4269;%s')", ewkt),
            dp.getUuid(), dp.getValue());
        if ((i % paginationSize == 0 && i != 0) || i == data.size() - 1) {
          q = q.substring(0, q.length() - 2) + "\n" + "ON CONFLICT (time,location,uuid) DO UPDATE\n"
              + "SET value = excluded.value;";
          // ";";
          client.query(q, res -> {
            Throwable ress = res.cause();
            if (res.failed()) {
              String reason = res.cause().getMessage();
              rh.handle(Future.failedFuture(res.cause()));
            }
          });
          q = "";
        }
      }
      assert q.length() == 0; // Just check if all queries are executed
      rh.handle(Future.succeededFuture());
    } catch (Exception e) {
      rh.handle(Future.failedFuture(e));
    }
  }// end function

  @Override
  public void queryDataBox(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {

  }

  @Override
  public void queryData(JsonObject query, Map<String, String> policies, Handler<AsyncResult<JsonArray>> rh) {
    try {
      String q = "SELECT time, ST_AsGeoJson(location), uuid, value \n" + "FROM " + TABLE_NAME + "\n" + "WHERE \n";
      if (query.containsKey("lat_max")) {
        assert query.containsKey("lat_min");
        assert query.containsKey("lng_max");
        assert query.containsKey("lng_min");
        double lngMin = query.getDouble("lng_min");
        double lngMax = query.getDouble("lng_max");
        double latMin = query.getDouble("lat_min");
        double latMax = query.getDouble("lat_max");
        q += "location && " + String.format("ST_MakeEnvelope (%f, %f, %f, %f, 4326) \n", lngMin, latMin, lngMax, latMax);
      }
      if (query.containsKey("timestamp_min")) {
        assert query.containsKey("timestamp_max");
        q += String.format("AND time >= '%s'\n", dateFormatter.format(new Date(query.getLong("timestamp_min")))) +
             String.format("AND time < '%s'\n", dateFormatter.format(new Date(query.getLong("timestamp_max"))));
      }
      if (query.containsKey("uuids")) {
        String uuidSetStr = "";
        Iterator<Object> iter = query.getJsonArray("uuids").iterator();
        while (iter.hasNext()) {
          uuidSetStr += String.format("'%s', ", (String) iter.next());
        }
        uuidSetStr = uuidSetStr.substring(0, uuidSetStr.length() - 2);
        q += String.format("AND uuid IN (%s)\n", uuidSetStr);
      }
      
      q += ";";
      client.query(q, res -> {
        try {
          if (res.succeeded()) {
            JsonArray jsonRes = new JsonArray();
            PgResult<Row> result = res.result();
            for (Row row : result) {
              JsonObject oneRes = new JsonObject();
              oneRes.put("timestamp", Timestamp.valueOf(row.getLocalDateTime(0)).getTime());
              oneRes.put("uuid", row.getString(2));
              oneRes.put("value", row.getDouble(3));
              JsonObject loc = new JsonObject(row.getString(1));
              oneRes.put("coordinates", loc.getJsonArray("coordinates")); // TODO: Implement parsing the locStr.
              oneRes.put("geometryType", loc.getString("type")); // TODO: Implement parsing the locStr.
              jsonRes.add(oneRes);
            }
            rh.handle(Future.succeededFuture(jsonRes));
          } else {
            System.out.println("query failed!!!");
            rh.handle(Future.failedFuture(res.cause()));
          }
        } catch (Exception e) {
          rh.handle(Future.failedFuture(e.getMessage()));
        }
      });
    } catch (Exception e) {
      rh.handle(Future.failedFuture(e.getCause()));
    }
  }

  @Override
  public void queryData(JsonObject query, Handler<AsyncResult<JsonArray>> rh) {
    System.out.println("Wrong access");
    rh.handle(Future.failedFuture("WRONG ACCESS"));

  }

  public static void main(String[] args) {
    VertxOptions options = new VertxOptions();
    options.setBlockedThreadCheckInterval(100000000);
    Vertx vertx = Vertx.vertx(options);
    ServiceDiscovery discovery = null;
    String host = "132.239.10.190";
    int port = 5432;
    String user = "citadel";
    String pw = "citadel!";
    TimeseriesPostgisService tsService = new TimeseriesPostgisService(vertx, discovery, host, port, user, pw);
    double value = new Random().nextDouble() * 100;
    // long millis = System.currentTimeMillis();
    Long ts = 1517791709000L;

    double lng = -117.232959;
    double lat = 32.881607;
    String uuid = "a178d9f2-36cb-41f6-9c8a-1af17fb51b99";
    JsonArray data = new JsonArray();
    JsonObject datum = new JsonObject();
    datum.put("uuid", uuid);
    datum.put("timestamp", ts);
    datum.put("value", value);
    ArrayList<ArrayList<Double>> coordinates = new ArrayList<ArrayList<Double>>();
    ArrayList<Double> coordinate = new ArrayList<Double>();
    coordinate.add(lng);
    coordinate.add(lat);
    coordinates.add(coordinate);
    datum.put("geometryType", "point");
    datum.put("coordinates", coordinates);
    data.add(datum);
    tsService.insertData(data, ar -> {
      if (ar.failed()) {
        System.out.println("FAILED");
        System.out.println(ar.cause().getMessage());
      } else {
        System.out.println("Insertion done in Main");
      }
    });
    JsonObject queryJson = new JsonObject()
        .put("lng_min", lng - 1)
        .put("lng_max", lng + 1)
        .put("lat_min", lat - 1)
        .put("lat_max", lat + 1)
        .put("timestamp_min", ts - 1000000)
        .put("timestamp_max", ts + 1000000)
        .put("uuids",  new JsonArray().add(uuid));

    tsService.queryData(queryJson, new HashMap<String, String>(), res -> {
      if (res.succeeded()) {
        System.out.println(res.result());
      } else {
        System.out.println("FAILED!");
        res.cause().printStackTrace();
      }
    });
  }
}
