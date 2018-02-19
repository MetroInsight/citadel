package metroinsight.citadel.federation.impl.MesoWest;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.serviceproxy.ServiceProxyBuilder;
import metroinsight.citadel.datacache.DataCacheService;
import metroinsight.citadel.federation.SourceConnector;
import metroinsight.citadel.metadata.MetadataService;

public class MesoWestConnector implements SourceConnector {
  Vertx vertx;
  WebClient client;
  String baseUrl = "api.mesowest.net";
  String SOURCE_NAME = "mesowest";
  String ADDRESS = "service.federation.";
  String EVENT_ADDRESS = "federation.";
  DataCacheService cacheService;
  MetadataService metadataService;
  String postfixAuth = "/v2/auth";
  String postfixMetadata = "/v2/stations/metadata";
  String postfixTs = "/v2/stations/timeseries";
  SimpleDateFormat dtFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
  SimpleDateFormat queryDtFormatter = new SimpleDateFormat("yyyyMMddHHmm");
  Map<String, String> uuidMap = new HashMap<String, String>();
  Map<String, String> unitMap = new HashMap<String, String>();
  Map<String, String> pointTypeMap = new HashMap<String, String>();
  Map<String, JsonObject> stationMap = new HashMap<String, JsonObject>();
  String masterToken;
  String masterApikey;
  MesoWestAccounts accounts;

  public MesoWestConnector (Vertx vertx) {
    this.ADDRESS += SOURCE_NAME;
    this.EVENT_ADDRESS += SOURCE_NAME;
    this.vertx = vertx;
    WebClientOptions options = new WebClientOptions()
        .setSsl(true)
        .setDefaultHost(baseUrl)
        .setDefaultPort(443);
    client = WebClient.create(vertx, options);
    ServiceProxyBuilder builder = new ServiceProxyBuilder(vertx).setAddress(MetadataService.ADDRESS);
    metadataService = builder.build(MetadataService.class);
    builder = new ServiceProxyBuilder(vertx).setAddress(DataCacheService.ADDRESS);
    cacheService = builder.build(DataCacheService.class);
    masterApikey = "Kzb7WQKff31ptrgHwamRP5r8iwgDn0k2kCj"; // TODO: Parameterize this
    initMetadata();
    accounts = new MesoWestAccounts(vertx);
  }
  
  public void test(Handler<AsyncResult<Void>> rh) {
    String id = "jbkoh@eng.ucsd.edu";
    accounts.addMesoWestUser(id, masterApikey, res -> {
      if (res.succeeded()) {
        System.out.println("SUCCEEDED at storing!!");
        accounts.getApikey(id, res2 -> {
          if (res2.succeeded()) {
            System.out.println("SUCCEEDED at retrieving");
            rh.handle(Future.succeededFuture());
          } else {
            rh.handle(Future.failedFuture(res2.cause().getMessage()));
          }
          });
      } else {
        rh.handle(Future.failedFuture(res.cause().getMessage()));
      }
    });
    
  }
  
  private void initMetadata() {
    getToken(masterApikey, tokenRes -> {
      if (tokenRes.succeeded()) {
        masterToken = tokenRes.result();
        client
          .get("/v2/variables")
          .addQueryParam("token", masterToken)
          .send(res -> {
            if (res.succeeded()) {
              JsonArray variables = res.result().bodyAsJsonObject().getJsonArray("VARIABLES");
              for (int i=0; i<variables.size(); i++) {
                JsonObject variable = variables.getJsonObject(i);
                String pointName = (String) variable.fieldNames().toArray()[0];
                JsonObject point = variable.getJsonObject(pointName);
                String unit = "NoUnit";
                if (point.containsKey("unit")) {
                  unit = point.getString("unit");
                }
                unitMap.put(pointName, unit);
                pointTypeMap.put(pointName, point.getString("long_name"));
              }
            } else {
              // TODO: Properly handle exception
              // TODO: Put logger generating ERROR
              res.cause().printStackTrace();
            }
          });
      } else {
              // TODO: Properly handle exception
              // TODO: Put logger generating ERROR
        tokenRes.cause().printStackTrace();
      }
    });
  }
  
  private void getToken(String apikey, Handler<AsyncResult<String>> rh) {
    client
      .get("/v2/auth").ssl(true)
      .setQueryParam("apikey", apikey)
      .send(res -> {
        if (res.succeeded()) {
          try {
            String token = res.result().bodyAsJsonObject().getString("TOKEN");
            rh.handle(Future.succeededFuture(token));
          } catch (Exception e) {
            rh.handle(Future.failedFuture(e.getMessage()));
          }
        } else {
          rh.handle(Future.failedFuture(res.cause()));
        }
      });;
  }
  
  private String getSrcid(String station, String var) {
    return station + "_" + var;
  }
  
  private void getUserToken(String identity, Handler<AsyncResult<String>> rh) {
    cacheService.getUserServiceInfo(identity, SOURCE_NAME, res -> {
      if (res.succeeded()) {
        JsonObject sourceInfo = res.result();
        if (sourceInfo.containsKey("token")) {
          rh.handle(Future.succeededFuture(sourceInfo.getString("token")));
        } else {
          // TODO: Add a way to retrieve the apikey per user.
          getToken("Kzb7WQKff31ptrgHwamRP5r8iwgDn0k2kCj", tokenRes -> {
            if (tokenRes.succeeded()) {
              rh.handle(Future.succeededFuture(tokenRes.result()));
            } else {
              rh.handle(Future.failedFuture(tokenRes.cause()));
            }
          });
        }
      } else {
        rh.handle(Future.failedFuture(res.cause()));
      }
    });
  }
  
  private String mesoGetError(JsonObject body) {
    if (body.getJsonObject("SUMMARY").getInteger("RESPONSE_CODE") == -1) {
      return body.getString("RESPONSE_MESSAGE");
    } else {
      return "";
    }
  }
  
  private void mesoMetadataQuery(JsonObject query, String token, Handler<AsyncResult<JsonArray>> rh) {
    // TODO: Use cache first.
    String bboxStr = String.format("%f,%f,%f,%f",
        query.getDouble("min_lng"),
        query.getDouble("min_lat"),
        query.getDouble("max_lng"),
        query.getDouble("max_lat"));
    client
      .get(postfixMetadata)
      .addQueryParam("status", "active")
      .addQueryParam("bbox", bboxStr)
      .addQueryParam("token", token)
      .addQueryParam("sensorvars", "1")
      .send(metaRes -> {
        if (metaRes.succeeded()) {
          JsonObject body = metaRes.result().bodyAsJsonObject();
          String mesoError = mesoGetError(body);
          if ( mesoError != "") {
            rh.handle(Future.failedFuture(mesoError));
          } else {
            JsonArray aaa = body.getJsonArray("STATION");
            int size = aaa.size();
            try {
              rh.handle(Future.succeededFuture(aaa));
            } catch (Exception e) {
              String errrrr = e.getMessage();
              e.printStackTrace();
            }
          }
        } else {
          rh.handle(Future.failedFuture(metaRes.cause()));
        }
      });
  }
  
  private void mesoGetTimeseries(String stid, Set<String> vars, Long minTs, Long maxTs, String token, Handler<AsyncResult<JsonArray>> rh) {
    // This returns "STATION' JsonArray in the MesoWest's response. It list the result per station.
    client
    .get(postfixTs)
    .addQueryParam("stid", stid)
    .addQueryParam("start", queryDtFormatter.format(new Date(minTs)))
    .addQueryParam("end", queryDtFormatter.format(new Date(maxTs)))
    .addQueryParam("token", token)
    // TODO: This vars can be filtered by the user query. (e.g., UUIDs)
    .addQueryParam("vars", String.join(",", vars))
    .send(res -> {
      if (res.succeeded()) {
        rh.handle(Future.succeededFuture(res.result().bodyAsJsonObject().getJsonArray("STATION")));
      } else {
        rh.handle(Future.failedFuture(res.cause()));
      }
    });
  }
  
  private Future<JsonObject> getStationFuture(String stid) {
    Future<JsonObject> stationFut = Future.future();
    getStation(stid, res -> {
      if (res.succeeded()) {
        stationFut.complete(res.result());
      } else {
        // TODO: error handling
        stationFut.fail(res.cause());
      }
    });
    return stationFut;
  }
  
  private void getStation(String stid, Handler<AsyncResult<JsonObject>> rh) {
    if (stationMap.containsKey(stid)) {
      rh.handle(Future.succeededFuture(stationMap.get(stid)));
    } else {
      client
        .get(postfixMetadata)
        .addQueryParam("token", masterToken)
        .addQueryParam("stid", stid)
        .send(res -> {
          if (res.succeeded()) {
            JsonObject station = res.result().bodyAsJsonObject().getJsonArray("STATION").getJsonObject(0);
            stationMap.put(stid, station);
            rh.handle(Future.succeededFuture(station));
          } else {
            rh.handle(Future.failedFuture(res.cause()));
          }
        });
    }
  }
  
  private void getUuid(String stid, String pointName, Handler<AsyncResult<String>> rh) {
    String srcid = getSrcid(stid, pointName);
    if (uuidMap.containsKey(srcid)) {
      rh.handle(Future.succeededFuture(uuidMap.get(srcid)));
    } else {
      //JsonObject q = new JsonObject().put("station", stid).put("pointType", pointName);
      JsonObject q = new JsonObject().put("name", srcid);
      metadataService.queryPoint(q, res -> {
        if (res.succeeded()) {
          JsonArray points = res.result();
          if (points.size() > 0) {
            String uuid = points.getString(0);
            uuidMap.put(srcid, uuid);
            rh.handle(Future.succeededFuture(uuid));
          } else {
            JsonObject point = new JsonObject()
                .put("unit", unitMap.get(pointName))
                .put("pointType", pointTypeMap.get(pointName))
                .put("name", srcid)
                .put("station", stid);
            metadataService.createPoint(point, createRes-> {
              if (createRes.succeeded()) {
                String uuid = createRes.result();
                uuidMap.put(srcid, uuid);
                rh.handle(Future.succeededFuture(uuid));
              } else {
                rh.handle(Future.failedFuture(createRes.cause()));
              }
            });
          }
        } else {
          rh.handle(Future.failedFuture(res.cause()));
        }
      });
      
    }
  }
  
  private void convStationTstoJsonArray(JsonObject station, JsonObject tsData, BiMap<String, String> varMap, Handler<AsyncResult<JsonArray>> rh) {
    List<Future> futList = new LinkedList<Future> ();
    JsonArray times = new JsonArray();
    tsData.getJsonArray("date_time").stream().forEach(s -> {
      try {
        times.add(dtFormatter.parse((String) s).getTime());
      } catch (Exception e) {
        rh.handle(Future.failedFuture(e));
      }
    });
    Double lng = Double.parseDouble(station.getString("LONGITUDE"));
    Double lat = Double.parseDouble(station.getString("LATITUDE"));
    JsonArray cds = new JsonArray().add(new JsonArray().add(lng).add(lat));
    String stid = station.getString("STID");
    for (String k: tsData.fieldNames()) {
      if (k.equals("date_time")) continue;
      Future<JsonArray> fut = Future.future();
      futList.add(fut);
      JsonObject datum = new JsonObject();
      String pointName = varMap.inverse().get(k);
      getUuid(stid, pointName, uuidres -> { // TODO: CurrArray is empty!!!! cause asynch.
        if (uuidres.succeeded()) {
          JsonArray currArray = new JsonArray();
          String uuid = uuidres.result();
          datum
            .put("geometryType", "point")
            .put("coordinates", cds)
            .put("uuid", uuid);
          JsonArray oneData = tsData.getJsonArray(k);
          for (int j=0; j<times.size(); j++) {
            Double value = null;
            Object val = oneData.getValue(j);
            try {
              value = (Double) val;
            } catch (ClassCastException ce) {
              continue;
            } catch (Exception e) {
              fut.fail(e.getMessage());
              return;
            }
            Long t = times.getLong(j);
            currArray.add(datum.copy().put("value", value).put("timestamp", t));
          }
          fut.complete(currArray);
        } else {
          fut.fail(uuidres.cause());
          rh.handle(Future.failedFuture(uuidres.cause()));
        }
      });
    }

    CompositeFuture.all(futList).setHandler(res -> {
      if (res.succeeded()) {
        JsonArray totalArray = new JsonArray();
        List<Object> allResults = res.result().list();
        for (int i=0; i<allResults.size(); i++) {
          totalArray.addAll((JsonArray) allResults.get(i));
        }
        rh.handle(Future.succeededFuture(totalArray));
      } else {
        rh.handle(Future.failedFuture(res.cause()));
      }
    });
  }
  
  private void convMesoResultToJsonArray(JsonArray datas, Handler<AsyncResult<JsonArray>> rh) {
    List<Future> futList = new LinkedList<Future> ();
    for (int i=0; i<datas.size(); i++) {
      Future<JsonArray> fut = Future.future();
      futList.add(fut);
      JsonObject data = datas.getJsonObject(i);
      String stid = data.getString("STID");
      //JsonObject station = getStationBlocking(stid);
      getStation(stid, stFut -> {
        if (stFut.succeeded()) {
          JsonObject station = stFut.result();
          JsonArray currArray = new JsonArray();
          JsonObject vars = data.getJsonObject("SENSOR_VARIABLES");
          BiMap<String, String> varMap = HashBiMap.create(); // TODO: This is slow.
          for (String k : vars.fieldNames()) {
            String srcName = vars.getJsonObject(k).fieldNames().iterator().next();
            varMap.put(k, srcName);
          }
          JsonObject tsData = data.getJsonObject("OBSERVATIONS");
          convStationTstoJsonArray(station, tsData, varMap, tsFut -> {
            if (tsFut.succeeded()) {
              fut.complete(tsFut.result());
            } else {
              rh.handle(Future.failedFuture(tsFut.cause().getMessage()));
            }
          });
        } else {
          rh.handle(Future.failedFuture(stFut.cause().getMessage()));
        }
      });
    }

    CompositeFuture.all(futList).setHandler(res -> {
      if (res.succeeded()) {
        JsonArray totalArray = new JsonArray();
        List<Object> allresults = res.result().list();
        for (int i=0; i<allresults.size(); i++) {
          totalArray.addAll((JsonArray) allresults.get(i));
        }
        rh.handle(Future.succeededFuture(totalArray));
      } else {
        rh.handle(Future.failedFuture(res.cause()));
      }
    });
  }
  
  @Override
  public void queryData(JsonObject query, String identity, Handler<AsyncResult<JsonArray>> rh) {
    getUserToken(identity, tokenRes -> {
      if (tokenRes.succeeded()) {
        String token = tokenRes.result();
        mesoMetadataQuery(query, token, metaRes -> {
          if (metaRes.succeeded()) {
            List<Future> futList = new LinkedList<Future> ();
            JsonArray stations = metaRes.result();
            for (int i=0; i<stations.size(); i++) {
              Future<JsonArray> fut = Future.future();
              futList.add(fut);
              JsonObject station = stations.getJsonObject(i);
              String stid = station.getString("STID");
              Set<String> vars = station.getJsonObject("SENSOR_VARIABLES").fieldNames();
              mesoGetTimeseries(stid, vars, query.getLong("timestamp_min"), query.getLong("timestamp_max"), token, tsRes -> {
                if (tsRes.succeeded()) {
                  JsonArray tsPerStation = tsRes.result();
                  convMesoResultToJsonArray(tsPerStation, convRes -> {
                    if (convRes.succeeded()) {
                      //rh.handle(Future.succeededFuture(convRes.result()));
                      fut.complete(convRes.result());
                    } else {
                      //rh.handle(Future.failedFuture(convRes.cause()));
                      fut.fail(convRes.cause().getMessage());
                    }
                  });
                } else {
                  //rh.handle(Future.failedFuture(tsRes.cause()));
                  fut.fail(tsRes.cause());
                }
              });
            }
            
            CompositeFuture.all(futList).setHandler(res -> {
              if (res.succeeded()) {
                JsonArray totalArray = new JsonArray();
                List<Object> allresults = res.result().list();
                for (int i=0; i<allresults.size(); i++) {
                  totalArray.addAll((JsonArray) allresults.get(i));
                  }
                rh.handle(Future.succeededFuture(totalArray));
              } else {
                rh.handle(Future.failedFuture(res.cause().getMessage()));
              }
            });
          } else {
            rh.handle(Future.failedFuture(metaRes.cause().getMessage()));
            return;
          }
        });
      } else {
        rh.handle(Future.failedFuture(tokenRes.cause()));
      }
      
    });
        
        
        
    /*
        Map<String, List<String>> stationVarMap = new HashMap<String, List<String>>();
        Map<String, String> uuidMap = new HashMap<String, String>();
        if (query.containsKey("uuids")) {
          JsonArray uuids = query.getJsonArray("uuids");
          Iterator<Object> uuidIter = uuids.iterator();
          while (uuidIter.hasNext()) {
            String uuid = (String) uuidIter.next();
            Future<JsonObject> pointFut = Future.future();
            metadataService.getPoint(uuid, pointRes -> {
              if (pointRes.succeeded()) {
                pointFut.complete(pointRes.result());
              } else {
                rh.handle(Future.failedFuture(pointRes.cause()));
              }
            });
            JsonObject point = pointFut.result();
            String var = point.getString("pointType");
            String stid = point.getString("station");
            List<String> vars = stationVarMap.getOrDefault(stid, new LinkedList<String>());
            vars.add(var);
            stationVarMap.put(stid, vars);
            uuidMap.put(getSrcid(stid, var), uuid);
          }
        }
        */

    /*
        client
          .get(postfixMetadata)
          .addQueryParam("status", "active")
          .addQueryParam("bbox", String.format("%f,%f,%f,%f", 
                                               query.getDouble("min_lat"),
                                               query.getDouble("max_lat"),
                                               query.getDouble("min_lng"),
                                               query.getDouble("max_lng")))
          .addQueryParam("token", token)
          .send(metaRes -> {
            JsonArray resArray = new JsonArray();
            if (metaRes.succeeded()) {
              JsonArray stations = metaRes.result().bodyAsJsonObject().getJsonArray("STATION");
              Iterator<Object> stIter = stations.iterator();
              List stFutures = new LinkedList<Future>();
              while (stIter.hasNext()) {
                JsonObject station = (JsonObject) stIter.next();
                String stid = station.getString("STID");
                HttpRequest<Buffer> tsBuf = client
                    .get(postfixTs)
                    .addQueryParam("stid", stid)
                    .addQueryParam("start", query.getString("timestamp_min"))
                    .addQueryParam("end", query.getString("timestamp_max"))
                    .addQueryParam("token", "TODOTODOTODO"); // TODO
                if (stationVarMap.containsKey(stid)) {
                  tsBuf.addQueryParam("vars", String.join(",", stationVarMap.get(stid)));
                }
                Double lat = station.getDouble("LATITUDE");
                Double lng = station.getDouble("LONGITUDE");
                JsonArray cds = new JsonArray().add(new JsonArray().add(lng).add(lat));
                Future<JsonArray> stationTsFut = Future.future();
                stFutures.add(stationTsFut);
                tsBuf.send(tsRes -> {
                  if (tsRes.succeeded()) {
                    JsonArray currArray = new JsonArray();
                    JsonObject tsBody = tsRes.result().bodyAsJsonObject();
                    JsonObject data = tsBody.getJsonArray("STATION").getJsonObject(0);
                    JsonObject sensorVars = data.getJsonObject("SENSOR_VARIABLES");
                    BiMap<String, String> varMap = HashBiMap.create();
                    for (String k : sensorVars.fieldNames()) {
                      varMap.put(k, (String) sensorVars.getString(k));
                    }
                    
                    JsonArray times = new JsonArray();
                    data.getJsonArray("date_time").stream().forEach(s -> {
                      try {
                        times.add(dtFormatter.parse((String) s).getTime());
                      } catch (ParseException e) {
                        // TODO Auto-generated catch block
                        rh.handle(Future.failedFuture(e));
                      }
                    });
                    for (String k: data.fieldNames()) {
                      JsonObject datum = new JsonObject();
                      String pointType = varMap.inverse().get(k);
                      datum.put("geometryType", "point");
                      datum.put("pointType", pointType);
                      datum.put("coordinates", cds);
                      String uuid = uuidMap.get(getSrcid(stid, pointType));
                      datum.put("uuid", uuid);
                      if (k.equals("date_time")) continue;
                      JsonArray oneData = data.getJsonArray(k);
                      for (int i=0; i<times.size(); i++) {
                        Double value = oneData.getDouble(i);
                        Long t = times.getLong(i);
                        oneData.add(datum.copy().put("value", value).put("timestamp", t));
                      }
                      currArray.add(oneData);
                    }
                    stationTsFut.complete(currArray);
                  }
                });
              }
              CompositeFuture.all(stFutures).setHandler(compRes -> {
                if (compRes.succeeded()) {
                  JsonArray finalArray = (JsonArray) stFutures.stream().reduce(new JsonArray(), (p1, p2) -> {
                    return ((JsonArray) ((Future) p1).result()).add((JsonArray) ((Future) p2).result());
                    });
                  rh.handle(Future.succeededFuture(finalArray));
                  // Finally success
                } else {
                  rh.handle(Future.failedFuture(compRes.cause()));
                }
              });
            } else {
              rh.handle(Future.failedFuture(metaRes.cause()));
            }
          });
        // TODO: Start here.
      } else {
        rh.handle(Future.failedFuture(res.cause()));
      }
    });
    */
  }

  public static void main(String[] args) {
    /*
    Vertx vertx = Vertx.vertx();
    MesoWestConnector meso = new MesoWestConnector(vertx);
    JsonObject query = new JsonObject()
        .put("min_lng", -117.257502)
        .put("min_lat", 32.826226)
        .put("max_lng", -117.132364)
        .put("max_lng", 32.966266)
        .put("timestamp_min", 1517443200000L)
        .put("timestamp_max", 1518307721000L);
        
    meso.queryData(query, "abcdfg", ar -> {
      if (ar.succeeded()) {
        System.out.println(ar.result());
      } else {
        ar.cause().printStackTrace();
      }
    });
    */
    WebClient c = WebClient.create(Vertx.vertx());
    try {
    c.get(443, "api.mesowest.net", "/v2/auth").ssl(true)
    .addQueryParam("apikey", "Kzb7WQKff31ptrgHwamRP5r8iwgDn0k2kCj")
    .send(res -> {
      if (res.succeeded()) {
        try {
          System.out.println("!!!!!!!!!!!!!");
          String token = res.result().bodyAsJsonObject().getString("TOKEN");
          System.out.println(token);
        } catch (Exception e) {
          System.out.println("??????");
          e.printStackTrace();
        }
      } else {
        res.cause().printStackTrace();
        System.out.println("nonononono");
      }
    });
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("never");
    }
    System.out.println("zzzzzzzzzzzzzzzzz");
  }
}







