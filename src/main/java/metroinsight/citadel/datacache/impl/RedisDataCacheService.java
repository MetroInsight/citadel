package metroinsight.citadel.datacache.impl;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.op.RangeLimitOptions;
import metroinsight.citadel.datacache.DataCacheService;

public class RedisDataCacheService implements DataCacheService {
  RedisClient redis;
  
  public RedisDataCacheService(Vertx vertx, String hostname) {
    RedisOptions config = new RedisOptions().
        setHost(hostname);
    redis = RedisClient.create(vertx, config);
  }
  
  public void testTest(String uuid, JsonObject data, List<String> indexKeys, Handler<AsyncResult<Void>> rh) {
    // Store Cache
    redis.hmset(uuid, data, redisRh2 -> { //Note that redis.hmset coerce values to be string.
      if (redisRh2.succeeded()) {
        // Construct indices
        JsonObject indexJson = new JsonObject();
        Iterator<String> keyIter = indexKeys.iterator();
        String key;
        while (keyIter.hasNext()) {
          key = keyIter.next();
          indexJson.put(key, data.getValue(key));
        }
        upsertIndex(uuid, indexJson, indexRh -> {
          if (indexRh.succeeded()) {
            rh.handle(Future.succeededFuture());
          } else {
            rh.handle(Future.failedFuture(indexRh.cause()));
          }
        });
      } else {
        rh.handle(Future.failedFuture(redisRh2.cause()));
      }
    });
}

  public void upsertData(String uuid, JsonObject data, List<String> indexKeys, Handler<AsyncResult<Void>> rh) {
    try {
      Long currTs = data.getLong("timestamp");
      redis.hget(uuid, "timestamp", redisRh -> {
        if (redisRh.succeeded()) {
          String tempRes = redisRh.result();
          Long existingTs;
          if (tempRes == null) {
            existingTs = 0L;
          } else {
            existingTs = Long.parseLong(tempRes);
          }
          if (currTs >= existingTs) {
            // Store Cache
            redis.hmset(uuid, data, redisRh2 -> { //Note that redis.hmset coerce values to be string.
              if (redisRh2.succeeded()) {
                // Construct indices
                JsonObject indexJson = new JsonObject();
                Iterator<String> keyIter = indexKeys.iterator();
                String key;
                while (keyIter.hasNext()) {
                  key = keyIter.next();
                  indexJson.put(key, data.getValue(key));
                }
                upsertIndex(uuid, indexJson, indexRh -> {
                  if (indexRh.failed()) {
                    rh.handle(Future.failedFuture(indexRh.cause()));
                  }
                });
              } else {
                rh.handle(Future.failedFuture(redisRh2.cause()));
              }
            }); 
          }
          rh.handle(Future.succeededFuture());
        } else {
          System.out.println(redisRh.cause());
          rh.handle(Future.failedFuture(redisRh.cause()));
        }
      });
    } catch (Exception e){
      rh.handle(Future.failedFuture(e));
    }
  }
  
  public void upsertIndex(String uuid, JsonObject data, Handler<AsyncResult<Void>> rh) {
    // TODO Using ZREM and ZADD
    try {
      // Check if the current timestamp is more recent than the existing one.
      
      Iterator<String> keyIter = data.fieldNames().iterator();
      String key;
      Double value;
      while (keyIter.hasNext()) {
        key = keyIter.next();
        value = data.getDouble(key);
        /* ZADD seems to upsert by default. TODO: Validate this.
        redis.zrem(key,uuid, redisRh -> {
          if (redisRh.failed()) {
            rh.handle(Future.failedFuture(redisRh.cause()));
          }
        });
        */
        redis.zadd(key, value, uuid, redisRh -> {
          if (redisRh.failed()) {
            rh.handle(Future.failedFuture(redisRh.cause()));
          }
        });
      }
      rh.handle(Future.succeededFuture());      
    } catch (Exception e) {
      rh.handle(Future.failedFuture(e));
    }
    
  }
  
  public void getData(String uuid, List<String> fields, Handler<AsyncResult<JsonObject>> rh) {
    // This returns every values to be String. Proper type casting is necessary at the caller.
    redis.hmget(uuid, fields, res -> {
      if (res.succeeded()) {
        JsonArray values = res.result();
        JsonObject datum = new JsonObject();
        for (int i=0; i<fields.size(); i++) {
          String field = fields.get(i);
          String value = values.getString(i);
          datum.put(field,  value);
        }
        rh.handle(Future.succeededFuture(datum));
      }
      
    });
  }
  
  public void getDatum(String uuid, Handler<AsyncResult<JsonObject>> rh) {
    redis.hgetall(uuid, redisRh -> {
      if (redisRh.succeeded()) {
        rh.handle(Future.succeededFuture(redisRh.result()));
      } else {
        rh.handle(Future.failedFuture(redisRh.cause()));
      }
    });
  }
  
  private Future<JsonObject> getDatumFuture(String uuid) {
    Future<JsonObject> fut = Future.future();
    getDatum(uuid, rh -> {
      if (rh.succeeded()) {
        JsonObject temp = rh.result();
        temp.put("uuid", uuid);
        fut.complete(temp);
      } else {
        fut.fail(rh.cause());
      }
    });
    return fut;
  }

  public void bboxQuery(Double minLng, Double maxLng, Double minLat, Double maxLat, Handler<AsyncResult<JsonArray>> rh) {
    rangeQuery("lng", minLng, maxLng, lngRh -> {
      if (lngRh.succeeded()) {
        JsonArray lngRes = lngRh.result();
        rangeQuery("lat", minLat, maxLat, latRh -> {
          if (latRh.succeeded()) {
            JsonArray latUuids = latRh.result();
            JsonArray lngUuids = lngRh.result();
            String uuid;
            List<Future> resFutures = new LinkedList<Future>();
            for (int i=0; i < latUuids.size(); i++) {
              uuid = latUuids.getString(i);
              if (lngUuids.contains(uuid)) {
                resFutures.add(getDatumFuture(uuid));
              }
            }
            CompositeFuture.all(resFutures).setHandler(resRh -> {
              if (resRh.succeeded()) {
                Iterator<Future> futIter = resFutures.iterator();
                JsonArray res = new JsonArray();
                while (futIter.hasNext()) {
                  Future fut = futIter.next();
                  JsonObject temp = (JsonObject) fut.result();
                  res.add(temp);
                }
                rh.handle(Future.succeededFuture(res));
              } else {
                rh.handle(Future.failedFuture(resRh.cause()));
              }
            });
          } else {
            rh.handle(Future.failedFuture(lngRh.cause()));
          }
        });
      } else {
        rh.handle(Future.failedFuture(lngRh.cause()));
      }
    });
  }

  public void rangeQuery(String key, Double minVal, Double maxVal, Handler<AsyncResult<JsonArray>> rh) {
    RangeLimitOptions limitOpt = new RangeLimitOptions();
    redis.zrangebyscore(key, minVal.toString(), maxVal.toString(), limitOpt, redisRh -> {
      if (redisRh.succeeded()) {
        rh.handle(Future.succeededFuture(redisRh.result()));
      } else {
        rh.handle(Future.failedFuture(redisRh.cause()));
      }
    });
  }
  
}
