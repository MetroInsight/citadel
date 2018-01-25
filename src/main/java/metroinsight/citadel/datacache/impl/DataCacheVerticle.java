package metroinsight.citadel.datacache.impl;

import static metroinsight.citadel.datacache.DataCacheService.ADDRESS;

import io.vertx.serviceproxy.ServiceBinder;
import metroinsight.citadel.common.MicroServiceVerticle;
import metroinsight.citadel.datacache.DataCacheService;

// probably no need. Keep it until determined.
public class DataCacheVerticle extends MicroServiceVerticle {
  @Override
  public void start() {
    super.start();
    String hostname = config().getString("datacache.redis.hostname");
    DataCacheService service = new RedisDataCacheService(vertx, hostname);
    new ServiceBinder(vertx)
      .setAddress(ADDRESS)
      .register(DataCacheService.class, service);
    
    publishEventBusService("datacahce", ADDRESS, DataCacheService.class, ar -> {
      if (ar.failed()) {
        ar.cause().printStackTrace();
      } else {
        System.out.println("Datacache service published");
      }
    });
    
  }

}
