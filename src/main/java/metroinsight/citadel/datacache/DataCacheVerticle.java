package metroinsight.citadel.datacache;

import static metroinsight.citadel.datacache.DataCacheService.ADDRESS;

import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ProxyHelper;
import metroinsight.citadel.common.MicroServiceVerticle;
import metroinsight.citadel.datacache.impl.RedisDataCacheService;

// probably no need. Keep it until determined.
public class DataCacheVerticle extends MicroServiceVerticle {
  @Override
  public void start() {
    super.start();
    DataCacheService service = new RedisDataCacheService(vertx);
    ProxyHelper.registerService(DataCacheService.class, vertx, service, ADDRESS);
    
    publishEventBusService("datacahce", ADDRESS, DataCacheService.class, ar -> {
      if (ar.failed()) {
        ar.cause().printStackTrace();
      } else {
        System.out.println("Datacache service published");
      }
      
    });
    
  }

}
