package metroinsight.citadel.metadata.impl;

import static metroinsight.citadel.metadata.MetadataService.ADDRESS;
import static metroinsight.citadel.metadata.MetadataService.EVENT_ADDRESS;

import io.vertx.serviceproxy.ProxyHelper;
import metroinsight.citadel.common.MicroServiceVerticle;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.virtualsensor.VirtualSensorService;

public class MetadataVerticle extends MicroServiceVerticle {
  
  @Override
 public void start() {
    super.start();
    MetadataService service = new VirtuosoService(vertx, discovery);
    ProxyHelper.registerService(MetadataService.class, vertx, service, ADDRESS);
    
    publishEventBusService("metadata", ADDRESS, VirtualSensorService.class, rh -> {
      if (rh.failed()) {
        rh.cause().printStackTrace();
      } else {
        System.out.println("Metadata service published: " + rh.succeeded());
      }
    });
    
    publishMessageSource("metadata-events", EVENT_ADDRESS, rh -> {
      if (rh.failed()) {
        rh.cause().printStackTrace();
      } else {
        System.out.println("Metadata event service published: " + rh.succeeded());
      }
    });
  }
}
