package metroinsight.citadel.metadata.impl;

import static metroinsight.citadel.metadata.MetadataService.ADDRESS;
import static metroinsight.citadel.metadata.MetadataService.EVENT_ADDRESS;

import io.vertx.serviceproxy.ProxyHelper;
import io.vertx.serviceproxy.ServiceBinder;
import metroinsight.citadel.common.MicroServiceVerticle;
import metroinsight.citadel.metadata.MetadataService;

public class MetadataVerticle extends MicroServiceVerticle {
  
  @Override
 public void start() {
    super.start();
    //MetadataService service = new VirtuosoRdf4jService(vertx, 
    MetadataService service = new VirtuosoService(vertx, 
                                                  config().getString("metadata.virt.hostname"), 
                                                  config().getInteger("metadata.virt.port"), 
                                                  config().getString("metadata.virt.graphname"),
                                                  config().getString("metadata.virt.username"),
                                                  config().getString("metadata.virt.password"),
                                                  discovery);
    new ServiceBinder(vertx)
      .setAddress(ADDRESS)
      .register(MetadataService.class, service);
    
    publishEventBusService("metadata", ADDRESS, MetadataService.class, rh -> {
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
