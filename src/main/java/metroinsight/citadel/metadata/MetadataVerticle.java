package metroinsight.citadel.metadata;

import static metroinsight.citadel.metadata.MetadataService.ADDRESS;

import java.util.Set;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.types.EventBusService;
import io.vertx.serviceproxy.ProxyHelper;
import metroinsight.citadel.common.MicroServiceVerticle;
import metroinsight.citadel.metadata.impl.MongoService;

public class MetadataVerticle extends MicroServiceVerticle {

  @Override
  public void start(){
  	super.start();
    // Init Mongo DB Instance
  	System.out.println("METADATA_VERTICLE STARTED");

    // Register and publish metadata service
    MetadataService metadataService = new MongoService(vertx);
    ProxyHelper.registerService(MetadataService.class, vertx, metadataService, ADDRESS);
    publishEventBusService("metadata", ADDRESS, MetadataService.class, ar -> {
    	if (ar.succeeded()) {
    		System.out.println("Metadata service published : " + ar.succeeded());
    	} else {
        ar.cause().printStackTrace();
      }
    });
  }

}
