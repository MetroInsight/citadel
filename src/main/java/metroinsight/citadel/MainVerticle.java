package metroinsight.citadel;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import metroinsight.citadel.metadata.impl.MetadataVerticle;
import metroinsight.citadel.virtualsensor.impl.VirtualSensorVerticle;

public class MainVerticle extends AbstractVerticle {
	
    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(RestApiVerticle.class.getName());
    }
    
	@Override
	public void start() throws Exception {
	  // Deploy verticles.
    vertx.deployVerticle(MetadataVerticle.class.getName());
    vertx.deployVerticle(VirtualSensorVerticle.class.getName());
    //vertx.deployVerticle(TimeseriesVerticle.class.getName());
    vertx.deployVerticle(RestApiVerticle.class.getName());
	}

}
