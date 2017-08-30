package metroinsight.citadel;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;

public class MainVerticle extends AbstractVerticle {
	
	@Override
	public void start() throws Exception {
	  // Deploy verticles.
    //vertx.deployVerticle(MetadataVerticle.class.getName());
    //vertx.deployVerticle(TimeseriesVerticle.class.getName());
    
		//old deployed
		//vertx.deployVerticle(RestApiVerticle.class.getName());
		
		DeploymentOptions opts = new DeploymentOptions()
	            .setWorker(true);
		
    vertx.deployVerticle(RestApiVerticle.class.getName(),opts);
    
    
	}

}
