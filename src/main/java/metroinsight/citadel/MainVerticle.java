package metroinsight.citadel;

import io.vertx.core.AbstractVerticle;
import metroinsight.citadel.timeseries.TimeseriesVerticle;

public class MainVerticle extends AbstractVerticle {
	
	@Override
	public void start() throws Exception {
	  // Deploy verticles.
    //vertx.deployVerticle(MetadataVerticle.class.getName());
    //vertx.deployVerticle(TimeseriesVerticle.class.getName());
    vertx.deployVerticle(RestApiVerticle.class.getName());
	}

}
