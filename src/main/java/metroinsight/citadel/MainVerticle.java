package metroinsight.citadel;

import io.vertx.core.AbstractVerticle;
import metroinsight.citadel.metadata.MetadataVerticle;
import metroinsight.citadel.timeseries.TimeseriesVerticle;

public class MainVerticle extends AbstractVerticle {
	
	@Override
	public void start() throws Exception {
    vertx.deployVerticle(MetadataVerticle.class.getName());
    vertx.deployVerticle(TimeseriesVerticle.class.getName());
    vertx.deployVerticle(RestApi.class.getName());
	}

}
