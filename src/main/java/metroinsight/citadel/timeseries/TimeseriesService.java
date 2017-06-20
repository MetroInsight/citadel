package metroinsight.citadel.timeseries;

import java.util.List;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.model.Timeseries;

@ProxyGen
@VertxGen
public interface TimeseriesService {
	
	String ADDRESS = "service.timeseries";
	String EVENT_ADDRESS = "timeseries";
	
	static public TimeseriesService create(Vertx vertx) {
		return new InfluxdbService(vertx);
	}
	
	static public TimeseriesService createProxy(Vertx vertx, String address) {
		return new TimeseriesServiceVertxEBProxy(vertx, address);
	}
	
	void getTimeseries(String srcid, String beginTimeStr, String endTimeStr, 
	    Handler<AsyncResult<List<Timeseries>>> rh);
	
	void postTimeseries(JsonObject data, Handler<AsyncResult<Void>> rh);

}
