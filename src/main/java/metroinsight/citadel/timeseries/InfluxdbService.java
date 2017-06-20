package metroinsight.citadel.timeseries;

import java.util.ArrayList;
import java.util.List;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import metroinsight.citadel.common.Util;
import metroinsight.citadel.model.Timeseries;

public class InfluxdbService implements TimeseriesService {
  private final Vertx vertx;
  private final InfluxDB influx;
  private final String dbname;

	public InfluxdbService (Vertx vertx) {
	  this.vertx = vertx;
	  dbname = "citadel";
	  influx = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
	}

	@Override
	public void getTimeseries(String srcid, String beginTimeStr, String endTimeStr, Handler<AsyncResult<List<Timeseries>>> rh) {
	  if (! Util.validateDateStringFormat(beginTimeStr)) {
			// TODO: Add exception case
		} 
		if (! Util.validateDateStringFormat(endTimeStr)) {
			// TODO: Add exception case
		} 
		String qStr = String.format(
				"select value from %s where time >= %s and time < %s",
				srcid, beginTimeStr, endTimeStr);
		Query q = new Query(qStr, dbname);
		List<Timeseries> seriesList = new ArrayList<Timeseries>();
		for (Result result : influx.query(q).getResults()) {
		  for (Series series : result.getSeries()) {
		    Timeseries ts = Json.decodeValue(series.toString(), Timeseries.class);
		    seriesList.add(ts);
		  }
		}
		rh.handle(Future.succeededFuture(seriesList));
	}

	@Override
	public void postTimeseries(JsonObject data, Handler<AsyncResult<Void>> rh) {

	}
}
