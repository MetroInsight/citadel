package metroinsight.citadel;

import java.util.List;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult.Series;
import io.vertx.core.json.Json;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        InfluxDB influx = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
        String dbName = "citadel";
        Query query = new Query("SELECT * FROM \"873035a5-c55c-495f-905a-2cb213102473\"", dbName);
        List<Series> serieses = influx
                                  .query(query)
                                  .getResults()
                                  .get(0)
                                  .getSeries();
        String tsString = Json.encode(serieses);
        System.out.println(tsString);

    }
}
