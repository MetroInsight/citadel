package metroinsight.citadel.common;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Util {
	static TimeZone tz = TimeZone.getTimeZone("UTC");
	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

	public Util () {
		df.setTimeZone(tz);
	}
	
	public static String date2str (Date date) {
		return df.format(date);
	}
	
	public static Date str2date (String dateStr) throws ParseException {
		return df.parse(dateStr);
	}
	
	public static Boolean validateDateStringFormat (String dateStr) {
		try {
			df.parse(dateStr);
			return true;
		} catch (ParseException e) {
			return false;
		}
	}

}
