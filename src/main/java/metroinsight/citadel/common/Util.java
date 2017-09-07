package metroinsight.citadel.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

final public class Util {
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
	
	public static List<List<String>> readCsvFile(String filename, char separators) {
	  List<List<String>> parsed = new LinkedList<List<String>>();
	  String csvFile = "resources/point_type.csv";
	  try {
	    Scanner scanner = new Scanner(new File(csvFile));
	    while (scanner.hasNext()) {
	      List<String> line = parseLine(scanner.nextLine(), ',');
	      System.out.println("Country [id= " + line.get(0) + ", code= " + line.get(1) + " , name=" + line.get(2) + "]");
	      }
	    scanner.close(); } catch (FileNotFoundException e) {
	      e.printStackTrace();
	    }
	  return parsed;
	}
	
	public static List<String> parseLine(String cvsLine, char separators) {

      List<String> result = new ArrayList<>();

      //if empty, return!
      if (cvsLine == null && cvsLine.isEmpty()) {
          return result;
      }

      StringBuffer curVal = new StringBuffer();
      boolean inQuotes = false;
      boolean startCollectChar = false;
      boolean doubleQuotesInColumn = false;

      char[] chars = cvsLine.toCharArray();

      for (char ch : chars) {

          if (inQuotes) {
              startCollectChar = true;
                  if (ch == '\"') {
                      if (!doubleQuotesInColumn) {
                          curVal.append(ch);
                          doubleQuotesInColumn = true;
                      }
                  } else {
                      curVal.append(ch);
                  }
          } else {
              if (ch == separators) {

                  result.add(curVal.toString());

                  curVal = new StringBuffer();
                  startCollectChar = false;

              } else if (ch == '\r') {
                  //ignore LF characters
                  continue;
              } else if (ch == '\n') {
                  //the end, break!
                  break;
              } else {
                  curVal.append(ch);
              }
          }

      }

      result.add(curVal.toString());

      return result;
  }
	
	public final static JsonObject getSubsetJson(JsonObject rawJson, List<String> keys) {
	  String key;
	  JsonObject targetJson = new JsonObject();
	  for (int i=0; i < keys.size(); i++) {
	    key = keys.get(i);
	    targetJson.put(key, rawJson.getJsonObject(key));
	  }
	  return targetJson;
	}
	
	public final static List<String> jsonArray2StringArray(JsonArray jsonArray) {
	  List<String> array = new ArrayList<String>();
	  for (int i = 0; i < jsonArray.size(); i++) {
	    array.add(jsonArray.getString(i));
	  }
	  return array;
	}

}
