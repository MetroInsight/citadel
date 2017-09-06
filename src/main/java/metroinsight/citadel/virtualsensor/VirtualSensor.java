package metroinsight.citadel.virtualsensor;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

public class VirtualSensor {
  String code;
  String languageType; //Follow file extension
  Long period;
  List<String> dependentUUIDs;
  String uuid;
  static String codeDir = "vs_codes/";
  String filename;

  public VirtualSensor(Long period, String uuid, String code, String languageType, List<String> dependentUUIDs) {
    this.uuid = uuid;
    this.languageType = languageType;
    this.period = period;
    this.dependentUUIDs = dependentUUIDs;
    this.filename = codeDir + uuid + "." + languageType;
    this.code = codePreprocessing(code);
    PrintWriter out = null;
    try {
      out = new PrintWriter(this.filename);
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    out.println(this.code);

  }
  
  public String codePreprocessing(String code) {
    return code;
  }
  
  public void exec() {
    
  }
  
  public void loop() {
    
  }

}
