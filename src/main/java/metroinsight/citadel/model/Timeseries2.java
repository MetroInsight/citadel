package metroinsight.citadel.model;

import java.util.concurrent.atomic.AtomicInteger;

public class Timeseries2 {
  private static final AtomicInteger COUNTER = new AtomicInteger();
  private final int id;
  private String name;
  private String origin;

  public Timeseries2(String name, String origin) {
    this.id = COUNTER.getAndIncrement();
    this.name = name;
    this.origin = origin;
  }
  
  public Timeseries2(){
    this.id = COUNTER.getAndIncrement();
  }
  
  public String getName() {
    return name;
  }
  
  public String getOrigin() {
    return origin;
  }
  
  public int getId() {
    return id;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public void setOrigin(String origin) {
    this.origin = origin;
  }

}
