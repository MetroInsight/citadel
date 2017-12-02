package metroinsight.citadel;

import metroinsight.citadel.data.impl.GeomesaHbase;

public class InitConfigs {

  public static void main(String[] args) {
    // TODO Auto-generated method stub
      GeomesaHbase gmh = new GeomesaHbase();
      gmh.geomesa_initialize();
  }
}
