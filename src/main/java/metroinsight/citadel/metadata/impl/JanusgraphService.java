package metroinsight.citadel.metadata.impl;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.example.GraphOfTheGodsFactory;

public class JanusgraphService{// implements MetadataService  {
//  private final Vertx vertx;
  static JanusGraph graph;
  //static Client client;
  
  public JanusgraphService() {//Vertx vertx) {
 //   this.vertx = vertx;
    graph = JanusGraphFactory.open("/home/jbkoh/tools/janusgraph-0.1.1-hadoop2/conf/janusgraph-hbase.properties");    
    GraphTraversalSource g = graph.traversal();
    if (g.V().count().next() == 0) {
      // load the schema and graph data
      GraphOfTheGodsFactory.load(graph);
  }
    /*graph = JanusGraphFactory.build().
        set("storage.backend", "berkeleyje").
        set("storage.directory", "/data/graph").
        open();
        */
    /*
    try {
      cluster = Cluster.open("/home/jbkoh/repo/citadel/src/main/resources/configs/remote.yaml");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    client = cluster.connect();
    // Init schema file here
     */
  }
  /*
  
  @Override
  public void queryPoint(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
  }

  @Override
  public void getPoint(String uuid, Handler<AsyncResult<Metadata>> resultHandler) {
  }
  
  @Override
  public void createPoint(JsonObject jsonMetadata, Handler<AsyncResult<String>> resultHandler) {
    String pointType = jsonMetadata.getString("pointType");
    // Schema should be loaded beforehand but here upsert manually
    Vertex pointTypeV = graph.traversal().V(pointType).next();
    
    String unit = jsonMetadata.getString("unit");
    String uuid = UUID.randomUUID().toString();
    String name = jsonMetadata.getString("name");
    Vertex v = graph.addVertex("ex:" + uuid);
//    v.addEdge("rdf:type", pointType);

  }*/
  
  public static void main(String[] args) {	
    //client.submit("graph.addVertex(T.label,'x','name','tom')");
    JanusgraphService janus = new JanusgraphService();
    graph.addVertex("name", "TESTTT");
    System.out.println("HELLO World");
  }

}
