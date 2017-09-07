package metroinsight.citadel.metadata.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.ServiceDiscovery;
import metroinsight.citadel.common.ErrorMessages;
import metroinsight.citadel.metadata.MetadataService;
import metroinsight.citadel.model.Metadata;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

public class VirtuosoService implements MetadataService  {
  private final Vertx vertx;
  private final ServiceDiscovery discovery;

  static VirtGraph graph = null;
  
  // Prefixes
  final String CITADEL = "http://metroinsight.io/citadel#";
  final String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
  final String RDFS = "http://www.w3.org/2000/01/rdf-schema#";
  final String EX = "http://example.com#";
  
  Map<String, Node> prefixMap;
  
  // TODO: Maybe add a map between uesr-prop to rdf property. e.g., "pointType" -> rdf:type.
  
  // Common Variables
  Node a;
  Node hasUnit;
  Node hasName;
  
  //Units
  List<String> units;
  List<String> types;
  
  private void initSchema() {
    // Init common variables
    units = new ArrayList<String>();
    types = new ArrayList<String>();

    a = NodeFactory.createURI(RDF + "type");
    hasUnit = NodeFactory.createURI(CITADEL + "unit");
    hasName = NodeFactory.createURI(CITADEL + "name");
    
    // Init Namespace Map
    // This may be automated once we get a schema file (in Turtle).
    prefixMap = new HashMap<String, Node>();
    prefixMap.put("pointType", NodeFactory.createURI(RDF + "type"));
    prefixMap.put("subClassOf", NodeFactory.createURI(RDFS + "subClassOf"));
    prefixMap.put("unit", NodeFactory.createURI(CITADEL + "unit"));
    prefixMap.put("name", NodeFactory.createURI(EX + "name"));
    
    // Init units
    // types
    List<String> citadelTypes = new ArrayList<String>(units);
    citadelTypes.addAll(types);
    for (int i=0; i < citadelTypes.size(); i++) {
      String v = citadelTypes.get(i);
      prefixMap.put(v, NodeFactory.createURI(CITADEL + v));
    }

  }
  
  private Node addPrefix(String prop) {
    return prefixMap.getOrDefault(prop, NodeFactory.createURI(CITADEL + prop));
  }
  
  public VirtuosoService(Vertx vertx) {
    this.vertx = vertx;
    this.discovery = ServiceDiscovery.create(vertx);
    if (graph==null) {
      graph = new VirtGraph("citadel", "jdbc:virtuoso://localhost:1111", "dba", "dba");
    }
    initSchema();
  }
  
  public VirtuosoService(Vertx vertx, ServiceDiscovery discovery) {
    this.vertx = vertx;
    this.discovery = discovery;
    if (graph==null) {
      graph = new VirtGraph("citadel", "jdbc:virtuoso://localhost:1111", "dba", "dba");
    }
    initSchema();
  }
  
  @Override
  public void queryPoint(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
    try {
      ParameterizedSparqlString pss = getDefaultPss();
      String qStr = "SELECT ?s WHERE {\n";
      //pss.setCommandText("SELECT ?s WHERE ");
      for (int i=0; i< query.fieldNames().size(); i++) {
        qStr += "?s ? ? . \n";
      }
      qStr += "}";
      pss.setCommandText(qStr);
      Set<String> keys = query.fieldNames();
      Iterator<String> keyIter = keys.iterator();
      int i = 0;
      while (keyIter.hasNext()) {
        String key = keyIter.next();
        String value = query.getString(key);
        if (key.equals("pointType")) { // TODO: Use map to organize below.
          key = RDF + "type";
          value = CITADEL + value;
        } else if (key.equals("name")) {
          key = CITADEL + key;
          value = EX + value;
        } else {
          key = CITADEL + key;
          value = CITADEL + value;
        }
        pss.setIri(i * 2, key);
        pss.setIri(i * 2 + 1, value);
        i += 1;
      }
      ResultSet results = sparqlQuery(pss.toString());
      JsonArray uuids = new JsonArray();
      while (results.hasNext()) {
        String uuid = results.nextSolution().get("s").toString().split("#")[1];
        uuids.add(uuid);
      }
      resultHandler.handle(Future.succeededFuture(uuids));
    }catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void getPoint(String uuid, Handler<AsyncResult<Metadata>> resultHandler) {
    try {
      String qStr = "SELECT ?p ?o WHERE {?s ?p ?o}";
      ParameterizedSparqlString pss = getDefaultPss();
      pss.setCommandText(qStr);
      pss.setIri("s", EX + uuid);
      ResultSet results = sparqlQuery(pss.toString());
      if (!results.hasNext()) {
        resultHandler.handle(Future.failedFuture("Not existing UUID"));
      } else {
        JsonObject jsonMetadata = new JsonObject();
        //TODO: Align this JSON to metadata.
        while (results.hasNext()) {
          QuerySolution result = results.nextSolution();
          String p = result.get("p").toString().split("#")[1];
          String o = result.get("o").toString().split("#")[1];
          if (p.equals("type")) {
            p = "pointType";
          }
          jsonMetadata.put(p, o);
        }
        jsonMetadata.put("uuid", uuid);
        Metadata metadata = jsonMetadata.mapTo(Metadata.class); // Validation // TODO: Not working. FIX!
        System.out.println(metadata.toJson());
        resultHandler.handle(Future.succeededFuture(metadata));
      }
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }
  
  private ParameterizedSparqlString getDefaultPss() {
      ParameterizedSparqlString pss = new ParameterizedSparqlString();
      pss.setBaseUri(EX);
      pss.setNsPrefix("ex", EX);
      pss.setNsPrefix("rdf", RDF);
      pss.setNsPrefix("rdfs", RDFS);
      pss.setNsPrefix("citadel", CITADEL);
      return pss;
  }

  @Override
  public void createPoint(JsonObject jsonMetadata, Handler<AsyncResult<String>> resultHandler) {
    try {
      // Check if it already exists
      ParameterizedSparqlString pss = getDefaultPss();
      pss.setCommandText("select ?s where {?s citadel:name ?name .}");
      String nameStr = jsonMetadata.getString("name");
      Node name = NodeFactory.createURI(EX + nameStr); // TODO: Change name to Literal later
      //Node name = NodeFactory.createLiteral(nameStr);
      pss.setParam("name", name);
      String qStr = pss.toString();
      ResultSet res = sparqlQuery(pss.toString());
      if (res.hasNext()) {
        resultHandler.handle(Future.failedFuture(ErrorMessages.EXISTING_POINT_NAME));
      } else {
        // Create the point
        String uuid = UUID.randomUUID().toString();
        Node point = NodeFactory.createURI(EX + uuid);
        Node pointType = NodeFactory.createURI(CITADEL + jsonMetadata.getString("pointType"));
        graph.add(new Triple(point, a, pointType));
        Node unit = NodeFactory.createURI(CITADEL + jsonMetadata.getString("unit"));
        graph.add(new Triple(point, hasUnit, unit));
        graph.add(new Triple(point, hasName, name));
        resultHandler.handle(Future.succeededFuture(uuid));
      }
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void upsertMetadata(String uuid, JsonObject newMetadata, Handler<AsyncResult<Void>> rh) {
    try {
      Node point = NodeFactory.createURI(EX + uuid);
      Iterator<String> keys = newMetadata.fieldNames().iterator();
      while (keys.hasNext()) {
        String key = keys.next();
        Node prop = addPrefix(key);
        Object value = newMetadata.getValue(key);
        if (value instanceof List) { // TODO: Check this is working. If not working, use try catch.
          Iterator<String> valueIter = ((List<String>) value).iterator();
          while (valueIter.hasNext()) {
            graph.add(new Triple(point, prop, addPrefix(valueIter.next())));
          }
        } else if (value instanceof String) {
            graph.add(new Triple(point, prop, addPrefix((String) value)));
        }
      }
      rh.handle(Future.succeededFuture());
    } catch (Exception e) {
      rh.handle(Future.failedFuture(e));
    }
  }


  private ResultSet sparqlQuery(String qStr) {
    Query sparql = QueryFactory.create(qStr);
    VirtuosoQueryExecution vqd = VirtuosoQueryExecutionFactory.create(sparql, graph);
    return vqd.execSelect();
  }
  
  public static void main(String[] args) {	
    //client.submit("graph.addVertex(T.label,'x','name','tom')");
    //Vertx vertx = new Vertx();
    //VirtuosoService vs = new VirtuosoService();
    
    // Test inserting
    graph = new VirtGraph("citadel", "jdbc:virtuoso://localhost:1111", "dba", "dba");
    //graph.clear();
    Node citadel = NodeFactory.createURI("http://metroinsight.io/citadel/schema");
    Node a = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    Node sch = NodeFactory.createURI("schema");
    graph.add(new Triple(citadel, a, sch));
    System.out.println(String.format("Entire triples in the graph: %s", graph.getCount()));

    // Print everything.
    String qStr = "select ?s ?p ?o where { ?s ?p ?o .}";
    Query sparql = QueryFactory.create(qStr);
    VirtuosoQueryExecution vqd = VirtuosoQueryExecutionFactory.create(sparql, graph);
    ResultSet results = vqd.execSelect();
    
    while (results.hasNext()) {
          QuerySolution result = results.nextSolution();
          String s = result.get("s").toString();
          String p = result.get("p").toString();
          String o = result.get("o").toString();
          System.out.println(s +"\t" + p + "\t" + o);
        }
  }

}