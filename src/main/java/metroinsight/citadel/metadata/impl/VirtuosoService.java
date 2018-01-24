package metroinsight.citadel.metadata.impl;

import static org.apache.jena.datatypes.xsd.XSDDatatype.XSDstring;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.ServiceDiscovery;
import metroinsight.citadel.common.ErrorMessages;
import metroinsight.citadel.metadata.MetadataService;
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
  final String BIF = "http://www.openlinksw.com/schema/sparql/extensions#";
  
  Map<String, Node> propertyMap;
  Map<String, String> valPropMap;
  // TODO: Maybe add a map between uesr-prop to rdf property. e.g., "pointType" -> rdf:type.
  
  // Common Variables
  Node a;
  Node hasUnit;
  Node hasName;
  
  //Units
  List<String> units;
  List<String> types;
  String graphname;
  
  public VirtuosoService(Vertx vertx, String virtHostname, Integer virtPort, String graphname, String username, String password, ServiceDiscovery discovery) {
    this.graphname = graphname;
    this.vertx = vertx;
    this.discovery = discovery;
    if (graph==null) {
      graph = new VirtGraph(this.graphname, String.format("jdbc:virtuoso://%s:%d", virtHostname, virtPort), username, password);
      System.out.println(String.format("Generated graph name: %s", graph.getGraphName()));
    }
    initSchema();
  }
  
  private void initSchema() {
    // Init common variables
    units = new ArrayList<String>();
    types = new ArrayList<String>();

    a = NodeFactory.createURI(RDF + "type");
    hasUnit = NodeFactory.createURI(CITADEL + "unit");
    hasName = NodeFactory.createURI(CITADEL + "name");
    
    // Init Namespace Map
    // This may be automated once we get a schema file (in Turtle).
    propertyMap = new HashMap<String, Node>();
    propertyMap.put("pointType", NodeFactory.createURI(RDF + "type"));
    propertyMap.put("subClassOf", NodeFactory.createURI(RDFS + "subClassOf"));
    propertyMap.put("unit", NodeFactory.createURI(CITADEL + "unit"));
    propertyMap.put("owner", NodeFactory.createURI(CITADEL + "owner"));
    propertyMap.put("name", NodeFactory.createURI(CITADEL + "name"));
    valPropMap = new HashMap<String, String>();
    valPropMap.put("name", "string");
    valPropMap.put("unit", "citadel");
    valPropMap.put("pointType", "citadel");
    
    // Init units
    // types
    List<String> citadelTypes = new ArrayList<String>(units);
    citadelTypes.addAll(types);
    for (int i=0; i < citadelTypes.size(); i++) {
      String v = citadelTypes.get(i);
      propertyMap.put(v, NodeFactory.createURI(CITADEL + v));
    }
  }
  
  private Node withPrefixValue (String prop, String id) {
    if (valPropMap.containsKey(prop)) {
      String valType = valPropMap.get(prop);
      if (valType.equals("string")) {
        return NodeFactory.createLiteralByValue(id, XSDstring);
      } else if (valType.equals("citadel")) {
        return NodeFactory.createURI(CITADEL + id);
      } else {
        System.out.println("TODO: Unknown value type");
        return NodeFactory.createURI(EX + id);
      }
    } else {
      return NodeFactory.createLiteral(id);
    }
  }
  
  private Node withPrefixProp(String id) {
    return propertyMap.getOrDefault(id, NodeFactory.createURI(EX + id));
  }
  
  private ParameterizedSparqlString getDefaultPss() {
      ParameterizedSparqlString pss = new ParameterizedSparqlString();
      pss.setBaseUri(EX);
      pss.setNsPrefix("ex", EX);
      pss.setNsPrefix("rdf", RDF);
      pss.setNsPrefix("rdfs", RDFS);
      pss.setNsPrefix("citadel", CITADEL);
      pss.setNsPrefix("bif", "bif:");
      return pss;
  }
  
  private ResultSet findByStringValue(String tag, String value) {
    ParameterizedSparqlString pss = getDefaultPss();
    //String qStr = String.format("select ?s WHERE {\n", graphname);
    String qStr = String.format("select ?s FROM <%s> WHERE {\n", graphname);
    if (value.startsWith("\"") && value.endsWith("\"")) {
      value = value.substring(1, value.length() - 1);
    }
    qStr += String.format("?s citadel:%s ?o . ?o bif:contains \"'%s'\". }\n", tag, value);
    pss.setCommandText(qStr);
    return sparqlQuery(pss.toString());
  }
  
  private ResultSet findByTagValues(List<List<String>> tagValuePairs) {
    ParameterizedSparqlString pss = getDefaultPss();
    String qStr = "select ?s where {\n";
    for (int i=0; i < tagValuePairs.size(); i++) {
      List<String> tagValue = tagValuePairs.get(i);
      String tag = tagValue.get(0);
      String value = tagValue.get(1);
      qStr += String.format("{?s citadel:%s ex:%s .}\n", tag, value);
      qStr += "UNION\n";
      qStr += String.format("{?s citadel:%s citadel:%s .}\n", tag, value);
    }
    qStr += "}";
    pss.setCommandText(qStr);
    return sparqlQuery(pss.toString());
  }
  
  @Override
  public void queryPoint(JsonObject query, Handler<AsyncResult<JsonArray>> resultHandler) {
    try {
      // Construct a SPARQL query.
      ParameterizedSparqlString pss = getDefaultPss();
      String qStr = "SELECT ?s WHERE {\n";
      for (int i=0; i< query.fieldNames().size(); i++) {
        qStr += "?s ? ? . \n";
      }
      qStr += "?s ?p ?o .\n}";
      pss.setCommandText(qStr);
      Set<String> keys = query.fieldNames();
      Iterator<String> keyIter = keys.iterator();
      int i = 0;
      String key;
      String value;
      while (keyIter.hasNext()) {
        key = keyIter.next();
        value = query.getString(key);
        Node keyNode = withPrefixProp(key);
        Node valueNode = withPrefixValue(key, value);
        pss.setIri(i * 2, keyNode.toString());
        if (valueNode.isLiteral()) {
          pss.setLiteral(i * 2 + 1, "'test_sensor1'", XSDstring);
          //pss.setLiteral(i * 2 + 1, valueNode.toString(), XSDstring);
        } else {
          pss.setIri(i * 2 + 1, valueNode.toString());
        }
        /*
        if (key.equals("pointType")) { // TODO: Use map to organize below.
          key = RDF + "type";
          value = CITADEL + value;
        } else if (key.equals("name")) {
        } else {
          key = CITADEL + key;
          value = CITADEL + value;
        }*/
        i += 1;
      }
      // Run SPARQL query.
      String qqStr = pss.toString();
      ResultSet results = sparqlQuery(pss.toString());
      // Get UUIDs from the result.
      HashSet<String> uuidSet = new HashSet<String>();
      while (results.hasNext()) {
        String ent = results.nextSolution().get("s").toString();
        if (ent.contains(EX)) {
          String uuid = ent.split("#")[1];
          uuidSet.add(uuid);
        }
      }
      JsonArray uuids = new JsonArray(new ArrayList<String>(uuidSet));
      resultHandler.handle(Future.succeededFuture(uuids));
    }catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void getPoint(String uuid, Handler<AsyncResult<JsonObject>> resultHandler) {
    try {
      String qStr = "SELECT ?p ?o WHERE {?s ?p ?o}";
      ParameterizedSparqlString pss = getDefaultPss();
      pss.setCommandText(qStr);
      pss.setIri("s", EX + uuid);
      ResultSet results = sparqlQuery(pss.toString());
      if (!results.hasNext()) {
        resultHandler.handle(Future.failedFuture("Not existing UUID"));
      } else {
        JsonObject metadata = new JsonObject();
        //TODO: Align this JSON to metadata.
        while (results.hasNext()) {
          QuerySolution result = results.nextSolution();
          String p = result.get("p").toString().split("#")[1];
          String o = result.get("o").toString().split("#")[1];
          if (p.equals("type")) {
            p = "pointType";
          }
          metadata.put(p, o);
        }
        metadata.put("uuid", uuid);
        resultHandler.handle(Future.succeededFuture(metadata));
      }
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void createPoint(JsonObject jsonMetadata, Handler<AsyncResult<String>> resultHandler) {
    try {
      long totalStartTime = System.nanoTime();
      if (jsonMetadata.getString("name").contains(" ")) {
        throw new Exception("Empty space is not allowed in name.");
      }
      else if (jsonMetadata.getString("unit").contains(" ")) {
        throw new Exception("Empty space is not allowed in unit.");
      }
      // Check if the name already exists
      String nameStr = jsonMetadata.getString("name");
      /*
      ParameterizedSparqlString pss = getDefaultPss();
      pss.setCommandText("select ?s where {?s citadel:name ?name .}");
      pss.setParam("name", name);
      ResultSet res = sparqlQuery(pss.toString());
      */
      long startTime = System.nanoTime();
      Node name = withPrefixValue("name", nameStr); // TODO: Change name to Literal later
      //ResultSet res = findByStringValue("name", nameStr);
      ResultSet res = findByStringValue("name", name.toString());
      long endTime = System.nanoTime();
      System.out.println(String.format("Name check time: %f", ((float)endTime - (float)startTime)/1000000));
      //Node name = withPrefixValue("name", nameStr); // TODO: Change name to Literal later
      if (res.hasNext()) {
        resultHandler.handle(Future.failedFuture(ErrorMessages.EXISTING_POINT_NAME));
      } else {
        // Create the point
        startTime = System.nanoTime();
        String uuid = jsonMetadata.getString("uuid");//UUID.randomUUID().toString();
        Node point = NodeFactory.createURI(EX + uuid);
        Node pointType = withPrefixValue("pointType", jsonMetadata.getString("pointType"));
        graph.add(new Triple(point, a, pointType));
        Node unit = withPrefixValue("unit", jsonMetadata.getString("unit"));
        graph.add(new Triple(point, hasUnit, unit));
        graph.add(new Triple(point, hasName, name));
        Iterator<String> tagIter = jsonMetadata.fieldNames().iterator();
        while (tagIter.hasNext()) {
          String tag = tagIter.next();
          String value = jsonMetadata.getString(tag);
          if (!tag.equals("unit") && !tag.equals("name") && !tag.equals("uuid") && !tag.equals("pointType")) {
            graph.add(new Triple(point, withPrefixProp(tag), withPrefixValue(tag, value)));
          }
        }
        endTime = System.nanoTime();
        System.out.println(String.format("Virtuoso creation time: %f", ((float)endTime - (float)startTime)/1000000));
        long totalEndTime = System.nanoTime();
        System.out.println(String.format("Virtuoso createPoint time: %f", ((float)totalEndTime - (float)totalStartTime)/1000000));
        resultHandler.handle(Future.succeededFuture(uuid));
      }
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void upsertMetadata(String uuid, JsonObject newMetadata, Handler<AsyncResult<Void>> rh) {
    try {
      Node point = withPrefixProp(uuid);
      Iterator<String> keys = newMetadata.fieldNames().iterator();
      while (keys.hasNext()) {
        String key = keys.next();
        Node prop = withPrefixProp(key);
        Object value = newMetadata.getValue(key);
        if (value instanceof List) { // TODO: Check this is working. If not working, use try catch.
          Iterator<String> valueIter = ((List<String>) value).iterator();
          while (valueIter.hasNext()) {
            graph.add(new Triple(point, prop, withPrefixProp(valueIter.next())));
          }
        } else if (value instanceof String) {
          graph.add(new Triple(point, prop, withPrefixProp((String) value)));
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
  
  public void testFunc () {
    String nameStr = "test_sensor1";
    String BASE = "http://base.org#";
    String uuid = "xxxxx";
    Node name = NodeFactory.createLiteralByValue(nameStr, XSDstring);
    Node point = NodeFactory.createURI(BASE + uuid);
    Node hasName = NodeFactory.createURI(BASE + "name");
    graph.add(new Triple(point, hasName, name));
    String qStr = 
        "PREFIX base: <http://base.org#>\n" + 
        "SELECT ?s WHERE {\n" + 
        "?s base:name \"test_sensor1\" . \n" + 
        "}";
    Query sparql = QueryFactory.create(qStr);
    VirtuosoQueryExecution vqd = VirtuosoQueryExecutionFactory.create(sparql, graph);
    ResultSet results = vqd.execSelect();
    while (results.hasNext()) {
      QuerySolution result = results.nextSolution();
      System.out.println(result.get("s").toString());
    }
    System.out.println("Done");
  }
  
  public static void main(String[] args) {	
    // Test inserting
    Vertx vertx = Vertx.vertx();
    Buffer configBuffer = vertx.fileSystem().readFileBlocking("./src/main/resources/conf/citadel-conf.json");
    JsonObject configs = new JsonObject(configBuffer);
    /*
    graph = new VirtGraph("citadel", String.format("jdbc:virtuoso://%s:%d", 
                                                   configs.getString("metadata.virt.hostname"), 
                                                   configs.getInteger("metadata.virt.port")), 
                          configs.getString("metadata.virt.username"), configs.getString("metadata.virt.password"));
                          */
    VirtuosoService vs = new VirtuosoService(vertx, 
                                             configs.getString("metadata.virt.hostname"),
                                             configs.getInteger("metadata.virt.port"), 
                                             configs.getString("metadata.virt.graphname"), 
                                             configs.getString("metadata.virt.username"), 
                                             configs.getString("metadata.virt.password"), 
                                             null);
    vs.testFunc();
    String nameStr = "test_sensor1";
    //Node name = vs.withPrefixValue("name", nameStr); // TODO: Change name to Literal later
    Node name = NodeFactory.createLiteralByValue(nameStr, XSDstring);
    String uuid = "xxxxx";
    Node point = NodeFactory.createURI(vs.EX + uuid);
    String BASE = "http://base.org#";
    Node hasName = NodeFactory.createURI(BASE + "name");
    graph.add(new Triple(point, hasName, name));
    
    //graph.clear();
    Node citadel = NodeFactory.createURI("http://metroinsight.io/citadel/schema");
    Node a = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    Node sch = NodeFactory.createURI("schema");
    graph.add(new Triple(citadel, a, sch));
    System.out.println(String.format("Entire triples in the graph: %s", graph.getCount()));

    // Print everything.
    //String qStr = "select ?s ?p ?o where { ?s ?p ?o .}";
    String qStr = 
        //"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
        //"PREFIX ex: <http://example.com#>\n" + 
        //"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
        //"PREFIX citadel: <http://metroinsight.io/citadel#>\n" + 
        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
        "PREFIX bif: <bif:>\n" + 
        "PREFIX base: <http://base.org#>\n" + 
        "SELECT ?s WHERE {\n" + 
        //"SELECT ?s ?p ?o WHERE {\n" + 
        //"?s citadel:name \"test_sensor1\"^^xsd:string . \n" + 
        "?s base:name \"test_sensor1\" . \n" + 
        //"?s citadel:name ?o . ?o bif:contains \"'test_sensor0'\". \n" +
        //"?s ?p ?o .\n" + 
        "}";
    Query sparql = QueryFactory.create(qStr);
    VirtuosoQueryExecution vqd = VirtuosoQueryExecutionFactory.create(sparql, graph);
    ResultSet results = vqd.execSelect();

    while (results.hasNext()) {
      System.out.println("==============================");
      QuerySolution result = results.nextSolution();
      String s = result.get("s").toString();
      String p = result.get("p").toString();
      String o = result.get("o").toString();
      /*
      RDFNode ooo = result.get("o");
      System.out.println(ooo.isLiteral());
      System.out.println(ooo.toString());
      */
      System.out.println(s +"\t" + p + "\t" + o);
      //System.out.println(s + "\t" + o);
      //System.out.println(s);
    }
    System.out.println("Done");
  }

}
