# Citadel With Vert.X

This is a Maven project. You can use any mvn commands.

### System Requirements
1. Mongodb

   (1) [Install](https://docs.mongodb.com/manual/installation/)
 
2. Hbase

   (1) [Install](https://hbase.apache.org/book.html#quickstart)
 
   (2) Follow instruction 2.1 and 2.2. The standalone version is sufficient for now.
 
   (3) Copy ``hbase-site.xml`` into ``${projectDir}/conf/``
 
3. Geomesa

   (1) Necessary packages will be installed via Maven.
 
4. Virtuoso

   (1) [Install](https://github.com/openlink/virtuoso-opensource)
 
5. Redis 

   (1) [Install](https://redis.io/topics/quickstart)



### Set up dev env.
```{r, engine='bash', count_lines}
mvn eclipse:eclipse
```

### Test Comma
```{r, engine='bash', count_lines}
mvn test
```

### Packaging
```{r, engine='bash', count_lines}
mvn clean package
```

### Naviation
1. MainVerticle contains all the verticles to be deployed (currently only one.)
2. model package contains data model we will use. It will be shared across different packages.
3. timeseries package is not used for now.
4. Best practice is to use different verticles for different services and then do port-forwarding to 80. Currently all APIs should be collected in RestApi Verticle.

### (For Dev) Add your module?
1. If you would like to add a module, add a package with the same structure of metadata and metadata.impl.
2. RestApi.java routes all functions in the modules in 1. to appropriate URLs. (This is not the best practice but it seems to be a way only with a verticle. Suggestion?)

### Notes
1. Eclipse may say it includes errors in models package. It complains about the code that should be generated during compile time.
2. Reference implementation: https://github.com/cescoffier/vertx-microservices-workshop
3. Reference documentation: http://escoffier.me/vertx-hol/ and http://vertx.io/.
