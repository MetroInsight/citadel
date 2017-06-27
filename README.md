Skeleton out the Vertx framework.

Currently design is a bit messy as a learning step. But you could grasp how it works.

A thing to observer though it's not the best is the relationship between metorinsight.citadel.RestApi and metorinsight.citadel.metadata.MetadataVerticle.
They are separate services and RestApi module requests queries and posting to MetdataVerticle via EventBus. Though they are separate, it looks like calling afunction from MetadataVerticle. Thus, the two services and their developments are totally separated and asynchronous.

You can feel the callback coding style from the code.

I still need to clean up and add comments. But take a look if you have time and provide comments.


### Naviation
1. MainVerticle contains all the verticles to be deployed (currently only one.)
2. model package contains data model we will use. It will be shared across different packages.
3. timeseries package is not used for now.
4. Best practice is to use different verticles for different services and then do port-forwarding to 80. Currently all APIs should be collected in RestApi Verticle.

### (For Dev) Add your module?
1. If you would like to add a module, add a package with the same structure of metadata and metadata.impl.
2. RestApi.java routes all functions in the modules in 1. to appropriate URLs. (This is not the best practice but it seems to be a way only with a verticle. Suggestion?)


