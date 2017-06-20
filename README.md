Skeleton out the Vertx framework.

Currently design is a bit messy as a learning step. But you could grasp how it works.

A thing to observer though it's not the best is the relationship between metorinsight.citadel.RestApi and metorinsight.citadel.metadata.MetadataVerticle.
They are separate services and RestApi module requests queries and posting to MetdataVerticle via EventBus. Though they are separate, it looks like calling afunction from MetadataVerticle. Thus, the two services and their developments are totally separated and asynchronous.

You can feel the callback coding style from the code.

I still need to clean up and add comments. But take a look if you have time and provide comments.

