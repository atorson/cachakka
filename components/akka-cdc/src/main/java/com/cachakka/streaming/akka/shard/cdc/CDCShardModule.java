package com.cachakka.streaming.akka.shard.cdc;



import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.cachakka.streaming.akka.*;
import scala.concurrent.ExecutionContext;

public class CDCShardModule extends AbstractModule {

    @Singleton
    @Provides CDCCassandraContext getCassContext() {
        return new CDCCassandraContext(AkkaCluster.configurationProvider(), AkkaCluster.actorSystem().dispatchers().lookup("dispatchers.db"));
    }

    @Override
    protected void configure() {}
}