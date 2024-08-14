package com.cachakka.streaming.connectors;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


public interface CassandraClusterConnector extends AutoCloseable{

    /**
     *  Opens a new Cassandra session
     * @return
     */
    default Session connect(){
        return getCluster().connect(getPrimaryKeyspaceName());
    }

    Cluster getCluster();

    String getPrimaryKeyspaceName();

    Long getPreparedCacheSize();

}
