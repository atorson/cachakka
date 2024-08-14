package com.cachakka.streaming.connectors;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.google.common.base.Strings;
import com.google.common.io.BaseEncoding;
import com.typesafe.config.Config;
import com.cachakka.streaming.configuration.ConfigurationProvider;


import io.getquill.context.cassandra.cluster.ClusterBuilder;
import io.vavr.control.Try;
import lombok.Builder;

import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/**
 * CASS conntector implementation
 */
@Builder
public class CassandraClusterConnectorImpl implements CassandraClusterConnector{

        @Builder.Default private String clusterName = "DEFAULT_CLUSTER_NAME";
        private String clusterIpAddressList;
        private int port;
        private String dataCenter;
        private String username;
        private String password;
        private String keyspace;
        @Builder.Default private long preparedStatementsCacheSize = 1000000L;
        @Builder.Default private ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
        @Builder.Default private String compression = ProtocolOptions.Compression.LZ4.name();
        @Builder.Default private int initConnectionsPerHost = 2;
        @Builder.Default private int maxConnectionsPerHost = 10;
        @Builder.Default private int initRemoteConnectionsPerHost = 0;
        @Builder.Default private int maxRemoteConnectionsPerHost = 0;
        @Builder.Default private int idleConnectionTimeout = 120000;
        @Builder.Default private int usedHostsPerRemoteDc = 1;
        //default is 1024 connections in datastax driver.
        @Builder.Default private int simultaneousRequestsPerConnection = 1024;
        @Builder.Default private int newConnectionThreshold = 800;
        // Takes a hit on caching if set to true
        @Builder.Default private Boolean shuffleReplicasAmongReplicationNodes = true;
        @Builder.Default private Boolean keepTCPConnectionAlive = true;
        @Builder.Default private Integer connectionTimeOut = SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS;
        // Socket timeout options, should be smaller on client side vs on server
        @Builder.Default private Integer readTimeout = SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS;
        @Builder.Default private Long reconnectPeriod = 5L;
        @Builder.Default private Boolean shuffleReplicas = true;
        private Boolean downgradableConsistency;
        private RetryPolicy retryPolicy;
        @Builder.Default private final AtomicReference<Cluster> clusterRef = new AtomicReference<>();

        public static class CassandraClusterConnectorBuilder {
            private Boolean downgradableConsistency = false;
            private RetryPolicy retryPolicy = downgradableConsistency ? DowngradingConsistencyRetryPolicy.INSTANCE : DefaultRetryPolicy.INSTANCE;
        }

    private Cluster.Builder builder;

    public static synchronized CassandraClusterConnectorImpl getInstance(ConfigurationProvider provider) {
        CassandraClusterConnectorImpl result;
        try {
            final Config config = provider.getConfig().getConfig("quill");
            Cluster.Builder builder = ClusterBuilder.apply(config.getConfig("builder"));
            result = CassandraClusterConnectorImpl.builder()
                    .dataCenter(config.getString("datacenter"))
                    .keyspace(config.getString("keyspace"))
                    .consistencyLevel(ConsistencyLevel.valueOf(config.getString("consistencyLevel")))
                    .shuffleReplicas(Try.of(() -> config.getBoolean("shuffleReplicas")).getOrElse(true))
                    .preparedStatementsCacheSize(config.getLong("preparedStatementCacheSize"))
                    .build();
            result.setBuilder(result.finalizeBuilder(builder));
        } catch (Exception e) {
            final Config config = provider.getConfig().getConfig("sp.integration.cass");
            String clusterName = generateRandomAlphanumeric();
            result = CassandraClusterConnectorImpl.builder()
                    .username(config.getString("username"))
                    .password(config.getString("password"))
                    .keyspace(config.getString("keyspace"))
                    .clusterIpAddressList(config.getString("hostname"))
                    .port(config.getInt("port"))
                    .dataCenter(config.getString("datacenter"))
                    .clusterName(clusterName)
                    .build();

            result.setBuilder(result.finalizeBuilder(Cluster.builder()
                    .addContactPoints(result.clusterIpAddressList.split(","))
                    .withClusterName(result.clusterName)
                    .withPort(result.port)
                    .withCredentials(result.username, result.password)));
        }
        final CassandraClusterConnectorImpl toReturn = result;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> toReturn.close()));
        return toReturn;
    }



    @Override
    public void close() {
       getCluster().close();
    }

    private static boolean isOpen(Cluster cluster){
        return (cluster != null && !cluster.isClosed());
    }

    public Cluster getCluster(){
        return clusterRef.updateAndGet(c -> isOpen(c)? c: builder.build());
    }


    @Override
    public String getPrimaryKeyspaceName() {
        return keyspace;
    }

    @Override
    public Long getPreparedCacheSize() {
        return preparedStatementsCacheSize;
    }

    protected synchronized void setBuilder(Cluster.Builder builder){
        this.builder = builder;
    }


    protected synchronized Cluster.Builder finalizeBuilder(Cluster.Builder builder) {
        Cluster.Builder result = builder;
        DCAwareRoundRobinPolicy.Builder dcAwareRoundRobinPolicyBuilder = DCAwareRoundRobinPolicy.builder();
        result = result.withPoolingOptions(getPoolingOptions())
                .withCompression(ProtocolOptions.Compression.valueOf(compression))
                .withRetryPolicy(retryPolicy)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(reconnectPeriod))
                .withSocketOptions(getSocketOptions());
        if (!Strings.isNullOrEmpty(dataCenter)) {
            dcAwareRoundRobinPolicyBuilder = dcAwareRoundRobinPolicyBuilder
                    .withLocalDc(dataCenter)
                    .withUsedHostsPerRemoteDc(usedHostsPerRemoteDc);
            result = result.withQueryOptions(new QueryOptions().setConsistencyLevel(consistencyLevel));
        } else {
            result = result.withProtocolVersion(ProtocolVersion.V3);
        }
        TokenAwarePolicy lbp = new TokenAwarePolicy(dcAwareRoundRobinPolicyBuilder.build(), shuffleReplicas);
        result = result.withLoadBalancingPolicy(lbp);
        return result;
    }


    private SocketOptions getSocketOptions() {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(connectionTimeOut);
        socketOptions.setReadTimeoutMillis(readTimeout);
        socketOptions.setKeepAlive(keepTCPConnectionAlive);
        return socketOptions;
    }

    private PoolingOptions getPoolingOptions() {

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, initConnectionsPerHost);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnectionsPerHost);
        poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, initRemoteConnectionsPerHost);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, maxRemoteConnectionsPerHost);
        poolingOptions.setNewConnectionThreshold(HostDistance.LOCAL, newConnectionThreshold);
        poolingOptions.setIdleTimeoutSeconds(idleConnectionTimeout);
        return poolingOptions;
    }

    private static String generateRandomAlphanumeric(){
        Random random = new Random();
        final byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        return BaseEncoding.base64Url().omitPadding().encode(buffer); // or base32()
    }

}