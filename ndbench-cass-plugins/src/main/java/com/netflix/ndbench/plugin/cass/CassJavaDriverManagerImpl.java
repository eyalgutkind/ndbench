/**
 * Copyright (c) 2017 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

/**
 * @author vchella
 */
public class CassJavaDriverManagerImpl implements CassJavaDriverManager {

    Cluster cluster;
    Session session;

    @Override
    public Cluster registerCluster(String clName, String contactPoint) {
        
        PoolingOptions poolingOpts = new PoolingOptions()
                                     .setConnectionsPerHost(HostDistance.LOCAL, 36, 36)
                                     .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
                                     .setNewConnectionThreshold(HostDistance.LOCAL, 1000);
        
        //http://docs.datastax.com/en/developer/java-driver/3.1/manual/load_balancing/
        cluster = Cluster.builder()
                .addContactPoint(contactPoint)
                .withClusterName(clName)
                .withPoolingOptions(poolingOpts)
                .withLoadBalancingPolicy( new TokenAwarePolicy( new RoundRobinPolicy() ) )
                .build();
        return cluster;
    }

    @Override
    public Session getSession(Cluster cluster) {
         session = cluster.connect();
         return session;
    }

    @Override
    public void shutDown() {
        cluster.close();
    }
}
