package de.idealo.mongodb.perf;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;

/**
 * Created by halongdo on 05.04.2022.
 */
public class CassandraDbAccessor {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraDbAccessor.class);

    private final InetAddress[] addresses;
    private final Integer port;
    private final int socketTimeOut;
    private final String user;
    private final String pw;
    private final boolean ssl;
    private Cluster cluster;



    private CassandraDbAccessor(){
        this(-1, null, null, false, null, null);
    };

    public CassandraDbAccessor(String user, String pw, boolean ssl, int port, InetAddress ... addresses){
        this(-1, user, pw, ssl, port, addresses);
    }

    public CassandraDbAccessor(int socketTimeOut, String user, String pw, boolean ssl, Integer port, InetAddress ... addresses){
        this.addresses = addresses;
        this.socketTimeOut = socketTimeOut;
        this.user = user;
        this.pw = pw;
        this.ssl = ssl;
        this.port = port;
        init();
    }

    public Cluster getCluster() {
        return cluster;
    }

    public boolean createKeyspaceAndTableIfNotExists(String keyspace, String table) {
        Session masterSession = cluster.connect();
        try {
            masterSession.execute(String.format(
                    "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
            keyspace));
            masterSession.execute(String.format(
                    "CREATE TABLE IF NOT EXISTS %s.%s (\n" +
                            "                    unique_id timeuuid,\n" +
                            "                    id bigint,\n" +
                            "                    threadId int,\n" +
                            "                    threadRunCount bigint,\n" +
                            "                    rnd bigint,\n" +
                            "                    rndTxt text,\n" +
                            "                    v int,\n" +
                            "            PRIMARY KEY (unique_id, id)) WITH CLUSTERING ORDER BY (id ASC)"
            , keyspace, table));
        } catch (Exception e) {
            LOG.error("Error while initializing cassandra at address {}", addresses, e);
            masterSession.close();
            return false;
        } finally {
            masterSession.close();
        }
        return true;
    }

    private void init() {
        LOG.info(">>> init {}", addresses);
        try {
            Cluster.Builder clusterBuilder = Cluster.builder()
                    .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(socketTimeOut == -1 ? 1000 * 10 : socketTimeOut))
                    .withPoolingOptions(new PoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, 5000))
                    .withPort(port);
            if (user != null && !user.isEmpty() && pw != null && !pw.isEmpty()) {
                clusterBuilder.withCredentials(user, pw);
                if (addresses.length == 1) {
                    cluster = clusterBuilder.addContactPoints(addresses[0]).build();
                } else {
                    cluster = clusterBuilder.addContactPoints(addresses).build();
                }
            } else {
                if (addresses.length == 1) {
                    cluster = clusterBuilder.addContactPoints(addresses[0]).build();
                }else {
                    cluster = clusterBuilder.addContactPoints(addresses).build();
                }
            }

        } catch (IllegalArgumentException e) {
            LOG.error("Error while initializing cassandra at address {}", addresses, e);
            closeConnections();
        }

        LOG.info("<<< init");
    }

    public long getMinMax(Session session, String table, String field, boolean min){
        UUID fixedPartitionKey = UUID.fromString("be6c9ee4-b46c-11ec-bfc2-6725fb26bf61");
        try {
            if (min) {
                long minID = session.execute(QueryBuilder.select("id").from(table)
                        .where(QueryBuilder.eq("unique_id", fixedPartitionKey))).one().getLong("id");
                LOG.info("Min ID = '{}' from cassandra", minID);
                return minID;
            }
            long maxID = session.execute(QueryBuilder.select("id").from(table)
                            .where(QueryBuilder.eq("unique_id", fixedPartitionKey)).limit(1)
                            .orderBy(QueryBuilder.desc("id"))).one().getLong("id");
            LOG.info("Max ID = '{}' from cassandra", maxID);
            return maxID;
        } catch (Exception e) {
            LOG.error("error while getting field '{}' from cassandra", field, e);
        }
        return 0;
    }

    public void dropKeyspace(Session session, String keyspace) {
        session.execute(SchemaBuilder.dropKeyspace(keyspace));
    }


    public void closeConnections() {
        LOG.info(">>> closeConnections {}", addresses);

        try {
            if(cluster != null) {
                cluster.close();
                cluster = null;
            }
        } catch (Throwable e) {
            LOG.error("Error while closing mongo ", e);
        }

        LOG.info("<<< closeConnections {}", addresses);
    }


}
