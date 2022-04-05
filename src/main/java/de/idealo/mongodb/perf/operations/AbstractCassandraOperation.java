package de.idealo.mongodb.perf.operations;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import de.idealo.mongodb.perf.CassandraDbAccessor;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by halongdo on 05.04.2022
 */
public abstract class AbstractCassandraOperation implements IOperation {


    private final Random random = ThreadLocalRandom.current();
    private final AtomicLong affectedDocs = new AtomicLong();
    final Session session;
    final CassandraDbAccessor cassandraDbAccessor;
    final long minId;
    final long maxId;
    final String queriedField;
    final String table;

    public Session getSession() {
        return session;
    }

    public AbstractCassandraOperation(CassandraDbAccessor cassandraDbAccessor, String keyspace, String table, String queriedField){
        this.cassandraDbAccessor = cassandraDbAccessor;
        this.session = cassandraDbAccessor.getCluster().connect(keyspace);
        this.table = table;
        this.queriedField = queriedField;
        SchemaBuilder.createIndex("indexedField_" + queriedField).ifNotExists().onTable(keyspace, table).andColumn(queriedField);
        minId = getMinMax(cassandraDbAccessor, queriedField, true);
        maxId = getMinMax(cassandraDbAccessor, queriedField, false);
    }

    /**
     *
     * @param threadId
     * @param threadRunCount
     * @param globalRunCount
     * @param randomId
     * @return number of affected documents
     */

    abstract long executeQuery(int threadId, long threadRunCount, long globalRunCount, long selectorId, long randomId);


    private long getMinMax(CassandraDbAccessor cassandraDbAccessor, String field, boolean min) {
        return cassandraDbAccessor.getMinMax(session, table, field, min);
    }

    @Override
    public void operation(int threadId, long threadRunCount, long globalRunCount) {

        final ThreadLocalRandom rnd = ThreadLocalRandom.current();
        final long selectorId = rnd.nextLong(minId, maxId+1l);//2nd paramter is exlusive, thus add 1
        final long randomId = rnd.nextLong();
        LOG.debug("{}: {} {}: {} {}: {} selectorId:{} {}: {}",
                THREAD_ID, threadId,
                THREAD_RUN_COUNT, threadRunCount,
                GLOBAL_RUN_COUNT, globalRunCount,
                selectorId,
                RANDOM_LONG, randomId);
        try {
            final long lAffectedDocs = executeQuery(threadId, threadRunCount, globalRunCount, selectorId, randomId);
            affectedDocs.addAndGet(lAffectedDocs);
        } catch (Exception e) {
            LOG.error("error while executing query on field '{}' with value '{}'", queriedField, selectorId, e);
        }
    }

    @Override
    public long getAffectedDocuments() {
        return affectedDocs.get();
    }


}
