package de.idealo.mongodb.perf.operations;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import de.idealo.mongodb.perf.CassandraDbAccessor;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by halongdo on 05.04.2022
 */
public class InsertCassandraOperation extends AbstractCassandraOperation {

    private final ThreadLocalRandom random;
    private int randomFieldLength = 0;


    public InsertCassandraOperation(CassandraDbAccessor cassandraDbAccessor, String keyspace, String table, String field){
        super(cassandraDbAccessor, keyspace, table, field);
        random = ThreadLocalRandom.current();
    }

    @Override
    long executeQuery(int threadId, long threadRunCount, long globalRunCount, long selectorId, long randomId) {
        ArrayList<String> columns = Lists.newArrayList();
        ArrayList<Object> values = Lists.newArrayList();
        columns.add("unique_id");
        values.add(UUID.fromString("be6c9ee4-b46c-11ec-bfc2-6725fb26bf61"));
        columns.add("id");
        values.add(maxId + globalRunCount);
        columns.add(THREAD_ID);
        values.add(threadId);
        columns.add(THREAD_RUN_COUNT);
        values.add(threadRunCount);
        columns.add(RANDOM_LONG);
        values.add(randomId);
        if (randomFieldLength > 0){
            columns.add(RANDOM_TEXT);
            values.add(generateRandomString(randomFieldLength));
        }
        columns.add(VERSION);
        values.add(1);
        session.execute(QueryBuilder.insertInto(table).values(columns, values));
        return 1L;
    }

    @Override
    public OperationModes getOperationMode(){
        return OperationModes.INSERT;
    };

    public void setRandomFieldLength(int randomFieldLength){
        this.randomFieldLength = randomFieldLength;
    }

    private String generateRandomString(int length){
        return random.ints(48,123)
                .filter(i -> (i < 58) || (i > 64 && i < 91) || (i > 96))
                .limit(length)
                .collect(StringBuilder::new, (sb, i) -> sb.append((char) i), StringBuilder::append)
                .toString();
    }

}
