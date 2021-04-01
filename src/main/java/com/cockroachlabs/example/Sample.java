package com.cockroachlabs.example;

import com.cockroachlabs.example.jooq.db.tables.records.AccountsRecord;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.time.StopWatch;
import org.jooq.*;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.cockroachlabs.example.jooq.db.Tables.ACCOUNTS;

public class Sample {

    private static final Logger log = LoggerFactory.getLogger(Sample.class);

    private static final Random RAND = new Random();
    private static final String RETRY_SQL_STATE = "40001";
    private static final int MAX_ATTEMPT_COUNT = 6;
    private static final int ACCOUNTS_SIZE = 1024;

    private static Function<DSLContext, Long> addAccounts(List<AccountsRecord> accountsRecords) {
        return ctx -> {
            int[] results = ctx.batchInsert(accountsRecords).execute();
            return (long) results.length;
        };
    }

    private static Function<DSLContext, Long> transferFunds(UUID fromId, UUID toId, long amount) {
        return ctx -> {

            AccountsRecord fromAccount = ctx.fetchSingle(ACCOUNTS, ACCOUNTS.ID.eq(fromId));
            AccountsRecord toAccount = ctx.fetchSingle(ACCOUNTS, ACCOUNTS.ID.eq(toId));

            if (!(amount > fromAccount.getBalance())) {
                fromAccount.setBalance(fromAccount.getBalance() - amount);
                toAccount.setBalance(toAccount.getBalance() + amount);

                int[] results = ctx.batchUpdate(fromAccount, toAccount).execute();
                return (long)results.length;
            }

            return -1L;
        };
    }

    private static Function<DSLContext, Long> getAccountBalance(UUID id) {
        return ctx -> ctx.fetchSingle(ACCOUNTS, ACCOUNTS.ID.eq(id)).getBalance();
    }

    private static long runQuery(DSLContext ctx, Function<DSLContext, Long> fn) {
        return fn.apply(ctx);
    }

    private static long runTransaction(DSLContext ctx, Function<DSLContext, Long> fn) {
        return ctx.transactionResult(configuration -> fn.apply(DSL.using(configuration)));
    }

    public static void main(String[] args) throws Exception {

        SQLDialect dialect = SQLDialect.POSTGRES;

        try {
            dialect = SQLDialect.valueOf("COCKROACHDB");
        } catch (IllegalArgumentException ex) {
            log.warn("CockroachDB Dialect is not available in the opensource version of Jooq.  You must download and install a commercial version.  Falling back to Postgres.");
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:26257/bank?ApplicationName=JooqBank&reWriteBatchedInserts=true&sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory&sslfactoryarg=classpath:certs/ca.crt");
        config.setUsername("maxroach");
        config.setPassword("password");
        config.setMaximumPoolSize(12);

        HikariDataSource dataSource = new HikariDataSource(config);

        List<AccountsRecord> accountsRecords = new ArrayList<>();

        StopWatch stopWatch = new StopWatch();

        for (int i = 0; i < ACCOUNTS_SIZE; i++) {
            accountsRecords.add(new AccountsRecord(UUID.randomUUID(), 1000L));
        }

        DSLContext ctx = DSL.using(dataSource, dialect, new Settings()
                .withExecuteLogging(true)
                .withRenderQuotedNames(RenderQuotedNames.NEVER));

        try (InputStream in = Sample.class.getResourceAsStream("/db.sql")) {
            ctx.parser().parse(Source.of(in).readString()).executeBatch();
        }


        stopWatch.start();
        runTransaction(ctx, addAccounts(accountsRecords));
        stopWatch.stop();

        log.info("inserted {} accounts in {} ms", accountsRecords.size(), stopWatch.getTime(TimeUnit.MILLISECONDS));

        long transferAmount = 100;

        stopWatch.reset();
        stopWatch.start();
        for (int i = 0; i < accountsRecords.size(); i++) {

            UUID fromRandom = accountsRecords.get(RAND.nextInt(accountsRecords.size())).getId();
            UUID toRandom = accountsRecords.get(RAND.nextInt(accountsRecords.size())).getId();

            if (!fromRandom.equals(toRandom)) {
                long transferResult = runTransaction(ctx, transferFunds(fromRandom, toRandom, transferAmount));
                if (transferResult != -1) {
                    log.trace("APP: transferFunds({}, {}, {}) --> {} ", fromRandom.toString(), toRandom.toString(), transferAmount, transferResult);

                    long fromBalanceAfter = runQuery(ctx, getAccountBalance(fromRandom));
                    log.trace("APP: getAccountBalance({}) --> {}", fromRandom.toString(), fromBalanceAfter);

                    long toBalanceAfter = runQuery(ctx, getAccountBalance(toRandom));
                    log.trace("APP: getAccountBalance({}) --> {}", toRandom.toString(), toBalanceAfter);
                } else {
                    log.warn("skipping... not enough money to transfer");
                }
            } else {
                log.warn("skipping... attempting to transfer to self");
            }
        }
        stopWatch.stop();

        long totalTransactionTime = stopWatch.getTime(TimeUnit.MILLISECONDS);
        log.info("completed {} business transactions in {} ms or {} s; avg per transaction = {} ms", accountsRecords.size(), totalTransactionTime, stopWatch.getTime(TimeUnit.SECONDS), (totalTransactionTime / ACCOUNTS_SIZE));


    }
}
