package com.cockroachlabs.example;

import com.cockroachlabs.example.jooq.db.tables.records.AccountsRecord;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.time.StopWatch;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.Source;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.cockroachlabs.example.jooq.db.Tables.ACCOUNTS;

public class Sample {

    private static final Logger log = LoggerFactory.getLogger(Sample.class);

    private static final Random RAND = new Random();
    private static final String RETRY_SQL_STATE = "40001";
    private static final int MAX_ATTEMPT_COUNT = 6;
    private static final int ACCOUNTS_SIZE = 10000;

    private static Function<DSLContext, Long> addAccounts(List<AccountsRecord> accountsRecords) {
        return ctx -> {
            long rv = 0;

            ctx.delete(ACCOUNTS).execute();
            ctx.batchInsert(accountsRecords).execute();

            rv = 1;
            log.trace("APP: addAccounts() --> {}", rv);
            return rv;
        };
    }

    private static Function<DSLContext, Long> transferFunds(UUID fromId, UUID toId, long amount) {
        return ctx -> {
            long rv = 0;

            AccountsRecord fromAccount = ctx.fetchSingle(ACCOUNTS, ACCOUNTS.ID.eq(fromId));
            AccountsRecord toAccount = ctx.fetchSingle(ACCOUNTS, ACCOUNTS.ID.eq(toId));

            if (!(amount > fromAccount.getBalance())) {
                fromAccount.setBalance(fromAccount.getBalance() - amount);
                toAccount.setBalance(toAccount.getBalance() + amount);

                ctx.batchUpdate(fromAccount, toAccount).execute();
                rv = amount;
                log.trace("APP: transferFunds({}, {}, {}) --> {}", fromId.toString(), toId.toString(), amount, rv);
            }

            return rv;
        };
    }

    private static Function<DSLContext, Long> getAccountBalance(UUID id) {
        return ctx -> {
            AccountsRecord account = ctx.fetchSingle(ACCOUNTS, ACCOUNTS.ID.eq(id));
            long balance = account.getBalance();
            log.trace("APP: getAccountBalance({}) --> {}", id.toString(), balance);
            return balance;
        };
    }

    private static long runQuery(DSLContext ctx, Function<DSLContext, Long> fn) {
        return fn.apply(ctx);
    }

    // Run SQL code in a way that automatically handles the
    // transaction retry logic so we don't have to duplicate it in
    // various places.
    private static long runTransaction(DSLContext ctx, Function<DSLContext, Long> fn) {
        AtomicLong rv = new AtomicLong(0L);
        AtomicInteger attemptCount = new AtomicInteger(0);

        while (attemptCount.get() < MAX_ATTEMPT_COUNT) {
            attemptCount.incrementAndGet();

            if (attemptCount.get() > 1) {
                log.debug("APP: Entering retry loop again, iteration {}", attemptCount.get());
            }

            if (ctx.connectionResult(connection -> {
                connection.setAutoCommit(false);
                log.trace("APP: BEGIN;");

                if (attemptCount.get() == MAX_ATTEMPT_COUNT) {
                    String err = String.format("hit max of %s attempts, aborting", MAX_ATTEMPT_COUNT);
                    throw new RuntimeException(err);
                }

                try {
                    rv.set(fn.apply(ctx));
                    if (rv.get() != -1) {
                        connection.commit();
                        log.trace("APP: COMMIT;");
                        return true;
                    }
                } catch (DataAccessException | SQLException e) {
                    String sqlState = e instanceof SQLException ? ((SQLException) e).getSQLState() : ((DataAccessException) e).sqlState();

                    if (RETRY_SQL_STATE.equals(sqlState)) {
                        // Since this is a transaction retry error, we
                        // roll back the transaction and sleep a little
                        // before trying again.  Each time through the
                        // loop we sleep for a little longer than the last
                        // time (A.K.A. exponential backoff).
                        log.warn("APP: retryable exception occurred:    sql state = [{}]    message = [{}]    retry counter = {}", sqlState, e.getMessage(), attemptCount.get());
                        log.warn("APP: ROLLBACK;");
                        connection.rollback();
                        int sleepMillis = (int) (Math.pow(2, attemptCount.get()) * 100) + RAND.nextInt(100);
                        log.warn("APP: Hit 40001 transaction retry error, sleeping {} milliseconds", sleepMillis);
                        try {
                            Thread.sleep(sleepMillis);
                        } catch (InterruptedException ignored) {
                            // no-op
                        }
                        rv.set(-1L);
                    } else {
                        throw e;
                    }
                }

                return false;
            })) {
                break;
            }
        }

        return rv.get();
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


        log.info("inserted {} accounts in  {} ms", accountsRecords.size(), stopWatch.getTime(TimeUnit.MILLISECONDS));

        long transferAmount = 100;

        stopWatch.reset();
        stopWatch.start();
        for (int i = 0; i < accountsRecords.size(); i++) {

            UUID fromRandom = accountsRecords.get(RAND.nextInt(accountsRecords.size())).getId();
            UUID toRandom = accountsRecords.get(RAND.nextInt(accountsRecords.size())).getId();
            long transferResult = runTransaction(ctx, transferFunds(fromRandom, toRandom, transferAmount));
            if (transferResult != -1) {
                // Success!
                log.trace("APP: transferFunds({}, {}, {}) --> {} ", fromRandom.toString(), toRandom.toString(), transferAmount, transferResult);

                long fromBalanceAfter = runQuery(ctx, getAccountBalance(fromRandom));
                long toBalanceAfter = runQuery(ctx, getAccountBalance(toRandom));
                if (fromBalanceAfter != -1 && toBalanceAfter != -1) {
                    // Success!
                    log.trace("APP: getAccountBalance({}) --> {}", fromRandom.toString(), fromBalanceAfter);
                    log.trace("APP: getAccountBalance({}) --> {}", toRandom.toString(), toBalanceAfter);
                }
            }
        }
        stopWatch.stop();

        long totalTransactionTime = stopWatch.getTime(TimeUnit.MILLISECONDS);
        log.info("completed {} business transactions in {} ms or {} s; avg per transaction = {} ms", accountsRecords.size(), totalTransactionTime, stopWatch.getTime(TimeUnit.SECONDS), (totalTransactionTime / ACCOUNTS_SIZE));


    }
}
