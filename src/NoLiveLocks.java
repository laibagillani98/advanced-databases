import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class NoLiveLocks {
    public static int num_tasks = 30;
    public static int num_queries_per_task = 3;



    Connection con;
    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;

    AtomicInteger tasks_finished = new AtomicInteger(0);
    AtomicInteger deadlocks = new AtomicInteger(0);
    AtomicInteger other_failures = new AtomicInteger(0);
    AtomicInteger total_retries = new AtomicInteger(0);
    AtomicInteger max_retries = new AtomicInteger(0);
    AtomicInteger exclusive_runs = new AtomicInteger(0);
    static final int THRESHOLD = Integer.getInteger("threshold", 3);

    public NoLiveLocks() {
        // Class fields already initialised above
    }

    void executeSQL(String sql, Connection con) {
        try (Statement stmt = con.createStatement()) {
            stmt.execute(sql);
            System.out.println("Executing " + sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void cerateDB() {
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;", con);
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);", con);

            con.setAutoCommit(false);
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO blocking_data VALUES (?, ?, ?)",
                    ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_UPDATABLE
            );

            for (int i = 0; i < num_tuple; i++) {
                ps.setInt(1, i);
                ps.setDouble(2, rand.nextDouble() * max_value);
                ps.setInt(3, 0);
                ps.addBatch();
                ps.clearParameters();
            }

            int[] results = ps.executeBatch();
            con.commit();
            ps.close();
            con.close();
            System.out.println("Inserted tuples: " + results.length);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        NoLiveLocks b = new NoLiveLocks();
        b.cerateDB();

        ArrayList<NoLiveLocks.Blocker> my_tasks = new ArrayList<>(num_taks);
        for (int task = 0; task < num_taks; task++) {
            my_tasks.add(b.new Blocker(task));
        }

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
        CompletionService<AttemptResult> results = new ExecutorCompletionService<>(executor);
        ArrayDeque<Blocker> waiting = new ArrayDeque<>(my_tasks);
        boolean exclusiveActive = false;
        int running = 0;
        int processed = 0;
        double start = System.currentTimeMillis();
        try {
            while (processed < num_taks) {
                // Dispatch tasks in FIFO order. Each submitted task performs one attempt.
                while (running < 4 && !waiting.isEmpty()) {
                    // If retry attempt escalated, task must run alone. 
                    if (waiting.peekFirst().attempts >= THRESHOLD && running > 0) break;
                    if (running > 0 && exclusiveActive) break;
                    Blocker next = waiting.removeFirst();
                    results.submit(() -> new AttemptResult(next, next.runAttempt()));
                    running++;
                    if (next.attempts >= THRESHOLD) {
                        exclusiveActive = true;
                        b.exclusive_runs.incrementAndGet();
                        break;
                    }
                }
                if (running == 0) throw new IllegalStateException("No runnable tasks remain");
                AttemptResult result = results.take().get();
                running--;
                if (exclusiveActive && running == 0) exclusiveActive = false;
                if (result.status == 0) {
                    processed++;
                } else if (result.status == 1) {
                    result.task.attempts++;
                    b.total_retries.incrementAndGet();
                    b.max_retries.accumulateAndGet(result.task.attempts, Math::max);
                    waiting.addLast(result.task);
                } else {
                    processed++; // Non-retryable SQL error: count as failed, not completed.
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        } finally {
            executor.shutdown();
        }
        double stop = System.currentTimeMillis();

        System.out.println("\n===== EXECUTION SUMMARY =====");
        System.out.println("Total tasks: " + num_taks);
        System.out.println("Completed: " + b.tasks_finished.get() + " of " + num_taks);
        System.out.println("Deadlocks caught: " + b.deadlocks.get());
        System.out.println("Total retries: " + b.total_retries.get());
        System.out.println("Maximum retries for one task: " + b.max_retries.get());
        System.out.println("Exclusive runs: " + b.exclusive_runs.get());
        System.out.println("Other SQL failures: " + b.other_failures.get());
        System.out.println("Execution time: " + (stop - start) + " ms");
    }

    public class Blocker {
        final int task;
        final double[] start_range;
        final double[] stop_range;

        public Blocker(int id) {
            this.task = id;
            this.start_range = new double[num_queries_per_task];
            this.stop_range = new double[num_queries_per_task];
            for (int q = 0; q < num_queries_per_task; q++) {
                stop_range[q] = rand.nextDouble() * max_value;
                start_range[q] = rand.nextDouble() * stop_range[q];
            }
        }

        int attempts = 0;

        // Return 0 for committed, 1 for deadlock, 2 for non-retryable failure.
        int runAttempt() {
            Connection con = null;
            PreparedStatement ps = null;
            try {
                con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                con.setAutoCommit(false);
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                ps = con.prepareStatement(
                        "UPDATE blocking_data SET data_value = ?, modified_by = " + task +
                                " WHERE data_value >= ? AND data_value <= ?;",
                        ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                int q = 0;
                while (q < num_queries_per_task) {
                    ps.setDouble(1, (float) (rand.nextDouble() * max_value));
                    ps.setDouble(2, start_range[q]);
                    ps.setDouble(3, stop_range[q]);
                    ps.execute();
                    q++;
                }
                con.commit();
                int finished = tasks_finished.incrementAndGet();
                System.out.println("Done task " + task + " after " + attempts +
                        " retries. Finisher #" + finished);
                return 0;
            } catch (SQLException e) {
                if (con != null) {
                    try { con.rollback(); } catch (SQLException rollbackEx) {
                        rollbackEx.printStackTrace();
                    }
                }
                if ("40P01".equals(e.getSQLState())) {
                    deadlocks.incrementAndGet();
                    System.err.println("Task " + task + " deadlocked. Returning to FIFO queue.");
                    return 1;
                }
                other_failures.incrementAndGet();
                System.err.println("Task " + task + " SQL error: " + e.getMessage());
                return 2;
            } finally {
                if (ps != null) try { ps.close(); } catch (SQLException ignored) {}
                if (con != null) try { con.close(); } catch (SQLException ignored) {}
            }
        }
    }

    static class AttemptResult {
        final Blocker task;
        final int status;
        AttemptResult(Blocker task, int status) {
            this.task = task;
            this.status = status;
        }
    }
}
