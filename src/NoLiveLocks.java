import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * To prevent livelocks and starvation, use a PriorityQueue
 * (tasks with more retries have a higher priority).
 */
public class NoLiveLocks {
    public static int num_taks = 30;
    public static int num_queries_per_task = 3;

    Connection con;
    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;

    AtomicInteger tasks_finished = new AtomicInteger(0);
    AtomicInteger deadlocks = new AtomicInteger(0);
    AtomicInteger other_failures = new AtomicInteger(0);
    AtomicInteger total_retries = new AtomicInteger(0);

    // Make taskQueue accessible to inner Blocker class for re-queuing
    BlockingQueue<Runnable> taskQueue;

    public NoLiveLocks() {
        // Priority blocking queue to enforce fair scheduling based on retry attempts (highest attempts first)
        this.taskQueue = new PriorityBlockingQueue<>(
                num_taks,
                Comparator.comparingInt((Runnable r) -> ((Blocker) r).attempts).reversed()
                        .thenComparingInt(r -> ((Blocker) r).task)
        );
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

        for (int task = 0; task < num_taks; task++) {
            b.taskQueue.add(b.new Blocker(task));
        }

        // Custom ThreadPoolExecutor using PriorityBlockingQueue
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                4, 4,
                0L, TimeUnit.MILLISECONDS,
                b.taskQueue
        );

        double start = System.currentTimeMillis();
        executor.prestartAllCoreThreads();

        // Feed the executor until all tasks finish
        while (b.tasks_finished.get() < num_taks) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                break;
            }
        }

        executor.shutdownNow();
        double stop = System.currentTimeMillis();

        System.out.println("\nEXECUTION SUMMARY");
        System.out.println("Total tasks: " + num_taks);
        System.out.println("Completed: " + b.tasks_finished.get() + " of " + num_taks);
        System.out.println("Deadlocks: " + b.deadlocks.get());
        System.out.println("Retries: " + b.total_retries.get());
        System.out.println("Other SQL failures: " + b.other_failures.get());
        System.out.println("Execution time: " + (stop - start) + " ms");
    }

    public class Blocker implements Runnable {
        final int task;
        final double[] start_range;
        final double[] stop_range;
        int attempts = 0; // Track attempts for priority ordering

        public Blocker(int id) {
            this.task = id;
            this.start_range = new double[num_queries_per_task];
            this.stop_range = new double[num_queries_per_task];
            for (int q = 0; q < num_queries_per_task; q++) {
                stop_range[q] = rand.nextDouble() * max_value;
                start_range[q] = rand.nextDouble() * stop_range[q];
            }
        }

        @Override
        public void run() {
            Connection con = null;
            PreparedStatement ps = null;
            try {
                con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                con.setAutoCommit(false);
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                ps = con.prepareStatement(
                        "UPDATE blocking_data SET data_value = ?, modified_by = " + task + " WHERE data_value >= ? AND data_value <= ?;",
                        ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE
                );

                int q = 0;
                while (q < num_queries_per_task) {
                    ps.setDouble(1, (float) (rand.nextDouble() * max_value));
                    ps.setDouble(2, start_range[q]);
                    ps.setDouble(3, stop_range[q]);
                    ps.execute();
                    q++;
                }

                con.commit();
                int num_finished = tasks_finished.incrementAndGet();
                if (attempts > 0) {
                    System.out.println("Done task " + task + " after " + attempts + " priority retries. Finisher #" + num_finished);
                } else {
                    System.out.println("Done task " + task + ". Finisher #" + num_finished);
                }
            } catch (SQLException e) {
                if (con != null) {
                    try { con.rollback(); } catch (SQLException rollbackEx) {}
                }

                if ("40P01".equals(e.getSQLState())) {
                    deadlocks.incrementAndGet();
                    total_retries.incrementAndGet();
                    attempts++; // Increase priority weight

                    System.err.println("Task " + task + " deadlocked (40P01). Re-queueing with higher priority (attempt " + attempts + ")");

                    // Re-insert task back into the priority queue with its increased attempt counter
                    taskQueue.offer(this);
                } else {
                    other_failures.incrementAndGet();
                    System.err.println("Task " + task + " encountered non-deadlock error: " + e.getMessage());
                }
            } finally {
                if (ps != null) { try { ps.close(); } catch (SQLException ignored) {} }
                if (con != null) { try { con.close(); } catch (SQLException ignored) {} }
            }
        }
    }
}
