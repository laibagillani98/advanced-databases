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
 * NoLiveLocks: Combines Priority Queue with Randomised Backoff
 */
public class NoLiveLocks {
    public static int num_taks = 100;
    public static int num_queries_per_task = 10;

    Connection con;
    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;

    AtomicInteger tasks_finished = new AtomicInteger(0);
    AtomicInteger deadlocks = new AtomicInteger(0);
    AtomicInteger other_failures = new AtomicInteger(0);
    AtomicInteger total_retries = new AtomicInteger(0);

    BlockingQueue<Runnable> taskQueue;

    public NoLiveLocks() {
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

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                4, 4,
                0L, TimeUnit.MILLISECONDS,
                b.taskQueue
        );

        double start = System.currentTimeMillis();
        executor.prestartAllCoreThreads();

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
        System.out.println("Deadlocks caught: " + b.deadlocks.get());
        System.out.println("Total retries: " + b.total_retries.get());
        System.out.println("Execution time: " + (stop - start) + " ms");
    }

    public class Blocker implements Runnable {
        final int task;
        final double[] start_range;
        final double[] stop_range;
        int attempts = 0;

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
                System.out.println("Done task " + task + " (attempts: " + attempts + "). Finisher #" + num_finished);
            } catch (SQLException e) {
                if (con != null) { try { con.rollback(); } catch (SQLException ex) {} }

                if ("40P01".equals(e.getSQLState())) {
                    deadlocks.incrementAndGet();
                    total_retries.incrementAndGet();
                    attempts++;

                    //Randomised backoff
                    int minSleep = 10;
                    int maxSleep = 200;
                    long sleepTime = minSleep + rand.nextInt(maxSleep - minSleep);

                    try {
                        System.err.println("Task " + task + " deadlocked. Randomized sleep: " + sleepTime + "ms");
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }

                    // Re-enqueue back into the priority queue
                    taskQueue.offer(this);
                } else {
                    other_failures.incrementAndGet();
                    System.err.println("Task " + task + " error: " + e.getMessage());
                }
            } finally {
                if (ps != null) { try { ps.close(); } catch (SQLException ignored) {} }
                if (con != null) { try { con.close(); } catch (SQLException ignored) {} }
            }
        }
    }
}
