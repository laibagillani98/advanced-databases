
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    public static final int POSTGRESQL = 1;

    public static Connection getDefaultParameterConnection(int databaseType) {
        if (databaseType != POSTGRESQL) {
            throw new IllegalArgumentException("Unsupported database");
        }

        String url = System.getenv().getOrDefault(
                "DB_URL",
                "jdbc:postgresql://localhost:5432/homework2"
        );

        String user = System.getenv().getOrDefault(
                "DB_USER",
                "postgres"
        );

        String password = System.getenv("DB_PASSWORD");

        if (password == null || password.isEmpty()) {
            throw new IllegalStateException(
                    "DB_PASSWORD environment variable is missing"
            );
        }

        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Could not connect to PostgreSQL", e
            );
        }
    }
}
