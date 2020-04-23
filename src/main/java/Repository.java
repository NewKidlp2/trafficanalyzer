import java.sql.*;

public class Repository {
    final static private String DATA_BASE_URL = "jdbc:postgresql://localhost:5432/limits";
    final static private String JDBC_DRIVER = "org.postgresql.Driver";

    final static private String ADMIN = "postgres";
    final static private String PASSWORD = "1238799";

    public int getMax() throws ClassNotFoundException, SQLException {
        int max = 0;
        Class.forName(JDBC_DRIVER);
        ResultSet resultSet;
        try (Connection connection = DriverManager.getConnection(DATA_BASE_URL, ADMIN, PASSWORD);
             Statement statement = connection.createStatement()) {
            String sql = "SELECT max_val FROM limits_per_hour WHERE date = (SELECT MAX(date) FROM limits_per_hour);";
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                max = resultSet.getInt(1);
            }
        }
        return max;
    }

    public int getMin() throws ClassNotFoundException, SQLException {
        int min = 0;
        Class.forName(JDBC_DRIVER);
        ResultSet resultSet;
        try (Connection connection = DriverManager.getConnection(DATA_BASE_URL, ADMIN, PASSWORD);
             Statement statement = connection.createStatement()) {
            String sql = "SELECT min_val FROM limits_per_hour WHERE date = (SELECT MAX(date) FROM limits_per_hour);";
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                min = resultSet.getInt(1);
            }
        }
        return min;
    }
}
