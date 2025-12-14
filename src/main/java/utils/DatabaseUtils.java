package utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Utility class for database operations
 */
public class DatabaseUtils {
    
    private static final String DB_PATH = "database/core/";
    
    /**
     * Checks if a database file exists
     * @param dbName The name of the database file
     * @return true if the file exists, false otherwise
     */
    public static boolean databaseExists(String dbName) {
        String dbFilePath = DB_PATH + dbName;
        Path dbPath = Paths.get(dbFilePath);
        return Files.exists(dbPath);
    }
    
    /**
     * Gets the full path to a database file
     * @param dbName The name of the database file
     * @return The full path to the database file
     */
    public static String getDatabasePath(String dbName) {
        return DB_PATH + dbName;
    }
    
    /**
     * Gets the JDBC connection URL for a database
     * @param dbName The name of the database file
     * @return The JDBC connection URL
     */
    public static String getConnectionUrl(String dbName) {
        return "jdbc:sqlite:" + getDatabasePath(dbName);
    }
    
    /**
     * Helper to set nullable Integer in PreparedStatement
     * @param pstmt The PreparedStatement to set the value on
     * @param parameterIndex The parameter index (1-based)
     * @param value The value to set (can be null)
     * @throws SQLException if a database error occurs
     */
    public static void setNullableInt(PreparedStatement pstmt, int parameterIndex, Integer value) throws SQLException {
        if (value == null) {
            pstmt.setNull(parameterIndex, java.sql.Types.INTEGER);
        } else {
            pstmt.setInt(parameterIndex, value);
        }
    }

    /**
     * Helper to set nullable String in PreparedStatement
     * @param pstmt The PreparedStatement to set the value on
     * @param parameterIndex The parameter index (1-based)
     * @param value The value to set (can be null)
     * @throws SQLException if a database error occurs
     */
    public static void setNullableString(PreparedStatement pstmt, int parameterIndex, String value) throws SQLException {
        if (value == null) {
            pstmt.setNull(parameterIndex, java.sql.Types.VARCHAR);
        } else {
            pstmt.setString(parameterIndex, value);
        }
    }

    /**
     * Helper to set nullable Boolean in PreparedStatement
     * @param pstmt The PreparedStatement to set the value on
     * @param parameterIndex The parameter index (1-based)
     * @param value The value to set (can be null)
     * @throws SQLException if a database error occurs
     */
    public static void setNullableBoolean(PreparedStatement pstmt, int parameterIndex, Boolean value) throws SQLException {
        if (value == null) {
            pstmt.setNull(parameterIndex, java.sql.Types.BOOLEAN);
        } else {
            pstmt.setBoolean(parameterIndex, value);
        }
    }
}
