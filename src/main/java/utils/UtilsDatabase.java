package utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Utility class for database operations
 */
public class UtilsDatabase {
    
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
     * Sets an integer value or NULL in a PreparedStatement
     * @param pstmt The PreparedStatement to set the value in
     * @param index The parameter index (1-based)
     * @param value The integer value to set, or null to set NULL
     * @throws SQLException if a database access error occurs
     */
    public static void setIntOrNull(PreparedStatement pstmt, int index, Integer value) throws SQLException {
        if (value == null) {
            pstmt.setNull(index, java.sql.Types.INTEGER);
        } else {
            pstmt.setInt(index, value);
        }
    }
    
    /**
     * Sets a double value or NULL in a PreparedStatement
     * @param pstmt The PreparedStatement to set the value in
     * @param index The parameter index (1-based)
     * @param value The double value to set, or null to set NULL
     * @throws SQLException if a database access error occurs
     */
    public static void setDoubleOrNull(PreparedStatement pstmt, int index, Double value) throws SQLException {
        if (value == null) {
            pstmt.setNull(index, java.sql.Types.REAL);
        } else {
            pstmt.setDouble(index, value);
        }
    }
    
    /**
     * Sets a string value or NULL in a PreparedStatement
     * @param pstmt The PreparedStatement to set the value in
     * @param index The parameter index (1-based)
     * @param value The string value to set, or null to set NULL
     * @throws SQLException if a database access error occurs
     */
    public static void setStringOrNull(PreparedStatement pstmt, int index, String value) throws SQLException {
        if (value == null) {
            pstmt.setNull(index, java.sql.Types.VARCHAR);
        } else {
            pstmt.setString(index, value);
        }
    }
    
    /**
     * Sets a boolean value or NULL in a PreparedStatement
     * @param pstmt The PreparedStatement to set the value in
     * @param index The parameter index (1-based)
     * @param value The boolean value to set, or null to set NULL
     * @throws SQLException if a database access error occurs
     */
    public static void setBooleanOrNull(PreparedStatement pstmt, int index, Boolean value) throws SQLException {
        if (value == null) {
            pstmt.setNull(index, java.sql.Types.BOOLEAN);
        } else {
            pstmt.setBoolean(index, value);
        }
    }

    /**
     * Helper to get nullable Boolean from ResultSet
     * SQLite stores BOOLEAN as INTEGER (0/1), so this method handles the conversion
     * @param rs The ResultSet to get the value from
     * @param columnName The column name
     * @return The Boolean value, or null if the column is NULL
     * @throws SQLException if a database error occurs
     */
    public static Boolean getNullableBoolean(java.sql.ResultSet rs, String columnName) throws SQLException {
        Object value = rs.getObject(columnName);
        if (value == null) {
            return null;
        } else if (value instanceof Integer) {
            // SQLite stores BOOLEAN as INTEGER (0/1)
            return ((Integer) value) != 0 ? Boolean.TRUE : Boolean.FALSE;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else {
            return null;
        }
    }
}
