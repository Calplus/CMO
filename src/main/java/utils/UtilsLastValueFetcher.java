package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for fetching last stored values from key-value database tables
 */
public class UtilsLastValueFetcher {
    
    /**
     * Fetches the last stored TEXT values from a key-value table
     * @param dbName The database name
     * @param tableName The table name (e.g., "A01a_ClanInfo_TEXT")
     * @return Map of key -> last stored value (String)
     * @throws SQLException if a database error occurs
     */
    public static Map<String, String> fetchLastTextValues(String dbName, String tableName) throws SQLException {
        Map<String, String> lastValues = new HashMap<>();
        String url = UtilsDatabase.getConnectionUrl(dbName);
        
        // Query to get the most recent value for each unique key
        String sql = "SELECT key, value FROM " + tableName + " t1 " +
                    "WHERE id = (" +
                    "  SELECT id FROM " + tableName + " t2 " +
                    "  WHERE t2.key = t1.key " +
                    "  ORDER BY dateLogged DESC, id DESC " +
                    "  LIMIT 1" +
                    ")";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String key = rs.getString("key");
                    String value = rs.getString("value");
                    lastValues.put(key, value);
                }
            }
        }
        
        return lastValues;
    }
    
    /**
     * Fetches the last stored INTEGER values from a key-value table
     * @param dbName The database name
     * @param tableName The table name (e.g., "A01b_ClanInfo_INT")
     * @return Map of key -> last stored value (Integer)
     * @throws SQLException if a database error occurs
     */
    public static Map<String, Integer> fetchLastIntValues(String dbName, String tableName) throws SQLException {
        Map<String, Integer> lastValues = new HashMap<>();
        String url = UtilsDatabase.getConnectionUrl(dbName);
        
        // Query to get the most recent value for each unique key
        String sql = "SELECT key, value FROM " + tableName + " t1 " +
                    "WHERE id = (" +
                    "  SELECT id FROM " + tableName + " t2 " +
                    "  WHERE t2.key = t1.key " +
                    "  ORDER BY dateLogged DESC, id DESC " +
                    "  LIMIT 1" +
                    ")";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String key = rs.getString("key");
                    Integer value = rs.getInt("value");
                    if (rs.wasNull()) {
                        value = null;
                    }
                    lastValues.put(key, value);
                }
            }
        }
        
        return lastValues;
    }
    
    /**
     * Fetches both TEXT and INT last stored values
     * @param dbName The database name
     * @param textTableName The TEXT table name
     * @param intTableName The INT table name
     * @return A CombinedLastValues object containing both maps
     * @throws SQLException if a database error occurs
     */
    public static CombinedLastValues fetchLastValues(String dbName, String textTableName, String intTableName) throws SQLException {
        Map<String, String> textValues = fetchLastTextValues(dbName, textTableName);
        Map<String, Integer> intValues = fetchLastIntValues(dbName, intTableName);
        return new CombinedLastValues(textValues, intValues);
    }
    
    /**
     * Container class for combined TEXT and INT last values
     */
    public static class CombinedLastValues {
        private final Map<String, String> textValues;
        private final Map<String, Integer> intValues;
        
        public CombinedLastValues(Map<String, String> textValues, Map<String, Integer> intValues) {
            this.textValues = textValues;
            this.intValues = intValues;
        }
        
        public String getTextValue(String key) {
            return textValues.get(key);
        }
        
        public Integer getIntValue(String key) {
            return intValues.get(key);
        }
        
        public boolean hasTextValue(String key) {
            return textValues.containsKey(key);
        }
        
        public boolean hasIntValue(String key) {
            return intValues.containsKey(key);
        }
    }
}
