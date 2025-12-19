package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import com.google.gson.JsonObject;

import discordbot.logs.DiscordLog;

/**
 * Utility class for key-value pair database operations
 */
public class UtilsKeyValue {
    
    /**
     * Simple key-value pair container
     */
    public static class KeyValuePair {
        public final String key;
        public final Object value;

        public KeyValuePair(String key, Object value) {
            this.key = key;
            this.value = value;
        }
    }
    
    /**
     * Adds a text key-value pair if it has changed
     * @param key The key name
     * @param newValue The new value
     * @param lastValues The last stored values
     * @param pairs The list to add the pair to if changed
     */
    public static void addTextIfChanged(String key, String newValue, UtilsLastValueFetcher.CombinedLastValues lastValues, List<KeyValuePair> pairs) {
        String lastValue = lastValues.getTextValue(key);
        if (!Objects.equals(newValue, lastValue)) {
            pairs.add(new KeyValuePair(key, newValue));
        }
    }

    /**
     * Adds an integer key-value pair if it has changed
     * @param key The key name
     * @param newValue The new value
     * @param lastValues The last stored values
     * @param pairs The list to add the pair to if changed
     */
    public static void addIntIfChanged(String key, Integer newValue, UtilsLastValueFetcher.CombinedLastValues lastValues, List<KeyValuePair> pairs) {
        Integer lastValue = lastValues.getIntValue(key);
        if (!Objects.equals(newValue, lastValue)) {
            pairs.add(new KeyValuePair(key, newValue));
        }
    }

    /**
     * Adds a text key-value pair from a nested JSON object if it has changed
     * @param key The key name for the database
     * @param parentObject The parent JSON object
     * @param nestedObjectKey The key of the nested object in the parent
     * @param nestedValueKey The key of the value within the nested object
     * @param lastValues The last stored values
     * @param pairs The list to add the pair to if changed
     */
    public static void addNestedTextIfChanged(String key, JsonObject parentObject, String nestedObjectKey, 
                                              String nestedValueKey, UtilsLastValueFetcher.CombinedLastValues lastValues, 
                                              List<KeyValuePair> pairs) {
        if (parentObject != null && parentObject.has(nestedObjectKey) && parentObject.get(nestedObjectKey).isJsonObject()) {
            JsonObject nestedObject = parentObject.getAsJsonObject(nestedObjectKey);
            String newValue = UtilsJson.getJsonString(nestedObject, nestedValueKey);
            addTextIfChanged(key, newValue, lastValues, pairs);
        }
    }

    /**
     * Adds an integer key-value pair from a nested JSON object if it has changed
     * @param key The key name for the database
     * @param parentObject The parent JSON object
     * @param nestedObjectKey The key of the nested object in the parent
     * @param nestedValueKey The key of the value within the nested object
     * @param lastValues The last stored values
     * @param pairs The list to add the pair to if changed
     */
    public static void addNestedIntIfChanged(String key, JsonObject parentObject, String nestedObjectKey, 
                                             String nestedValueKey, UtilsLastValueFetcher.CombinedLastValues lastValues, 
                                             List<KeyValuePair> pairs) {
        if (parentObject != null && parentObject.has(nestedObjectKey) && parentObject.get(nestedObjectKey).isJsonObject()) {
            JsonObject nestedObject = parentObject.getAsJsonObject(nestedObjectKey);
            Integer newValue = UtilsJson.getJsonInt(nestedObject, nestedValueKey);
            addIntIfChanged(key, newValue, lastValues, pairs);
        }
    }

    /**
     * Inserts TEXT key-value pairs into the database
     * @param dbName The database name
     * @param tableName The table name
     * @param pairs The list of key-value pairs to insert
     * @param currentDateTime The current date/time string
     * @param season The season string
     * @param clanTag The clan tag (for error messages)
     * @param discordLogger The Discord logger
     * @return The number of rows inserted
     * @throws SQLException if a database error occurs
     */
    public static int insertTextValues(String dbName, String tableName, List<KeyValuePair> pairs, 
                                       String currentDateTime, String season, String clanTag, 
                                       DiscordLog discordLogger) throws SQLException {
        if (pairs.isEmpty()) {
            return 0;
        }

        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "INSERT INTO " + tableName + " (dateLogged, season, key, value) VALUES (?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            for (KeyValuePair pair : pairs) {
                pstmt.setString(1, currentDateTime);
                pstmt.setString(2, season);
                pstmt.setString(3, pair.key);
                UtilsDatabase.setStringOrNull(pstmt, 4, (String) pair.value);
                pstmt.addBatch();
            }

            int[] results = pstmt.executeBatch();
            return results.length;

        } catch (SQLException e) {
            String errMsg = "Failed to insert TEXT values for clan: " + clanTag + " - " + e.getMessage();
            System.err.println(errMsg);
            if (discordLogger != null) {
                discordLogger.logError(errMsg);
            }
            throw e;
        }
    }

    /**
     * Inserts INT key-value pairs into the database
     * @param dbName The database name
     * @param tableName The table name
     * @param pairs The list of key-value pairs to insert
     * @param currentDateTime The current date/time string
     * @param season The season string
     * @param clanTag The clan tag (for error messages)
     * @param discordLogger The Discord logger
     * @return The number of rows inserted
     * @throws SQLException if a database error occurs
     */
    public static int insertIntValues(String dbName, String tableName, List<KeyValuePair> pairs, 
                                      String currentDateTime, String season, String clanTag, 
                                      DiscordLog discordLogger) throws SQLException {
        if (pairs.isEmpty()) {
            return 0;
        }

        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "INSERT INTO " + tableName + " (dateLogged, season, key, value) VALUES (?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            for (KeyValuePair pair : pairs) {
                pstmt.setString(1, currentDateTime);
                pstmt.setString(2, season);
                pstmt.setString(3, pair.key);
                UtilsDatabase.setIntOrNull(pstmt, 4, (Integer) pair.value);
                pstmt.addBatch();
            }

            int[] results = pstmt.executeBatch();
            return results.length;

        } catch (SQLException e) {
            String errMsg = "Failed to insert INT values for clan: " + clanTag + " - " + e.getMessage();
            System.err.println(errMsg);
            if (discordLogger != null) {
                discordLogger.logError(errMsg);
            }
            throw e;
        }
    }
}
