package utils;

import com.google.gson.JsonObject;

public class JsonUtils {
    /**
     * Safely gets a string value from JSON, returns null if not present
     */
    public static String getJsonString(JsonObject json, String key) {
        return json.has(key) && !json.get(key).isJsonNull() ? json.get(key).getAsString() : null;
    }

    /**
     * Safely gets an integer value from JSON, returns 0 if not present
     */
    public static Integer getJsonInt(JsonObject json, String key) {
        return json.has(key) && !json.get(key).isJsonNull() ? json.get(key).getAsInt() : null;
    }

    /**
     * Safely gets a boolean value from JSON, returns null if not present
     */
    public static Boolean getJsonBoolean(JsonObject json, String key) {
        return json.has(key) && !json.get(key).isJsonNull() ? json.get(key).getAsBoolean() : null;
    }

    /**
     * Safely gets a REAL (double) value from JSON, returns null if not present
     */
    public static Double getJsonReal(JsonObject json, String key) {
        return json.has(key) && !json.get(key).isJsonNull() ? json.get(key).getAsDouble() : null;
    }
}
