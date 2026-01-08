package com.calplus.cmo.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Utility class for configuration and environment operations
 */
public class UtilsConfig {
    
    private static final String CLAN_CONFIG_DIR = "config/clans/";
    
    /**
     * Loads a property from the application.properties file
     * @param propertyName The name of the property to load
     * @return The property value
     * @throws RuntimeException if property is not found or cannot be loaded
     */
    public static String loadProperty(String propertyName) {
        String value = PropertyResolver.getProperty(propertyName);
        
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException(propertyName + " not found in application.properties");
        }
        
        return value;
    }
    
    /**
     * Loads the API key from the application.properties file
     * @return The API key
     * @throws RuntimeException if the API key is not found or cannot be loaded
     */
    public static String loadApiKey() {
        return loadProperty("api.coc.key");
    }
    
    /**
     * Loads the season from a clan's config JSON file
     * @param clanTag The clan tag (without # symbol)
     * @return The season string
     * @throws RuntimeException if the config file cannot be read or season is not found
     */
    public static String loadSeasonFromConfig(String clanTag) {
        String configFileName = CLAN_CONFIG_DIR + clanTag + ".json";

        try {
            Path configPath = Paths.get(configFileName);

            if (!Files.exists(configPath)) {
                throw new IOException("Config file not found: " + configFileName);
            }

            String jsonContent = Files.readString(configPath);
            JsonObject config = JsonParser.parseString(jsonContent).getAsJsonObject();

            if (config.has("season") && !config.get("season").isJsonNull()) {
                return config.get("season").getAsString();
            } else {
                throw new IOException("'season' not found in config file: " + configFileName);
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to load season from config: " + e.getMessage(), e);
        }
    }
    
    /**
     * Converts an exception's stack trace to a string
     * @param e The exception
     * @return The stack trace as a string
     */
    public static String getStackTraceAsString(Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append(e.getClass().getName()).append(": ").append(e.getMessage()).append("\n");
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append("  at ").append(element.toString()).append("\n");
        }
        return sb.toString();
    }
}
