package com.calplus.cmo.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Example utility class demonstrating how to read JSON configuration files
 * whose paths are defined in application.properties.
 * 
 * This shows the recommended pattern for accessing configuration files
 * that are external to the JAR (like clan configs, timing settings, etc.)
 */
public class ConfigReader {
    
    /**
     * Example: Reading a clan configuration JSON file using the path from application.properties
     * 
     * @param clanTag The clan tag (without # symbol)
     * @return JsonObject containing the clan configuration
     * @throws IOException if the config file cannot be read
     */
    public static JsonObject readClanConfig(String clanTag) throws IOException {
        // Get the clans directory path from application.properties
        String clansDir = PropertyResolver.getProperty("config.clans.directory", "config/clans");
        
        // Build the full path to the clan's JSON file
        String configFilePath = clansDir + "/" + clanTag + ".json";
        Path configPath = Paths.get(configFilePath);
        
        if (!Files.exists(configPath)) {
            throw new IOException("Clan config file not found: " + configFilePath);
        }
        
        // Read and parse the JSON file
        String jsonContent = Files.readString(configPath);
        return JsonParser.parseString(jsonContent).getAsJsonObject();
    }
    
    /**
     * Example: Reading a specific setting from a clan's config
     * 
     * @param clanTag The clan tag (without # symbol)
     * @param settingKey The JSON key to retrieve (e.g., "season", "updateTime")
     * @return The value as a String, or null if not found
     */
    public static String getClanSetting(String clanTag, String settingKey) {
        try {
            JsonObject config = readClanConfig(clanTag);
            
            if (config.has(settingKey) && !config.get(settingKey).isJsonNull()) {
                return config.get(settingKey).getAsString();
            }
            
            return null;
        } catch (IOException e) {
            System.err.println("Error reading clan config: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Example: Reading timing configuration from application.properties
     * This demonstrates how you might store timing intervals in properties
     * 
     * @return The update interval in milliseconds
     */
    public static long getUpdateInterval() {
        // You can add timing configs to application.properties like:
        // update.interval.ms=300000
        String intervalStr = PropertyResolver.getProperty("update.interval.ms", "300000");
        
        try {
            return Long.parseLong(intervalStr);
        } catch (NumberFormatException e) {
            System.err.println("Invalid update interval, using default: " + e.getMessage());
            return 300000; // Default: 5 minutes
        }
    }
    
    /**
     * Example: Reading COC data configuration
     * 
     * @param dataFileName The name of the data file (e.g., "thLeagueCount.json")
     * @return JsonObject containing the COC data
     * @throws IOException if the data file cannot be read
     */
    public static JsonObject readCocData(String dataFileName) throws IOException {
        // Get the cocdata directory path from application.properties
        String cocdataDir = PropertyResolver.getProperty("config.cocdata.directory", "config/cocdata");
        
        // Build the full path to the data file
        String dataFilePath = cocdataDir + "/" + dataFileName;
        Path dataPath = Paths.get(dataFilePath);
        
        if (!Files.exists(dataPath)) {
            throw new IOException("COC data file not found: " + dataFilePath);
        }
        
        // Read and parse the JSON file
        String jsonContent = Files.readString(dataPath);
        return JsonParser.parseString(jsonContent).getAsJsonObject();
    }
    
    /**
     * Example: Checking if a config file exists before reading
     * 
     * @param clanTag The clan tag to check
     * @return true if the clan config exists, false otherwise
     */
    public static boolean clanConfigExists(String clanTag) {
        String clansDir = PropertyResolver.getProperty("config.clans.directory", "config/clans");
        String configFilePath = clansDir + "/" + clanTag + ".json";
        Path configPath = Paths.get(configFilePath);
        return Files.exists(configPath);
    }
    
    /**
     * Example: Getting the config directory path for external use
     * 
     * @return The absolute path to the clans config directory
     */
    public static Path getClansConfigDirectory() {
        String clansDir = PropertyResolver.getProperty("config.clans.directory", "config/clans");
        return Paths.get(clansDir).toAbsolutePath();
    }
}
