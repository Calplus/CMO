package utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Utility class for configuration and environment operations
 */
public class UtilsConfig {
    
    private static final String ENV_FILE = ".env";
    private static final String CLAN_CONFIG_DIR = "src/config/clans/";
    
    /**
     * Loads a property from the .env file
     * @param propertyName The name of the property to load
     * @return The property value
     * @throws RuntimeException if the .env file cannot be read or property is not found
     */
    public static String loadEnvProperty(String propertyName) {
        try {
            Path envPath = Paths.get(ENV_FILE);
            Properties props = new Properties();
            props.load(Files.newInputStream(envPath));
            
            String value = props.getProperty(propertyName);
            
            if (value == null || value.isEmpty()) {
                throw new IllegalStateException(propertyName + " not found in .env file");
            }
            
            return value;
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to load .env file", e);
        }
    }
    
    /**
     * Loads the API key from the .env file
     * @return The API key
     * @throws RuntimeException if the .env file cannot be read or API key is not found
     */
    public static String loadApiKey() {
        return loadEnvProperty("API_COC_KEY");
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
