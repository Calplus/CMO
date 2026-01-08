package com.calplus.cmo.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Utility class for loading and resolving application properties.
 * Loads properties from .env file and application.properties, with .env taking precedence.
 * This class provides centralized property management for the application.
 */
public class PropertyResolver {
    private static final String PROPERTIES_FILE = "/application.properties";
    private static final String ENV_FILE = ".env";
    private static Properties properties;
    private static final Object lock = new Object();

    /**
     * Loads properties from both .env file and application.properties.
     * .env file values take precedence over application.properties.
     * Properties are cached after first load.
     * 
     * @return Properties object containing all application properties
     * @throws IOException if the properties files cannot be loaded
     */
    public static Properties loadProperties() throws IOException {
        if (properties == null) {
            synchronized (lock) {
                if (properties == null) {
                    properties = new Properties();
                    
                    // First load from application.properties (classpath)
                    try (InputStream input = PropertyResolver.class.getResourceAsStream(PROPERTIES_FILE)) {
                        if (input != null) {
                            properties.load(input);
                        }
                    }
                    
                    // Then load from .env file (external), which overrides application.properties
                    Path envPath = Paths.get(System.getProperty("user.dir"), ENV_FILE);
                    if (Files.exists(envPath)) {
                        Properties envProps = new Properties();
                        try {
                            Files.lines(envPath).forEach(line -> {
                                // Skip comments and empty lines
                                if (line.trim().isEmpty() || line.trim().startsWith("#")) {
                                    return;
                                }
                                String[] parts = line.split("=", 2);
                                if (parts.length == 2) {
                                    String key = parts[0].trim();
                                    String value = parts[1].trim();
                                    
                                    // Map old .env keys to new application.properties keys
                                    if (key.equals("API_COC_KEY")) {
                                        envProps.setProperty("api.coc.key", value);
                                    } else if (key.equals("DISCORD_BOT_TOKEN")) {
                                        envProps.setProperty("discord.bot.token", value);
                                    } else if (key.equals("DISCORD_LOG_CHANNELID")) {
                                        envProps.setProperty("discord.log.channelId", value);
                                    } else if (key.equals("DISCORD_ADMIN_USERID")) {
                                        envProps.setProperty("discord.admin.userId", value);
                                    } else {
                                        // Also add with original key for backward compatibility
                                        envProps.setProperty(key, value);
                                    }
                                }
                            });
                            // Merge env properties (they override application.properties)
                            properties.putAll(envProps);
                        } catch (IOException e) {
                            System.err.println("Warning: Failed to load .env file: " + e.getMessage());
                        }
                    }
                }
            }
        }
        return properties;
    }

    /**
     * Gets a specific property value by key.
     * 
     * @param key The property key to retrieve
     * @return The property value, or null if not found
     */
    public static String getProperty(String key) {
        try {
            Properties props = loadProperties();
            return props.getProperty(key);
        } catch (IOException e) {
            System.err.println("Error loading properties: " + e.getMessage());
            return null;
        }
    }

    /**
     * Gets a specific property value by key with a default value.
     * 
     * @param key The property key to retrieve
     * @param defaultValue The default value to return if property is not found
     * @return The property value, or defaultValue if not found
     */
    public static String getProperty(String key, String defaultValue) {
        try {
            Properties props = loadProperties();
            return props.getProperty(key, defaultValue);
        } catch (IOException e) {
            System.err.println("Error loading properties: " + e.getMessage());
            return defaultValue;
        }
    }

    /**
     * Reloads the properties from the file.
     * Useful for testing or when properties need to be refreshed.
     * 
     * @throws IOException if the properties file cannot be loaded
     */
    public static void reload() throws IOException {
        synchronized (lock) {
            properties = null;
            loadProperties();
        }
    }

    /**
     * Checks if a property exists.
     * 
     * @param key The property key to check
     * @return true if the property exists, false otherwise
     */
    public static boolean hasProperty(String key) {
        try {
            Properties props = loadProperties();
            return props.containsKey(key);
        } catch (IOException e) {
            System.err.println("Error loading properties: " + e.getMessage());
            return false;
        }
    }
}
