package databaseupdater;

import java.io.IOException;
import java.io.PrintStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import discordbot.logs.DiscordLog;
import utils.JsonUtils;

/**
 * Updates the A01_ClanInfo table with daily clan information from the Clash of Clans API.
 * Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}
 * DB format: New rows daily
 * DB Ordering: Date logged Ascending
 */

public class A01_ClanInfo {
    
    private static final String API_BASE_URL = "https://api.clashofclans.com/v1/clans/";
    private static final String DB_PATH = "database/core/";
    private static final String ENV_FILE = ".env";
    private static final String CLAN_CONFIG_DIR = "src/config/clans/";
    
    private String apiKey;
    private String dbName;
    private String clanTag;
    private DiscordLog discordLogger;
    private static PrintStream originalErr;
    
    /**
     * Custom PrintStream that intercepts System.err and logs to Discord
     */
    private static class ErrorInterceptor extends PrintStream {
        private final DiscordLog logger;
        private final PrintStream original;
        private final StringBuilder lineBuffer = new StringBuilder();
        private static final ThreadLocal<Boolean> isIntercepting = ThreadLocal.withInitial(() -> false);

        public ErrorInterceptor(OutputStream out, DiscordLog logger, PrintStream original) {
            super(out, true);
            this.logger = logger;
            this.original = original;
        }

        @Override
        public void write(byte[] buf, int off, int len) {
            String text = new String(buf, off, len);
            original.write(buf, off, len);
            
            // Prevent infinite recursion - don't intercept if already intercepting
            if (isIntercepting.get()) {
                return;
            }
            
            try {
                isIntercepting.set(true);
                
                // Accumulate the text in buffer
                lineBuffer.append(text);
                
                // Check if we have a complete line (ends with newline)
                if (text.contains("\n")) {
                    String fullText = lineBuffer.toString().trim();
                    if (!fullText.isEmpty() && !fullText.startsWith("🔴")) {
                        // Don't log if it's already a Discord error message (starts with emoji)
                        logger.logError(fullText);
                    }
                    lineBuffer.setLength(0);
                }
            } finally {
                isIntercepting.set(false);
            }
        }

        @Override
        public void println(String x) {
            original.println(x);
            
            // Prevent infinite recursion
            if (isIntercepting.get()) {
                return;
            }
            
            try {
                isIntercepting.set(true);
                if (x != null && !x.trim().isEmpty() && !x.startsWith("🔴")) {
                    logger.logError(x);
                }
            } finally {
                isIntercepting.set(false);
            }
        }

        @Override
        public void println(Object x) {
            original.println(x);
            
            // Prevent infinite recursion
            if (isIntercepting.get()) {
                return;
            }
            
            try {
                isIntercepting.set(true);
                if (x != null) {
                    String msg = x.toString();
                    if (!msg.startsWith("🔴")) {
                        logger.logError(msg);
                    }
                }
            } finally {
                isIntercepting.set(false);
            }
        }
    }
    
    /**
     * Sets up error interception to log all System.err to Discord
     */
    private static void setupErrorInterception(DiscordLog logger) {
        if (originalErr == null) {
            originalErr = System.err;
            ErrorInterceptor interceptor = new ErrorInterceptor(originalErr, logger, originalErr);
            System.setErr(interceptor);
        }
    }
    
    public A01_ClanInfo(String dbName) {
        this.dbName = dbName;
        
        // Extract clan tag from database name (remove .db extension)
        this.clanTag = dbName.replace(".db", "");
        this.discordLogger = new DiscordLog();
        
        // Setup error interception to log all errors to Discord
        setupErrorInterception(this.discordLogger);
        
        loadApiKey();
    }

    /**
     * Loads the season from the config JSON file with the same name as the database
     */
    private String loadSeasonFromConfig() {
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
     * Loads the API key from the .env file
     */
    private void loadApiKey() {

        try {

            Path envPath = Paths.get(ENV_FILE);
            Properties props = new Properties();
            props.load(Files.newInputStream(envPath));
            this.apiKey = props.getProperty("API_COC_KEY");
            
            if (this.apiKey == null || this.apiKey.isEmpty()) {
                throw new IllegalStateException("API_COC_KEY not found in .env file");
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to load .env file", e);
        }
    }
    
    /**
     * Fetches clan information from the Clash of Clans API
     */

    private JsonObject fetchClanInfo() throws IOException, InterruptedException {
        
        String url = API_BASE_URL + "%23" + clanTag;
        
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + apiKey)
                .header("Accept", "application/json")
                .GET()
                .build();
        
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() != 200) {
            String errMsg = "API request failed with status code: " + response.statusCode();
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new IOException(errMsg);
        }
        
        String successMsg = "API fetch successful (200 OK) for clan: " + clanTag;
        System.out.println(successMsg);
        discordLogger.logSuccess(successMsg);
        
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }
    
    
    /**
     * Gets the district level by name from the clanCapital districts array
     */
    private Integer getDistrictLevel(JsonArray districts, String districtName) {

        for (int i = 0; i < districts.size(); i++) {
            JsonObject district = districts.get(i).getAsJsonObject();

            if (district.has("name") && district.get("name").getAsString().equals(districtName)) {
                Integer level = JsonUtils.getJsonInt(district, "districtHallLevel");
                return level;
            }
        }

        return null;
    }
    
    /**
     * Helper to set nullable Integer in PreparedStatement
     */
    private void setNullableInt(PreparedStatement pstmt, int parameterIndex, Integer value) throws SQLException {
        if (value == null) {
            pstmt.setNull(parameterIndex, java.sql.Types.INTEGER);
        } else {
            pstmt.setInt(parameterIndex, value);
        }
    }

    /**
     * Helper to set nullable String in PreparedStatement
     */
    private void setNullableString(PreparedStatement pstmt, int parameterIndex, String value) throws SQLException {
        if (value == null) {
            pstmt.setNull(parameterIndex, java.sql.Types.VARCHAR);
        } else {
            pstmt.setString(parameterIndex, value);
        }
    }

    /**
     * Helper to set nullable Boolean in PreparedStatement
     */
    private void setNullableBoolean(PreparedStatement pstmt, int parameterIndex, Boolean value) throws SQLException {
        if (value == null) {
            pstmt.setNull(parameterIndex, java.sql.Types.BOOLEAN);
        } else {
            pstmt.setBoolean(parameterIndex, value);
        }
    }

    /**
     * Inserts clan information into the database
     */
    public void updateDatabase() throws SQLException, IOException, InterruptedException {

        discordLogger.logInfo("Starting database update for clan: " + clanTag);

        String dbFilePath = DB_PATH + dbName;
        Path dbPath = Paths.get(dbFilePath);
        if (!Files.exists(dbPath)) {
            System.err.println("Database file does not exist: " + dbFilePath);
            return; // Terminate early, do not proceed
        }

        JsonObject clanData = fetchClanInfo();
        // Use UTC+0 (Zulu time) in ISO 8601 format
        String currentDateTime = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC).format(java.time.format.DateTimeFormatter.ISO_INSTANT);
        String season = loadSeasonFromConfig();

        String url = "jdbc:sqlite:" + dbFilePath;

        String sql = "INSERT INTO A01_ClanInfo ("
                + "dateLogged, season, name, type, description, memberCount, clanLevel, "
                + "clanPoints, clanBbPoints, locationName, isFamilyFriendly, chatLanguage, "
                + "requiredTrophies, requiredBbTrophies, requiredThLevel, "
                + "warFrequency, isWarLogPublic, warWinStreak, warWins, warTies, warLosses, CWLLeagueName, "
                + "capitalHallLevel, capitalPoints, "
                + "lvlCapitalPeak, lvlBarbarianCamp, lvlWizardValley, lvlBalloonLagoon, "
                + "lvlBuildersWorkshop, lvlDragonCliffs, lvlGolemQuarry, lvlSkeletonPark, lvlGoblinMines"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(url);
        
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            discordLogger.logInfo("Preparing database insert for clan: " + clanTag);
            
            // Basic info
            setNullableString(pstmt, 1, currentDateTime);
            setNullableString(pstmt, 2, season);
            setNullableString(pstmt, 3, JsonUtils.getJsonString(clanData, "name"));
            setNullableString(pstmt, 4, JsonUtils.getJsonString(clanData, "type"));
            setNullableString(pstmt, 5, JsonUtils.getJsonString(clanData, "description"));
            setNullableInt(pstmt, 6, JsonUtils.getJsonInt(clanData, "members"));
            setNullableInt(pstmt, 7, JsonUtils.getJsonInt(clanData, "clanLevel"));
            setNullableInt(pstmt, 8, JsonUtils.getJsonInt(clanData, "clanPoints"));
            setNullableInt(pstmt, 9, JsonUtils.getJsonInt(clanData, "clanBuilderBasePoints"));

            // Location
            String locationName = null;
            if (clanData.has("location") && clanData.get("location").isJsonObject()) {
                JsonObject location = clanData.getAsJsonObject("location");
                locationName = JsonUtils.getJsonString(location, "name");
            }
            setNullableString(pstmt, 10, locationName);
            setNullableBoolean(pstmt, 11, JsonUtils.getJsonBoolean(clanData, "isFamilyFriendly"));
            
            // Chat language
            String chatLanguage = null;
            if (clanData.has("chatLanguage") && clanData.get("chatLanguage").isJsonObject()) {
                JsonObject language = clanData.getAsJsonObject("chatLanguage");
                chatLanguage = JsonUtils.getJsonString(language, "name");
            }
            setNullableString(pstmt, 12, chatLanguage);

            // Requirements
            setNullableInt(pstmt, 13, JsonUtils.getJsonInt(clanData, "requiredTrophies"));
            setNullableInt(pstmt, 14, JsonUtils.getJsonInt(clanData, "requiredBuilderBaseTrophies"));
            setNullableInt(pstmt, 15, JsonUtils.getJsonInt(clanData, "requiredTownhallLevel"));

            // War info
            setNullableString(pstmt, 16, JsonUtils.getJsonString(clanData, "warFrequency"));
            setNullableBoolean(pstmt, 17, JsonUtils.getJsonBoolean(clanData, "isWarLogPublic"));
            setNullableInt(pstmt, 18, JsonUtils.getJsonInt(clanData, "warWinStreak"));
            setNullableInt(pstmt, 19, JsonUtils.getJsonInt(clanData, "warWins"));
            setNullableInt(pstmt, 20, JsonUtils.getJsonInt(clanData, "warTies"));
            setNullableInt(pstmt, 21, JsonUtils.getJsonInt(clanData, "warLosses"));

            // CWL League
            String cwlLeagueName = null;
            if (clanData.has("warLeague") && clanData.get("warLeague").isJsonObject()) {
                JsonObject warLeague = clanData.getAsJsonObject("warLeague");
                cwlLeagueName = JsonUtils.getJsonString(warLeague, "name");
            }
            setNullableString(pstmt, 22, cwlLeagueName);

            // Clan Capital
            Integer capitalHallLevel = null;
            Integer capitalPoints = null;
            JsonArray districts = null;

            if (clanData.has("clanCapital") && clanData.get("clanCapital").isJsonObject()) {
                JsonObject clanCapital = clanData.getAsJsonObject("clanCapital");

                capitalHallLevel = JsonUtils.getJsonInt(clanCapital, "capitalHallLevel");

                if (clanData.has("clanCapitalPoints")) {
                    capitalPoints = JsonUtils.getJsonInt(clanData, "clanCapitalPoints");
                }
                
                if (clanCapital.has("districts") && clanCapital.get("districts").isJsonArray()) {
                    districts = clanCapital.getAsJsonArray("districts");
                }
            }
            setNullableInt(pstmt, 23, capitalHallLevel);
            setNullableInt(pstmt, 24, capitalPoints);

            // District levels
            if (districts != null) {
                setNullableInt(pstmt, 25, getDistrictLevel(districts, "Capital Peak"));
                setNullableInt(pstmt, 26, getDistrictLevel(districts, "Barbarian Camp"));
                setNullableInt(pstmt, 27, getDistrictLevel(districts, "Wizard Valley"));
                setNullableInt(pstmt, 28, getDistrictLevel(districts, "Balloon Lagoon"));
                setNullableInt(pstmt, 29, getDistrictLevel(districts, "Builder's Workshop"));
                setNullableInt(pstmt, 30, getDistrictLevel(districts, "Dragon Cliffs"));
                setNullableInt(pstmt, 31, getDistrictLevel(districts, "Golem Quarry"));
                setNullableInt(pstmt, 32, getDistrictLevel(districts, "Skeleton Park"));
                setNullableInt(pstmt, 33, getDistrictLevel(districts, "Goblin Mines"));
            } else {
                // Set all district levels to null
                for (int i = 25; i <= 33; i++) {
                    pstmt.setNull(i, java.sql.Types.INTEGER);
                }
            }

            int rowsAffected = pstmt.executeUpdate();
            
            if (rowsAffected > 0) {
                String successMsg = "Database write successful: Inserted " + rowsAffected + " row(s) into A01_ClanInfo for clan: " + clanTag + " on " + currentDateTime;
                System.out.println(successMsg);
                discordLogger.logSuccess(successMsg);
            } else {
                String warnMsg = "Database write completed but no rows were affected for clan: " + clanTag;
                System.out.println(warnMsg);
                discordLogger.logWarning(warnMsg);
            }
        } catch (SQLException e) {
            String errMsg = "Database write failed for clan: " + clanTag + " - " + e.getMessage();
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw e;
        }
    }
    
    /**
     * Main method for testing or standalone execution
     */
    public static void main(String[] args) {
        String dbName;

        if (args.length < 1) {
            dbName = "20CG8UURL.db"; // Assign a temporary name for debugging
            System.err.println("[DEBUG] No dbName argument provided. Using temporary dbName: " + dbName);
        } else {
            dbName = args[0];
        }
        
        A01_ClanInfo updater = null;
        try {
            updater = new A01_ClanInfo(dbName);
            updater.updateDatabase();
            
            // Wait a bit for Discord messages to be sent before exiting
            Thread.sleep(5000);
            
        } catch (Exception e) {
            String errorMsg = "Error updating database: " + e.getMessage();
            System.err.println(errorMsg);
            e.printStackTrace();
            
            // Log to Discord if updater was initialized
            if (updater != null && updater.discordLogger != null) {
                updater.discordLogger.logError(errorMsg);
                updater.discordLogger.logError("Stack trace: " + getStackTraceAsString(e));
            }
            
            // Wait for Discord messages to be sent
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            
            System.exit(1);
        }
    }
    
    /**
     * Converts stack trace to string for logging
     */
    private static String getStackTraceAsString(Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append(e.getClass().getName()).append(": ").append(e.getMessage()).append("\n");
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append("  at ").append(element.toString()).append("\n");
        }
        return sb.toString();
    }
}
