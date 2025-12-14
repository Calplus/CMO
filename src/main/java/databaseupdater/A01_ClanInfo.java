package databaseupdater;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import discordbot.logs.DiscordLog;
import utils.ConfigUtils;
import utils.DatabaseUtils;
import utils.ErrorInterceptor;
import utils.JsonUtils;

/**
 * Updates the A01_ClanInfo table with daily clan information from the Clash of Clans API.
 * Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}
 * DB format: New rows daily
 * DB Ordering: Date logged Ascending
 */

public class A01_ClanInfo {
    
    private static final String API_BASE_URL = "https://api.clashofclans.com/v1/clans/";
    
    private String apiKey;
    private String dbName;
    private String clanTag;
    private DiscordLog discordLogger;
    
    public A01_ClanInfo(String dbName) {
        this.dbName = dbName;
        
        // Extract clan tag from database name (remove .db extension)
        this.clanTag = dbName.replace(".db", "");
        this.discordLogger = new DiscordLog();
        
        // Setup error interception to log all errors to Discord
        ErrorInterceptor.setupErrorInterception(this.discordLogger);
        
        this.apiKey = ConfigUtils.loadApiKey();
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
     * Inserts clan information into the database
     */
    public void updateDatabase() throws SQLException, IOException, InterruptedException {

        discordLogger.logInfo("Starting database update for clan: " + clanTag);

        if (!DatabaseUtils.databaseExists(dbName)) {
            String errMsg = "Database file does not exist: " + DatabaseUtils.getDatabasePath(dbName);
            System.err.println(errMsg);
            return; // Terminate early, do not proceed
        }

        JsonObject clanData = fetchClanInfo();
        // Use UTC+0 (Zulu time) in ISO 8601 format
        String currentDateTime = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC).format(java.time.format.DateTimeFormatter.ISO_INSTANT);
        String season = ConfigUtils.loadSeasonFromConfig(clanTag);

        String url = DatabaseUtils.getConnectionUrl(dbName);

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
            DatabaseUtils.setNullableString(pstmt, 1, currentDateTime);
            DatabaseUtils.setNullableString(pstmt, 2, season);
            DatabaseUtils.setNullableString(pstmt, 3, JsonUtils.getJsonString(clanData, "name"));
            DatabaseUtils.setNullableString(pstmt, 4, JsonUtils.getJsonString(clanData, "type"));
            DatabaseUtils.setNullableString(pstmt, 5, JsonUtils.getJsonString(clanData, "description"));
            DatabaseUtils.setNullableInt(pstmt, 6, JsonUtils.getJsonInt(clanData, "members"));
            DatabaseUtils.setNullableInt(pstmt, 7, JsonUtils.getJsonInt(clanData, "clanLevel"));
            DatabaseUtils.setNullableInt(pstmt, 8, JsonUtils.getJsonInt(clanData, "clanPoints"));
            DatabaseUtils.setNullableInt(pstmt, 9, JsonUtils.getJsonInt(clanData, "clanBuilderBasePoints"));

            // Location
            String locationName = null;
            if (clanData.has("location") && clanData.get("location").isJsonObject()) {
                JsonObject location = clanData.getAsJsonObject("location");
                locationName = JsonUtils.getJsonString(location, "name");
            }
            DatabaseUtils.setNullableString(pstmt, 10, locationName);
            DatabaseUtils.setNullableBoolean(pstmt, 11, JsonUtils.getJsonBoolean(clanData, "isFamilyFriendly"));
            
            // Chat language
            String chatLanguage = null;
            if (clanData.has("chatLanguage") && clanData.get("chatLanguage").isJsonObject()) {
                JsonObject language = clanData.getAsJsonObject("chatLanguage");
                chatLanguage = JsonUtils.getJsonString(language, "name");
            }
            DatabaseUtils.setNullableString(pstmt, 12, chatLanguage);

            // Requirements
            DatabaseUtils.setNullableInt(pstmt, 13, JsonUtils.getJsonInt(clanData, "requiredTrophies"));
            DatabaseUtils.setNullableInt(pstmt, 14, JsonUtils.getJsonInt(clanData, "requiredBuilderBaseTrophies"));
            DatabaseUtils.setNullableInt(pstmt, 15, JsonUtils.getJsonInt(clanData, "requiredTownhallLevel"));

            // War info
            DatabaseUtils.setNullableString(pstmt, 16, JsonUtils.getJsonString(clanData, "warFrequency"));
            DatabaseUtils.setNullableBoolean(pstmt, 17, JsonUtils.getJsonBoolean(clanData, "isWarLogPublic"));
            DatabaseUtils.setNullableInt(pstmt, 18, JsonUtils.getJsonInt(clanData, "warWinStreak"));
            DatabaseUtils.setNullableInt(pstmt, 19, JsonUtils.getJsonInt(clanData, "warWins"));
            DatabaseUtils.setNullableInt(pstmt, 20, JsonUtils.getJsonInt(clanData, "warTies"));
            DatabaseUtils.setNullableInt(pstmt, 21, JsonUtils.getJsonInt(clanData, "warLosses"));

            // CWL League
            String cwlLeagueName = null;
            if (clanData.has("warLeague") && clanData.get("warLeague").isJsonObject()) {
                JsonObject warLeague = clanData.getAsJsonObject("warLeague");
                cwlLeagueName = JsonUtils.getJsonString(warLeague, "name");
            }
            DatabaseUtils.setNullableString(pstmt, 22, cwlLeagueName);

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
            DatabaseUtils.setNullableInt(pstmt, 23, capitalHallLevel);
            DatabaseUtils.setNullableInt(pstmt, 24, capitalPoints);

            // District levels
            if (districts != null) {
                DatabaseUtils.setNullableInt(pstmt, 25, getDistrictLevel(districts, "Capital Peak"));
                DatabaseUtils.setNullableInt(pstmt, 26, getDistrictLevel(districts, "Barbarian Camp"));
                DatabaseUtils.setNullableInt(pstmt, 27, getDistrictLevel(districts, "Wizard Valley"));
                DatabaseUtils.setNullableInt(pstmt, 28, getDistrictLevel(districts, "Balloon Lagoon"));
                DatabaseUtils.setNullableInt(pstmt, 29, getDistrictLevel(districts, "Builder's Workshop"));
                DatabaseUtils.setNullableInt(pstmt, 30, getDistrictLevel(districts, "Dragon Cliffs"));
                DatabaseUtils.setNullableInt(pstmt, 31, getDistrictLevel(districts, "Golem Quarry"));
                DatabaseUtils.setNullableInt(pstmt, 32, getDistrictLevel(districts, "Skeleton Park"));
                DatabaseUtils.setNullableInt(pstmt, 33, getDistrictLevel(districts, "Goblin Mines"));
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
                updater.discordLogger.logError("Stack trace: " + ConfigUtils.getStackTraceAsString(e));
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
}
