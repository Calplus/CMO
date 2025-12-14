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
import utils.UtilsConfig;
import utils.UtilsDatabase;
import utils.UtilsErrorInterceptor;
import utils.UtilsJson;

/**
 * Updates the A01_ClanInfo table with daily clan information from the Clash of Clans API.
 * Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}
 * DB format: New rows daily
 * DB Ordering: Date logged Ascending
 */

public class A02_ClanMembers {
    
    private static final String API_BASE_URL = "https://api.clashofclans.com/v1/clans/";
    
    private String apiKey;
    private String dbName;
    private String clanTag;
    private DiscordLog discordLogger;
    
    public A02_ClanMembers(String dbName) {
        this.dbName = dbName;
        
        // Extract clan tag from database name (remove .db extension)
        this.clanTag = dbName.replace(".db", "");
        this.discordLogger = new DiscordLog();
        
        // Setup error interception to log all errors to Discord
        UtilsErrorInterceptor.setupErrorInterception(this.discordLogger);
        
        this.apiKey = UtilsConfig.loadApiKey();
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
                Integer level = UtilsJson.getJsonInt(district, "districtHallLevel");
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

        if (!UtilsDatabase.databaseExists(dbName)) {
            String errMsg = "Database file does not exist: " + UtilsDatabase.getDatabasePath(dbName);
            System.err.println(errMsg);
            return; // Terminate early, do not proceed
        }

        JsonObject clanData = fetchClanInfo();
        // Use UTC+0 (Zulu time) in ISO 8601 format
        String currentDateTime = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC).format(java.time.format.DateTimeFormatter.ISO_INSTANT);
        String season = UtilsConfig.loadSeasonFromConfig(clanTag);

        String url = UtilsDatabase.getConnectionUrl(dbName);

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
            UtilsDatabase.setNullableString(pstmt, 1, currentDateTime);
            UtilsDatabase.setNullableString(pstmt, 2, season);
            UtilsDatabase.setNullableString(pstmt, 3, UtilsJson.getJsonString(clanData, "name"));
            UtilsDatabase.setNullableString(pstmt, 4, UtilsJson.getJsonString(clanData, "type"));
            UtilsDatabase.setNullableString(pstmt, 5, UtilsJson.getJsonString(clanData, "description"));
            UtilsDatabase.setNullableInt(pstmt, 6, UtilsJson.getJsonInt(clanData, "members"));
            UtilsDatabase.setNullableInt(pstmt, 7, UtilsJson.getJsonInt(clanData, "clanLevel"));
            UtilsDatabase.setNullableInt(pstmt, 8, UtilsJson.getJsonInt(clanData, "clanPoints"));
            UtilsDatabase.setNullableInt(pstmt, 9, UtilsJson.getJsonInt(clanData, "clanBuilderBasePoints"));

            // Location
            String locationName = null;
            if (clanData.has("location") && clanData.get("location").isJsonObject()) {
                JsonObject location = clanData.getAsJsonObject("location");
                locationName = UtilsJson.getJsonString(location, "name");
            }
            UtilsDatabase.setNullableString(pstmt, 10, locationName);
            UtilsDatabase.setNullableBoolean(pstmt, 11, UtilsJson.getJsonBoolean(clanData, "isFamilyFriendly"));
            
            // Chat language
            String chatLanguage = null;
            if (clanData.has("chatLanguage") && clanData.get("chatLanguage").isJsonObject()) {
                JsonObject language = clanData.getAsJsonObject("chatLanguage");
                chatLanguage = UtilsJson.getJsonString(language, "name");
            }
            UtilsDatabase.setNullableString(pstmt, 12, chatLanguage);

            // Requirements
            UtilsDatabase.setNullableInt(pstmt, 13, UtilsJson.getJsonInt(clanData, "requiredTrophies"));
            UtilsDatabase.setNullableInt(pstmt, 14, UtilsJson.getJsonInt(clanData, "requiredBuilderBaseTrophies"));
            UtilsDatabase.setNullableInt(pstmt, 15, UtilsJson.getJsonInt(clanData, "requiredTownhallLevel"));

            // War info
            UtilsDatabase.setNullableString(pstmt, 16, UtilsJson.getJsonString(clanData, "warFrequency"));
            UtilsDatabase.setNullableBoolean(pstmt, 17, UtilsJson.getJsonBoolean(clanData, "isWarLogPublic"));
            UtilsDatabase.setNullableInt(pstmt, 18, UtilsJson.getJsonInt(clanData, "warWinStreak"));
            UtilsDatabase.setNullableInt(pstmt, 19, UtilsJson.getJsonInt(clanData, "warWins"));
            UtilsDatabase.setNullableInt(pstmt, 20, UtilsJson.getJsonInt(clanData, "warTies"));
            UtilsDatabase.setNullableInt(pstmt, 21, UtilsJson.getJsonInt(clanData, "warLosses"));

            // CWL League
            String cwlLeagueName = null;
            if (clanData.has("warLeague") && clanData.get("warLeague").isJsonObject()) {
                JsonObject warLeague = clanData.getAsJsonObject("warLeague");
                cwlLeagueName = UtilsJson.getJsonString(warLeague, "name");
            }
            UtilsDatabase.setNullableString(pstmt, 22, cwlLeagueName);

            // Clan Capital
            Integer capitalHallLevel = null;
            Integer capitalPoints = null;
            JsonArray districts = null;

            if (clanData.has("clanCapital") && clanData.get("clanCapital").isJsonObject()) {
                JsonObject clanCapital = clanData.getAsJsonObject("clanCapital");

                capitalHallLevel = UtilsJson.getJsonInt(clanCapital, "capitalHallLevel");

                if (clanData.has("clanCapitalPoints")) {
                    capitalPoints = UtilsJson.getJsonInt(clanData, "clanCapitalPoints");
                }
                
                if (clanCapital.has("districts") && clanCapital.get("districts").isJsonArray()) {
                    districts = clanCapital.getAsJsonArray("districts");
                }
            }
            UtilsDatabase.setNullableInt(pstmt, 23, capitalHallLevel);
            UtilsDatabase.setNullableInt(pstmt, 24, capitalPoints);

            // District levels
            if (districts != null) {
                UtilsDatabase.setNullableInt(pstmt, 25, getDistrictLevel(districts, "Capital Peak"));
                UtilsDatabase.setNullableInt(pstmt, 26, getDistrictLevel(districts, "Barbarian Camp"));
                UtilsDatabase.setNullableInt(pstmt, 27, getDistrictLevel(districts, "Wizard Valley"));
                UtilsDatabase.setNullableInt(pstmt, 28, getDistrictLevel(districts, "Balloon Lagoon"));
                UtilsDatabase.setNullableInt(pstmt, 29, getDistrictLevel(districts, "Builder's Workshop"));
                UtilsDatabase.setNullableInt(pstmt, 30, getDistrictLevel(districts, "Dragon Cliffs"));
                UtilsDatabase.setNullableInt(pstmt, 31, getDistrictLevel(districts, "Golem Quarry"));
                UtilsDatabase.setNullableInt(pstmt, 32, getDistrictLevel(districts, "Skeleton Park"));
                UtilsDatabase.setNullableInt(pstmt, 33, getDistrictLevel(districts, "Goblin Mines"));
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
        
        A02_ClanMembers updater = null;
        try {
            updater = new A02_ClanMembers(dbName);
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
                updater.discordLogger.logError("Stack trace: " + UtilsConfig.getStackTraceAsString(e));
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
