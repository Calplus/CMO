package databaseupdater;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import discordbot.logs.DiscordLog;
import utils.UtilsConfig;
import utils.UtilsDatabase;
import utils.UtilsErrorInterceptor;
import utils.UtilsJson;
import utils.UtilsKeyValue;
import utils.UtilsLastValueFetcher;

/**
 * Updates the A01a_ClanInfo_TEXT and A01b_ClanInfo_INT tables with daily clan information from the Clash of Clans API.
 * Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}
 * DB format: Key-value pairs split by data type (TEXT vs INT)
 * DB Ordering: Date logged Ascending
 */

public class A01_ClanInfo {
    
    private static final String API_BASE_URL = "https://api.clashofclans.com/v1/clans/";
    private static final String TEXT_TABLE = "A01a_ClanInfo_TEXT";
    private static final String INT_TABLE = "A01b_ClanInfo_INT";
    
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
     * Inserts or updates clan information in the database using key-value format
     */
    public void updateDatabase() throws SQLException, IOException, InterruptedException {

        discordLogger.logInfo("Starting database update for clan: " + clanTag);

        if (!UtilsDatabase.databaseExists(dbName)) {
            String errMsg = "Database file does not exist: " + UtilsDatabase.getDatabasePath(dbName);
            System.err.println(errMsg);
            return; // Terminate early, do not proceed
        }

        // Fetch data from API
        JsonObject clanData = fetchClanInfo();
        
        // Use UTC+0 (Zulu time) in ISO 8601 format
        String currentDateTime = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC).format(java.time.format.DateTimeFormatter.ISO_INSTANT);
        String season = UtilsConfig.loadSeasonFromConfig(clanTag);

        // Fetch last stored values from database (all keys, regardless of season)
        UtilsLastValueFetcher.CombinedLastValues lastValues = UtilsLastValueFetcher.fetchLastValues(dbName, TEXT_TABLE, INT_TABLE);

        // Extract all key-value pairs from API data
        List<UtilsKeyValue.KeyValuePair> textPairs = new ArrayList<>();
        List<UtilsKeyValue.KeyValuePair> intPairs = new ArrayList<>();
        
        extractKeyValuePairs(clanData, lastValues, textPairs, intPairs);

        // Insert changed values into database
        int textInserts = UtilsKeyValue.insertTextValues(dbName, TEXT_TABLE, textPairs, currentDateTime, season, clanTag, discordLogger);
        int intInserts = UtilsKeyValue.insertIntValues(dbName, INT_TABLE, intPairs, currentDateTime, season, clanTag, discordLogger);
        
        int totalInserts = textInserts + intInserts;
        if (totalInserts > 0) {
            String successMsg = "Database write successful: Inserted " + totalInserts + " key-value pair(s) (" + textInserts + " TEXT, " + intInserts + " INT) for clan: " + clanTag;
            System.out.println(successMsg);
            discordLogger.logSuccess(successMsg);
        } else {
            String infoMsg = "No changes detected for clan: " + clanTag + " - database is up to date";
            System.out.println(infoMsg);
            discordLogger.logInfo(infoMsg);
        }
    }

    /**
     * Extracts key-value pairs from clan data and compares with last stored values
     */
    private void extractKeyValuePairs(JsonObject clanData, UtilsLastValueFetcher.CombinedLastValues lastValues, List<UtilsKeyValue.KeyValuePair> textPairs, List<UtilsKeyValue.KeyValuePair> intPairs) {
        
        // TEXT values
        UtilsKeyValue.addTextIfChanged("name", UtilsJson.getJsonString(clanData, "name"), lastValues, textPairs);
        UtilsKeyValue.addTextIfChanged("type", UtilsJson.getJsonString(clanData, "type"), lastValues, textPairs);
        UtilsKeyValue.addTextIfChanged("description", UtilsJson.getJsonString(clanData, "description"), lastValues, textPairs);
        UtilsKeyValue.addTextIfChanged("warFrequency", UtilsJson.getJsonString(clanData, "warFrequency"), lastValues, textPairs);
        
        // Location
        if (clanData.has("location") && clanData.get("location").isJsonObject()) {
            JsonObject location = clanData.getAsJsonObject("location");
            UtilsKeyValue.addTextIfChanged("locationName", UtilsJson.getJsonString(location, "name"), lastValues, textPairs);
        }
        
        // Chat language
        if (clanData.has("chatLanguage") && clanData.get("chatLanguage").isJsonObject()) {
            JsonObject language = clanData.getAsJsonObject("chatLanguage");
            UtilsKeyValue.addTextIfChanged("chatLanguage", UtilsJson.getJsonString(language, "name"), lastValues, textPairs);
        }
        
        // CWL League
        if (clanData.has("warLeague") && clanData.get("warLeague").isJsonObject()) {
            JsonObject warLeague = clanData.getAsJsonObject("warLeague");
            UtilsKeyValue.addTextIfChanged("CWLLeagueName", UtilsJson.getJsonString(warLeague, "name"), lastValues, textPairs);
        }

        // INTEGER/BOOLEAN values
        UtilsKeyValue.addIntIfChanged("memberCount", UtilsJson.getJsonInt(clanData, "members"), lastValues, intPairs);
        UtilsKeyValue.addIntIfChanged("clanLevel", UtilsJson.getJsonInt(clanData, "clanLevel"), lastValues, intPairs);
        UtilsKeyValue.addIntIfChanged("clanPoints", UtilsJson.getJsonInt(clanData, "clanPoints"), lastValues, intPairs);
        UtilsKeyValue.addIntIfChanged("clanBbPoints", UtilsJson.getJsonInt(clanData, "clanBuilderBasePoints"), lastValues, intPairs);
        UtilsKeyValue.addIntIfChanged("requiredTrophies", UtilsJson.getJsonInt(clanData, "requiredTrophies"), lastValues, intPairs);
        UtilsKeyValue.addIntIfChanged("requiredBbTrophies", UtilsJson.getJsonInt(clanData, "requiredBuilderBaseTrophies"), lastValues, intPairs);
        UtilsKeyValue.addIntIfChanged("requiredThLevel", UtilsJson.getJsonInt(clanData, "requiredTownhallLevel"), lastValues, intPairs);
        UtilsKeyValue.addIntIfChanged("warWinStreak", UtilsJson.getJsonInt(clanData, "warWinStreak"), lastValues, intPairs);
        UtilsKeyValue.addIntIfChanged("warWins", UtilsJson.getJsonInt(clanData, "warWins"), lastValues, intPairs);
        UtilsKeyValue.addIntIfChanged("warTies", UtilsJson.getJsonInt(clanData, "warTies"), lastValues, intPairs);
        UtilsKeyValue.addIntIfChanged("warLosses", UtilsJson.getJsonInt(clanData, "warLosses"), lastValues, intPairs);
        
        // Boolean as int (0 or 1)
        Boolean isFamilyFriendly = UtilsJson.getJsonBoolean(clanData, "isFamilyFriendly");
        UtilsKeyValue.addIntIfChanged("isFamilyFriendly", isFamilyFriendly != null ? (isFamilyFriendly ? 1 : 0) : null, lastValues, intPairs);
        
        Boolean isWarLogPublic = UtilsJson.getJsonBoolean(clanData, "isWarLogPublic");
        UtilsKeyValue.addIntIfChanged("isWarLogPublic", isWarLogPublic != null ? (isWarLogPublic ? 1 : 0) : null, lastValues, intPairs);

        // Clan Capital
        if (clanData.has("clanCapital") && clanData.get("clanCapital").isJsonObject()) {
            JsonObject clanCapital = clanData.getAsJsonObject("clanCapital");
            UtilsKeyValue.addIntIfChanged("capitalHallLevel", UtilsJson.getJsonInt(clanCapital, "capitalHallLevel"), lastValues, intPairs);
            
            if (clanData.has("clanCapitalPoints")) {
                UtilsKeyValue.addIntIfChanged("capitalPoints", UtilsJson.getJsonInt(clanData, "clanCapitalPoints"), lastValues, intPairs);
            }
            
            if (clanCapital.has("districts") && clanCapital.get("districts").isJsonArray()) {
                JsonArray districts = clanCapital.getAsJsonArray("districts");
                UtilsKeyValue.addIntIfChanged("lvlCapitalPeak", getDistrictLevel(districts, "Capital Peak"), lastValues, intPairs);
                UtilsKeyValue.addIntIfChanged("lvlBarbarianCamp", getDistrictLevel(districts, "Barbarian Camp"), lastValues, intPairs);
                UtilsKeyValue.addIntIfChanged("lvlWizardValley", getDistrictLevel(districts, "Wizard Valley"), lastValues, intPairs);
                UtilsKeyValue.addIntIfChanged("lvlBalloonLagoon", getDistrictLevel(districts, "Balloon Lagoon"), lastValues, intPairs);
                UtilsKeyValue.addIntIfChanged("lvlBuildersWorkshop", getDistrictLevel(districts, "Builder's Workshop"), lastValues, intPairs);
                UtilsKeyValue.addIntIfChanged("lvlDragonCliffs", getDistrictLevel(districts, "Dragon Cliffs"), lastValues, intPairs);
                UtilsKeyValue.addIntIfChanged("lvlGolemQuarry", getDistrictLevel(districts, "Golem Quarry"), lastValues, intPairs);
                UtilsKeyValue.addIntIfChanged("lvlSkeletonPark", getDistrictLevel(districts, "Skeleton Park"), lastValues, intPairs);
                UtilsKeyValue.addIntIfChanged("lvlGoblinMines", getDistrictLevel(districts, "Goblin Mines"), lastValues, intPairs);
            }
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
