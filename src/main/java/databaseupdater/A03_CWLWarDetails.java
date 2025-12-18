package databaseupdater;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import discordbot.logs.DiscordLog;
import utils.UtilsConfig;
import utils.UtilsDatabase;
import utils.UtilsErrorInterceptor;
import utils.UtilsJson;

/**
 * Updates the A03_CWLWarDetails table with CWL war information from the Clash of Clans API.
 * Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/currentwar/leaguegroup
 * Secondary endpoint: https://api.clashofclans.com/v1/clanwarleagues/wars/%23{warTag}
 * DB format: Each row represents one CWL war
 * DB Ordering: Season > War number > War tag
 */
public class A03_CWLWarDetails {
    
    private static final String LEAGUE_GROUP_API = "https://api.clashofclans.com/v1/clans/";
    private static final String WAR_DETAILS_API = "https://api.clashofclans.com/v1/clanwarleagues/wars/";
    private static final String TABLE_NAME = "A03_CWLWarDetails";
    
    private String apiKey;
    private String dbName;
    private String clanTag;
    private DiscordLog discordLogger;
    private A04_CWLAttackDetails attackDetailsUpdater;
    private boolean isTestMode = false;
    
    public A03_CWLWarDetails(String dbName) {
        this.dbName = dbName;
        this.clanTag = dbName.replace(".db", "");
        this.discordLogger = new DiscordLog();
        
        // Setup error interception to log uncaught exceptions
        UtilsErrorInterceptor.setupErrorInterception(this.discordLogger);
        
        this.apiKey = UtilsConfig.loadApiKey();
        this.attackDetailsUpdater = new A04_CWLAttackDetails(dbName, discordLogger);
    }
    
    /**
     * Fetches the current CWL league group from the API
     */
    private JsonObject fetchLeagueGroup() throws IOException, InterruptedException {
        String url = LEAGUE_GROUP_API + "%23" + clanTag + "/currentwar/leaguegroup";
        
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + apiKey)
                .header("Accept", "application/json")
                .GET()
                .build();
        
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() == 404) {
            JsonObject body = JsonParser.parseString(response.body()).getAsJsonObject();
            if (body.has("reason") && "notFound".equals(body.get("reason").getAsString())) {
                return null; // CWL not active
            }
        }
        
        if (response.statusCode() != 200) {
            String errMsg = "League group API request failed with status code: " + response.statusCode();
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new IOException(errMsg);
        }
        
        String successMsg = "API fetch successful (200 OK) for league group: #" + clanTag;
        System.out.println(successMsg);
        discordLogger.logSuccess(successMsg);
        
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }
    
    /**
     * Fetches detailed war information from the API
     */
    private JsonObject fetchWarDetails(String warTag) throws IOException, InterruptedException {
        // Remove # from warTag if present
        String cleanWarTag = warTag.replace("#", "");
        String url = WAR_DETAILS_API + "%23" + cleanWarTag;
        
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + apiKey)
                .header("Accept", "application/json")
                .GET()
                .build();
        
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() != 200) {
            String errMsg = "War details API request failed for warTag " + warTag + " with status code: " + response.statusCode();
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new IOException(errMsg);
        }
        
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }
    
    /**
     * Gets the last season and seasonWarState from the database
     */
    private SeasonState getLastSeasonWarState() throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT season, seasonWarState FROM " + TABLE_NAME + 
                     " ORDER BY dateLogged DESC LIMIT 1";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            if (rs.next()) {
                SeasonState state = new SeasonState();
                state.season = rs.getString("season");
                state.seasonWarState = rs.getString("seasonWarState");
                return state;
            }
        }
        
        return null;
    }
    
    /**
     * Gets unique clan tags from the database for the given season
     */
    private Set<String> getUniqueClanTags(String season) throws SQLException {
        Set<String> clanTags = new HashSet<>();
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT DISTINCT clan1Tag, clan2Tag FROM " + TABLE_NAME + 
                     " WHERE season = ?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, season);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String clan1 = rs.getString("clan1Tag");
                    String clan2 = rs.getString("clan2Tag");
                    if (clan1 != null && !clan1.equals("#" + clanTag)) {
                        clanTags.add(clan1.replace("#", ""));
                    }
                    if (clan2 != null && !clan2.equals("#" + clanTag)) {
                        clanTags.add(clan2.replace("#", ""));
                    }
                }
            }
        }
        
        return clanTags;
    }
    
    /**
     * Updates the season value in the clan config JSON file
     */
    private void updateSeasonInConfig(String newSeason) throws IOException {
        if (isTestMode) {
            System.out.println("Test mode: Skipping season update in config file");
            return;
        }
        
        String configPath = "src/config/clans/" + clanTag + ".json";
        File configFile = new File(configPath);
        
        if (!configFile.exists()) {
            String warnMsg = "Config file not found: " + configPath;
            System.err.println(warnMsg);
            discordLogger.logWarning(warnMsg);
            return;
        }
        
        try {
            String content = new String(Files.readAllBytes(Paths.get(configPath)));
            JsonObject config = JsonParser.parseString(content).getAsJsonObject();
            
            String currentSeason = config.has("season") ? config.get("season").getAsString() : null;
            
            if (!newSeason.equals(currentSeason)) {
                config.addProperty("season", newSeason);
                
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                try (FileWriter writer = new FileWriter(configFile)) {
                    gson.toJson(config, writer);
                }
                
                String infoMsg = "Updated season in config from " + currentSeason + " to " + newSeason;
                System.out.println(infoMsg);
                discordLogger.logInfo(infoMsg);
            }
        } catch (Exception e) {
            String errMsg = "Failed to update season in config: " + e.getMessage();
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new IOException(errMsg, e);
        }
    }
    
    /**
     * Determines the winning clan tag based on stars and destruction percentage
     */
    private String determineWinner(WarData war) {
        if (war.clan1Stars > war.clan2Stars) {
            return war.clan1Tag;
        } else if (war.clan2Stars > war.clan1Stars) {
            return war.clan2Tag;
        }
        
        // Stars are equal, check destruction percentage
        if (war.clan1DestructionPercent > war.clan2DestructionPercent) {
            return war.clan1Tag;
        } else if (war.clan2DestructionPercent > war.clan1DestructionPercent) {
            return war.clan2Tag;
        }
        
        // Complete tie
        return "tie";
    }
    
    /**
     * Gets existing war record from database by warTag
     */
    private WarRecord getExistingWarRecord(String warTag) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT * FROM " + TABLE_NAME + " WHERE warTag = ?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, warTag);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    WarRecord record = new WarRecord();
                    record.id = rs.getInt("id");
                    record.season = rs.getString("season");
                    record.seasonWarState = rs.getString("seasonWarState");
                    record.teamSize = rs.getInt("teamSize");
                    record.warNum = rs.getInt("warNum");
                    record.warTag = rs.getString("warTag");
                    record.warState = rs.getString("warState");
                    record.prepStartTime = rs.getString("prepStartTime");
                    record.startTime = rs.getString("startTime");
                    record.endTime = rs.getString("endTime");
                    record.clan1Tag = rs.getString("clan1Tag");
                    record.clan1Name = rs.getString("clan1Name");
                    record.clan1BannerUrl = rs.getString("clan1BannerUrl");
                    record.clan1Level = rs.getInt("clan1Level");
                    record.clan1Attacks = rs.getInt("clan1Attacks");
                    record.clan1Stars = rs.getInt("clan1Stars");
                    record.clan1DestructionPercent = rs.getDouble("clan1DestructionPercent");
                    record.clan2Tag = rs.getString("clan2Tag");
                    record.clan2Name = rs.getString("clan2Name");
                    record.clan2BannerUrl = rs.getString("clan2BannerUrl");
                    record.clan2Level = rs.getInt("clan2Level");
                    record.clan2Attacks = rs.getInt("clan2Attacks");
                    record.clan2Stars = rs.getInt("clan2Stars");
                    record.clan2DestructionPercent = rs.getDouble("clan2DestructionPercent");
                    record.winningClanTag = rs.getString("winningClanTag");
                    return record;
                }
            }
        }
        
        return null;
    }
    
    /**
     * Checks if war data has changed
     */
    private boolean hasWarDataChanged(WarData newData, WarRecord existingRecord) {
        return !Objects.equals(newData.seasonWarState, existingRecord.seasonWarState) ||
               !Objects.equals(newData.teamSize, existingRecord.teamSize) ||
               !Objects.equals(newData.warState, existingRecord.warState) ||
               !Objects.equals(newData.prepStartTime, existingRecord.prepStartTime) ||
               !Objects.equals(newData.startTime, existingRecord.startTime) ||
               !Objects.equals(newData.endTime, existingRecord.endTime) ||
               !Objects.equals(newData.clan1Tag, existingRecord.clan1Tag) ||
               !Objects.equals(newData.clan1Name, existingRecord.clan1Name) ||
               !Objects.equals(newData.clan1BannerUrl, existingRecord.clan1BannerUrl) ||
               !Objects.equals(newData.clan1Level, existingRecord.clan1Level) ||
               !Objects.equals(newData.clan1Attacks, existingRecord.clan1Attacks) ||
               !Objects.equals(newData.clan1Stars, existingRecord.clan1Stars) ||
               !Objects.equals(newData.clan1DestructionPercent, existingRecord.clan1DestructionPercent) ||
               !Objects.equals(newData.clan2Tag, existingRecord.clan2Tag) ||
               !Objects.equals(newData.clan2Name, existingRecord.clan2Name) ||
               !Objects.equals(newData.clan2BannerUrl, existingRecord.clan2BannerUrl) ||
               !Objects.equals(newData.clan2Level, existingRecord.clan2Level) ||
               !Objects.equals(newData.clan2Attacks, existingRecord.clan2Attacks) ||
               !Objects.equals(newData.clan2Stars, existingRecord.clan2Stars) ||
               !Objects.equals(newData.clan2DestructionPercent, existingRecord.clan2DestructionPercent) ||
               !Objects.equals(newData.winningClanTag, existingRecord.winningClanTag);
    }
    
    /**
     * Inserts or updates war details in the database
     */
    private void upsertWarDetails(WarData war, String currentDateTime, boolean dataChanged, 
                                   WarRecord existingRecord) throws SQLException {
        
        String url = UtilsDatabase.getConnectionUrl(dbName);
        
        if (existingRecord == null) {
            // Insert new war
            String sql = "INSERT INTO " + TABLE_NAME + " (" +
                         "dateLogged, season, seasonWarState, teamSize, warNum, warTag, warState, " +
                         "prepStartTime, startTime, endTime, " +
                         "clan1Tag, clan1Name, clan1BannerUrl, clan1Level, clan1Attacks, clan1Stars, clan1DestructionPercent, " +
                         "clan2Tag, clan2Name, clan2BannerUrl, clan2Level, clan2Attacks, clan2Stars, clan2DestructionPercent, " +
                         "winningClanTag" +
                         ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            try (Connection conn = DriverManager.getConnection(url);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                
                pstmt.setString(1, currentDateTime);
                pstmt.setString(2, war.season);
                pstmt.setString(3, war.seasonWarState);
                pstmt.setInt(4, war.teamSize);
                pstmt.setInt(5, war.warNum);
                pstmt.setString(6, war.warTag);
                pstmt.setString(7, war.warState);
                pstmt.setString(8, war.prepStartTime);
                pstmt.setString(9, war.startTime);
                pstmt.setString(10, war.endTime);
                pstmt.setString(11, war.clan1Tag);
                pstmt.setString(12, war.clan1Name);
                pstmt.setString(13, war.clan1BannerUrl);
                pstmt.setInt(14, war.clan1Level);
                pstmt.setInt(15, war.clan1Attacks);
                pstmt.setInt(16, war.clan1Stars);
                pstmt.setDouble(17, war.clan1DestructionPercent);
                pstmt.setString(18, war.clan2Tag);
                pstmt.setString(19, war.clan2Name);
                pstmt.setString(20, war.clan2BannerUrl);
                pstmt.setInt(21, war.clan2Level);
                pstmt.setInt(22, war.clan2Attacks);
                pstmt.setInt(23, war.clan2Stars);
                pstmt.setDouble(24, war.clan2DestructionPercent);
                pstmt.setString(25, war.winningClanTag);
                
                pstmt.executeUpdate();
                
                String infoMsg = "Inserted new war: " + war.warTag + " (War #" + war.warNum + ")";
                System.out.println(infoMsg);
                discordLogger.logInfo(infoMsg);
            }
        } else if (dataChanged) {
            // Update existing war
            String sql = "UPDATE " + TABLE_NAME + " SET " +
                         "dateLogged = ?, seasonWarState = ?, teamSize = ?, warState = ?, " +
                         "prepStartTime = ?, startTime = ?, endTime = ?, " +
                         "clan1Name = ?, clan1BannerUrl = ?, clan1Level = ?, clan1Attacks = ?, clan1Stars = ?, clan1DestructionPercent = ?, " +
                         "clan2Name = ?, clan2BannerUrl = ?, clan2Level = ?, clan2Attacks = ?, clan2Stars = ?, clan2DestructionPercent = ?, " +
                         "winningClanTag = ? " +
                         "WHERE warTag = ?";
            
            try (Connection conn = DriverManager.getConnection(url);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                
                pstmt.setString(1, currentDateTime);
                pstmt.setString(2, war.seasonWarState);
                pstmt.setInt(3, war.teamSize);
                pstmt.setString(4, war.warState);
                pstmt.setString(5, war.prepStartTime);
                pstmt.setString(6, war.startTime);
                pstmt.setString(7, war.endTime);
                pstmt.setString(8, war.clan1Name);
                pstmt.setString(9, war.clan1BannerUrl);
                pstmt.setInt(10, war.clan1Level);
                pstmt.setInt(11, war.clan1Attacks);
                pstmt.setInt(12, war.clan1Stars);
                pstmt.setDouble(13, war.clan1DestructionPercent);
                pstmt.setString(14, war.clan2Name);
                pstmt.setString(15, war.clan2BannerUrl);
                pstmt.setInt(16, war.clan2Level);
                pstmt.setInt(17, war.clan2Attacks);
                pstmt.setInt(18, war.clan2Stars);
                pstmt.setDouble(19, war.clan2DestructionPercent);
                pstmt.setString(20, war.winningClanTag);
                pstmt.setString(21, war.warTag);
                
                pstmt.executeUpdate();
                
                String infoMsg = "Updated war: " + war.warTag + " (War #" + war.warNum + ")";
                System.out.println(infoMsg);
                discordLogger.logInfo(infoMsg);
            }
        }
    }
    
    /**
     * Main update method
     */
    public void updateDatabase() throws SQLException, IOException, InterruptedException {
        discordLogger.logInfo("Starting CWL war details update for clan: #" + clanTag);
        
        if (!UtilsDatabase.databaseExists(dbName)) {
            String errMsg = "Database does not exist: " + dbName;
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new SQLException(errMsg);
        }
        
        // Get current date time
        String currentDateTime = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC)
                                .format(java.time.format.DateTimeFormatter.ISO_INSTANT);
        
        // Try to fetch league group
        JsonObject leagueGroup = fetchLeagueGroup();
        
        if (leagueGroup == null) {
            // 404 - CWL not active
            SeasonState lastState = getLastSeasonWarState();
            
            if (lastState == null) {
                // No previous data in database - ask user for test mode directly
                String infoMsg = "No CWL in progress and no previous data in database.";
                System.out.println(infoMsg);
                discordLogger.logInfo(infoMsg);
                
                System.err.println("Do you want to use test data from cocCWLOverview.json? (yes/no)");
                
                try (Scanner scanner = new Scanner(System.in)) {
                    String response = scanner.nextLine().trim().toLowerCase();
                    
                    if ("yes".equals(response) || "y".equals(response)) {
                        // Load test data
                        String testDataPath = "samplejsonfiles/cocCWLOverview.json";
                        String testData = new String(Files.readAllBytes(Paths.get(testDataPath)));
                        leagueGroup = JsonParser.parseString(testData).getAsJsonObject();
                        isTestMode = true;
                        
                        String infoMsg2 = "Using test data from cocCWLOverview.json";
                        System.out.println(infoMsg2);
                        discordLogger.logInfo(infoMsg2);
                    } else {
                        String errMsg = "User chose not to use test data. Exiting.";
                        System.err.println(errMsg);
                        discordLogger.logError(errMsg);
                        return;
                    }
                }
            } else if ("ended".equals(lastState.seasonWarState)) {
                String infoMsg = "CWL season ended. Database is up to date.";
                System.out.println(infoMsg);
                discordLogger.logInfo(infoMsg);
                return;
            } else {
                // Try alternate clan tags
                String infoMsg = "CWL not found for main clan. Trying alternate clan tags...";
                System.out.println(infoMsg);
                discordLogger.logInfo(infoMsg);
                
                Set<String> alternateTags = getUniqueClanTags(lastState.season);
                
                for (String altTag : alternateTags) {
                    String originalClanTag = this.clanTag;
                    this.clanTag = altTag;
                    
                    try {
                        leagueGroup = fetchLeagueGroup();
                        if (leagueGroup != null) {
                            String successMsg = "Found CWL data using alternate clan tag: #" + altTag;
                            System.out.println(successMsg);
                            discordLogger.logSuccess(successMsg);
                            break;
                        }
                    } catch (Exception e) {
                        // Continue to next tag
                    } finally {
                        this.clanTag = originalClanTag;
                    }
                }
                
                if (leagueGroup == null) {
                    // Still not found - ask user for test mode
                    System.err.println("ERROR: Could not fetch CWL data from any clan tag.");
                    System.err.println("Do you want to use test data from cocCWLOverview.json? (yes/no)");
                    
                    try (Scanner scanner = new Scanner(System.in)) {
                        String response = scanner.nextLine().trim().toLowerCase();
                        
                        if ("yes".equals(response) || "y".equals(response)) {
                            // Load test data
                            String testDataPath = "samplejsonfiles/cocCWLOverview.json";
                            String testData = new String(Files.readAllBytes(Paths.get(testDataPath)));
                            leagueGroup = JsonParser.parseString(testData).getAsJsonObject();
                            isTestMode = true;
                            
                            String infoMsg2 = "Using test data from cocCWLOverview.json";
                            System.out.println(infoMsg2);
                            discordLogger.logInfo(infoMsg2);
                        } else {
                            String errMsg = "User chose not to use test data. Exiting.";
                            System.err.println(errMsg);
                            discordLogger.logError(errMsg);
                            return;
                        }
                    }
                }
            }
        }
        
        // Extract season and state from league group
        String season = UtilsJson.getJsonString(leagueGroup, "season");
        String seasonWarState = UtilsJson.getJsonString(leagueGroup, "state");
        
        if (season == null || seasonWarState == null) {
            String errMsg = "Failed to extract season or state from league group response";
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new IOException(errMsg);
        }
        
        // Update config with current season (method handles internal comparison)
        updateSeasonInConfig(season);
        
        // Process rounds
        JsonArray rounds = leagueGroup.has("rounds") ? leagueGroup.getAsJsonArray("rounds") : null;
        
        if (rounds == null || rounds.size() == 0) {
            String warnMsg = "No rounds found in league group";
            System.err.println(warnMsg);
            discordLogger.logWarning(warnMsg);
            return;
        }
        
        int totalWarsProcessed = 0;
        int totalWarsSkipped = 0;
        int totalWarsUpdated = 0;
        int totalWarsInserted = 0;
        
        // Iterate through rounds
        for (int roundNum = 0; roundNum < rounds.size(); roundNum++) {
            JsonObject round = rounds.get(roundNum).getAsJsonObject();
            JsonArray warTags = round.has("warTags") ? round.getAsJsonArray("warTags") : null;
            
            if (warTags == null) continue;
            
            for (JsonElement warTagElement : warTags) {
                String warTag = warTagElement.getAsString();
                int warNum = roundNum + 1;
                
                // Skip #0 tags
                if ("#0".equals(warTag)) {
                    totalWarsSkipped++;
                    continue;
                }
                
                // Check if war already exists and if we should skip it
                WarRecord existingRecord = getExistingWarRecord(warTag);
                if (existingRecord != null) {
                    // Skip if season doesn't match or if seasonWarState is "ended"
                    if (!season.equals(existingRecord.season) || "ended".equals(existingRecord.seasonWarState)) {
                        totalWarsSkipped++;
                        continue;
                    }
                }
                
                // Fetch war details
                JsonObject warDetails = fetchWarDetails(warTag);
                
                // Parse war data
                WarData warData = parseWarData(warDetails, season, seasonWarState, warNum, warTag);
                
                // Determine winner
                warData.winningClanTag = determineWinner(warData);
                
                // Check if data changed
                boolean dataChanged = existingRecord == null || hasWarDataChanged(warData, existingRecord);
                
                // Upsert to database
                upsertWarDetails(warData, currentDateTime, dataChanged, existingRecord);
                
                if (existingRecord == null) {
                    totalWarsInserted++;
                } else if (dataChanged) {
                    totalWarsUpdated++;
                }
                
                // Call A04 to update attack details
                attackDetailsUpdater.processWarAttacks(warDetails, season, warTag, currentDateTime);
                
                totalWarsProcessed++;
            }
        }
        
        String summaryMsg = String.format(
            "CWL update complete. Processed: %d, Inserted: %d, Updated: %d, Skipped: %d",
            totalWarsProcessed, totalWarsInserted, totalWarsUpdated, totalWarsSkipped
        );
        System.out.println(summaryMsg);
        discordLogger.logSuccess(summaryMsg);
    }
    
    /**
     * Parses war data from API response
     */
    private WarData parseWarData(JsonObject warDetails, String season, String seasonWarState, 
                                  int warNum, String warTag) {
        WarData data = new WarData();
        
        data.season = season;
        data.seasonWarState = seasonWarState;
        data.teamSize = UtilsJson.getJsonInt(warDetails, "teamSize");
        data.warNum = warNum;
        data.warTag = warTag;
        data.warState = UtilsJson.getJsonString(warDetails, "state");
        data.prepStartTime = UtilsJson.getJsonString(warDetails, "preparationStartTime");
        data.startTime = UtilsJson.getJsonString(warDetails, "startTime");
        data.endTime = UtilsJson.getJsonString(warDetails, "endTime");
        
        // Parse clan 1 (clan)
        if (warDetails.has("clan") && warDetails.get("clan").isJsonObject()) {
            JsonObject clan = warDetails.getAsJsonObject("clan");
            data.clan1Tag = UtilsJson.getJsonString(clan, "tag");
            data.clan1Name = UtilsJson.getJsonString(clan, "name");
            data.clan1Level = UtilsJson.getJsonInt(clan, "clanLevel");
            data.clan1Attacks = UtilsJson.getJsonInt(clan, "attacks");
            data.clan1Stars = UtilsJson.getJsonInt(clan, "stars");
            data.clan1DestructionPercent = UtilsJson.getJsonReal(clan, "destructionPercentage");
            
            if (clan.has("badgeUrls") && clan.get("badgeUrls").isJsonObject()) {
                JsonObject badges = clan.getAsJsonObject("badgeUrls");
                data.clan1BannerUrl = UtilsJson.getJsonString(badges, "large");
            }
        }
        
        // Parse clan 2 (opponent)
        if (warDetails.has("opponent") && warDetails.get("opponent").isJsonObject()) {
            JsonObject opponent = warDetails.getAsJsonObject("opponent");
            data.clan2Tag = UtilsJson.getJsonString(opponent, "tag");
            data.clan2Name = UtilsJson.getJsonString(opponent, "name");
            data.clan2Level = UtilsJson.getJsonInt(opponent, "clanLevel");
            data.clan2Attacks = UtilsJson.getJsonInt(opponent, "attacks");
            data.clan2Stars = UtilsJson.getJsonInt(opponent, "stars");
            data.clan2DestructionPercent = UtilsJson.getJsonReal(opponent, "destructionPercentage");
            
            if (opponent.has("badgeUrls") && opponent.get("badgeUrls").isJsonObject()) {
                JsonObject badges = opponent.getAsJsonObject("badgeUrls");
                data.clan2BannerUrl = UtilsJson.getJsonString(badges, "large");
            }
        }
        
        return data;
    }
    
    /**
     * Data container for season state
     */
    private static class SeasonState {
        String season;
        String seasonWarState;
    }
    
    /**
     * Data container class for war information
     */
    private static class WarData {
        String season;
        String seasonWarState;
        Integer teamSize;
        int warNum;
        String warTag;
        String warState;
        String prepStartTime;
        String startTime;
        String endTime;
        
        String clan1Tag;
        String clan1Name;
        String clan1BannerUrl;
        Integer clan1Level;
        Integer clan1Attacks;
        Integer clan1Stars;
        Double clan1DestructionPercent;
        
        String clan2Tag;
        String clan2Name;
        String clan2BannerUrl;
        Integer clan2Level;
        Integer clan2Attacks;
        Integer clan2Stars;
        Double clan2DestructionPercent;
        
        String winningClanTag;
    }
    
    /**
     * Database record class for existing war data
     */
    private static class WarRecord {
        int id;
        String season;
        String seasonWarState;
        Integer teamSize;
        int warNum;
        String warTag;
        String warState;
        String prepStartTime;
        String startTime;
        String endTime;
        
        String clan1Tag;
        String clan1Name;
        String clan1BannerUrl;
        Integer clan1Level;
        Integer clan1Attacks;
        Integer clan1Stars;
        Double clan1DestructionPercent;
        
        String clan2Tag;
        String clan2Name;
        String clan2BannerUrl;
        Integer clan2Level;
        Integer clan2Attacks;
        Integer clan2Stars;
        Double clan2DestructionPercent;
        
        String winningClanTag;
    }
    
    /**
     * Main method for testing or standalone execution
     */
    public static void main(String[] args) {
        String dbName;
        
        if (args.length < 1) {
            dbName = "20CG8UURL.db";
            System.out.println("No database name provided. Using default: " + dbName);
        } else {
            dbName = args[0];
        }
        
        A03_CWLWarDetails updater = null;
        try {
            updater = new A03_CWLWarDetails(dbName);
            updater.updateDatabase();
            
            // Ensure all Discord messages are sent
            updater.discordLogger.flush();
            
            String successMsg = "Database update completed successfully for: " + dbName;
            System.out.println(successMsg);
            updater.discordLogger.logSuccess(successMsg);
            
        } catch (Exception e) {
            String errMsg = "Fatal error during database update: " + e.getMessage();
            System.err.println(errMsg);
            e.printStackTrace();
            
            if (updater != null) {
                updater.discordLogger.logError(errMsg + "\n" + e.toString());
                updater.discordLogger.flush();
            }
            
            System.exit(1);
        }
    }
}
