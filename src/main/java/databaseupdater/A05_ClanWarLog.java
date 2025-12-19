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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
 * Updates the A05_ClanWarLog table with clan war log information from the Clash of Clans API.
 * Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/warlog
 * DB format: Stores high-level overview of war details
 * DB Ordering: First to last war
 */
public class A05_ClanWarLog {
    
    private static final String WAR_LOG_API = "https://api.clashofclans.com/v1/clans/";
    private static final String TABLE_NAME = "A05_ClanWarLog";
    
    private String apiKey;
    private String dbName;
    private String clanTag;
    private DiscordLog discordLogger;
    
    public A05_ClanWarLog(String dbName) {
        this.dbName = dbName;
        this.clanTag = dbName.replace(".db", "");
        this.discordLogger = new DiscordLog();
        
        // Setup error interception to log uncaught exceptions
        UtilsErrorInterceptor.setupErrorInterception(this.discordLogger);
        
        this.apiKey = UtilsConfig.loadApiKey();
    }
    
    /**
     * Checks if the A05_ClanWarLog table is empty
     */
    private boolean isTableEmpty() throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT COUNT(*) as count FROM " + TABLE_NAME;
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            if (rs.next()) {
                return rs.getInt("count") == 0;
            }
        }
        
        return true;
    }
    
    /**
     * Fetches war log from the API
     * @param useLimit If true, adds ?limit=5 to the URL
     */
    private JsonObject fetchWarLog(boolean useLimit) throws IOException, InterruptedException {
        String url = WAR_LOG_API + "%23" + clanTag + "/warlog";
        if (useLimit) {
            url += "?limit=5";
        }
        
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + apiKey)
                .header("Accept", "application/json")
                .GET()
                .build();
        
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() != 200) {
            String errMsg = "War log API request failed with status code: " + response.statusCode();
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new IOException(errMsg);
        }
        
        String successMsg = "API fetch successful (200 OK) for war log: #" + clanTag + (useLimit ? " (limit=5)" : "");
        System.out.println(successMsg);
        discordLogger.logSuccess(successMsg);
        
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }
    
    /**
     * Checks if a war with the given endTime exists in the database
     */
    private boolean warExists(String endTime) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT COUNT(*) as count FROM " + TABLE_NAME + " WHERE endTime = ?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, endTime);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("count") > 0;
                }
            }
        }
        
        return false;
    }
    
    /**
     * Checks if a war is a CWL (Clan War League) war
     * CWL wars have no opponent level information (null or 0)
     */
    private boolean isCwlWar(WarData war) {
        return war.opponentLevel == null || war.opponentLevel == 0;
    }
    
    /**
     * Transforms result value for CWL wars
     * Regular wars: "win", "lose", null
     * CWL wars: "promoted", "demoted", "stayed"
     */
    private String transformResultForCwl(String result) {
        if (result == null) {
            return "stayed";
        }
        switch (result) {
            case "win":
                return "promoted";
            case "tie":
                return "demoted";
            default:
                return result;
        }
    }
    
    /**
     * Gets the last cwSeason from the database
     * Returns null if no records exist
     */
    private String getLastCwSeason() throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT cwSeason FROM " + TABLE_NAME + " ORDER BY id DESC LIMIT 1";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            if (rs.next()) {
                return rs.getString("cwSeason");
            }
        }
        
        return null;
    }
    
    /**
     * Gets the previous N wars from the database (ordered by ID descending)
     * @param count Number of previous wars to retrieve
     * @return List of previous wars (most recent first)
     */
    private List<PreviousWarInfo> getPreviousWars(int count) throws SQLException {
        List<PreviousWarInfo> wars = new ArrayList<>();
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT id, cwSeason, endTime FROM " + TABLE_NAME + " ORDER BY id DESC LIMIT ?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, count);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    PreviousWarInfo info = new PreviousWarInfo();
                    info.id = rs.getInt("id");
                    info.cwSeason = rs.getString("cwSeason");
                    info.endTime = rs.getString("endTime");
                    wars.add(info);
                }
            }
        }
        
        return wars;
    }
    
    /**
     * Updates cwSeason for specific war IDs
     * @param updates Map of war ID to new cwSeason value
     */
    private void updatePreviousWarSeasons(List<PreviousWarUpdate> updates) throws SQLException {
        if (updates.isEmpty()) {
            return;
        }
        
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "UPDATE " + TABLE_NAME + " SET cwSeason = ? WHERE id = ?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            for (PreviousWarUpdate update : updates) {
                pstmt.setString(1, update.newCwSeason);
                pstmt.setInt(2, update.warId);
                pstmt.executeUpdate();
                
                String infoMsg = "Updated war ID " + update.warId + " cwSeason from '" + 
                                update.oldCwSeason + "' to '" + update.newCwSeason + "'";
                System.out.println(infoMsg);
                discordLogger.logInfo(infoMsg);
            }
        }
    }
    
    /**
     * Calculates the next cwSeason value
     * @param endTime The endTime from the war data (format: "20251201T041803.000Z")
     * @param lastCwSeason The last cwSeason from the database (can be null)
     * @param isCwlWar Whether this is a CWL war (no ID appended for CWL wars)
     * @return The next cwSeason value
     */
    private String calculateCwSeason(String endTime, String lastCwSeason, boolean isCwlWar) throws IOException {
        // Extract season from endTime (first 6 digits: YYYYMM)
        String seasonStr = endTime.substring(0, 6); // e.g., "202512"
        String formattedSeason = seasonStr.substring(0, 4) + "-" + seasonStr.substring(4); // e.g., "2025-12"
        
        // CWL wars don't have season IDs, just return the season
        if (isCwlWar) {
            return formattedSeason;
        }
        
        int cwSeasonId;
        
        if (lastCwSeason == null) {
            // No previous wars, check config file
            String configPath = "src/config/clans/" + clanTag + ".json";
            File configFile = new File(configPath);
            
            if (!configFile.exists()) {
                // No config file, start from 1
                cwSeasonId = 1;
            } else {
                try {
                    String content = new String(Files.readAllBytes(Paths.get(configPath)));
                    JsonObject config = JsonParser.parseString(content).getAsJsonObject();
                    
                    String configSeason = config.has("season") ? config.get("season").getAsString() : null;
                    
                    if (formattedSeason.equals(configSeason)) {
                        cwSeasonId = config.has("cwSeasonId") ? config.get("cwSeasonId").getAsInt() : 1;
                    } else {
                        // Different season, start from 1
                        cwSeasonId = 1;
                    }
                } catch (Exception e) {
                    cwSeasonId = 1;
                }
            }
        } else {
            // Extract season and ID from lastCwSeason
            String[] parts = lastCwSeason.split("-");
            
            // Check if lastCwSeason has an ID (format: "2025-12-9") or is just season (format: "2025-12")
            if (parts.length == 3) {
                // Last war was a regular war with ID
                String lastSeason = parts[0] + "-" + parts[1]; // e.g., "2025-12"
                int lastId = Integer.parseInt(parts[2]);
                
                if (formattedSeason.equals(lastSeason)) {
                    // Same season, increment ID
                    cwSeasonId = lastId + 1;
                } else {
                    // New season, start from 1
                    cwSeasonId = 1;
                }
            } else {
                // Last war was a CWL war (no ID), start regular wars at 1
                String lastSeason = lastCwSeason; // Already in "YYYY-MM" format
                
                if (formattedSeason.equals(lastSeason)) {
                    // Same season as CWL, start from 1
                    cwSeasonId = 1;
                } else {
                    // New season, start from 1
                    cwSeasonId = 1;
                }
            }
        }
        
        return formattedSeason + "-" + cwSeasonId;
    }
    
    /**
     * Updates the cwSeasonId in the clan config JSON file
     */
    private void updateCwSeasonIdInConfig(int nextCwSeasonId) throws IOException {
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
            
            config.addProperty("cwSeasonId", nextCwSeasonId);
            
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            try (FileWriter writer = new FileWriter(configFile)) {
                gson.toJson(config, writer);
            }
            
            String infoMsg = "Updated cwSeasonId in config to: " + nextCwSeasonId;
            System.out.println(infoMsg);
            discordLogger.logInfo(infoMsg);
        } catch (Exception e) {
            String errMsg = "Failed to update cwSeasonId in config: " + e.getMessage();
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new IOException(errMsg, e);
        }
    }
    
    /**
     * Parses war data from API response
     */
    private WarData parseWarData(JsonObject warItem, String cwSeason, boolean isCwlWar) {
        WarData data = new WarData();
        
        data.cwSeason = cwSeason;
        String rawResult = UtilsJson.getJsonString(warItem, "result");
        data.result = isCwlWar ? transformResultForCwl(rawResult) : rawResult;
        data.endTime = UtilsJson.getJsonString(warItem, "endTime");
        data.teamSize = UtilsJson.getJsonInt(warItem, "teamSize");
        data.attacksPerMember = UtilsJson.getJsonInt(warItem, "attacksPerMember");
        data.battleModifier = UtilsJson.getJsonString(warItem, "battleModifier");
        
        // Parse clan info
        if (warItem.has("clan") && warItem.get("clan").isJsonObject()) {
            JsonObject clan = warItem.getAsJsonObject("clan");
            data.clanTag = UtilsJson.getJsonString(clan, "tag");
            data.clanName = UtilsJson.getJsonString(clan, "name");
            data.clanLevel = UtilsJson.getJsonInt(clan, "clanLevel");
            data.clanAttacks = UtilsJson.getJsonInt(clan, "attacks");
            data.clanStars = UtilsJson.getJsonInt(clan, "stars");
            data.clanDestructionPercent = UtilsJson.getJsonReal(clan, "destructionPercentage");
            data.clanXPGained = UtilsJson.getJsonInt(clan, "expEarned");
        }
        
        // Parse opponent info
        if (warItem.has("opponent") && warItem.get("opponent").isJsonObject()) {
            JsonObject opponent = warItem.getAsJsonObject("opponent");
            data.opponentTag = UtilsJson.getJsonString(opponent, "tag");
            data.opponentName = UtilsJson.getJsonString(opponent, "name");
            data.opponentLevel = UtilsJson.getJsonInt(opponent, "clanLevel");
            data.opponentStars = UtilsJson.getJsonInt(opponent, "stars");
            data.opponentDestructionPercent = UtilsJson.getJsonReal(opponent, "destructionPercentage");
            
            // For CWL wars, convert 0 values to null for opponent stats
            if (isCwlWar) {
                if (data.opponentLevel != null && data.opponentLevel == 0) {
                    data.opponentLevel = null;
                }
                if (data.opponentDestructionPercent != null && data.opponentDestructionPercent == 0.0) {
                    data.opponentDestructionPercent = null;
                }
            }
            
            // Get opponent banner URL
            if (opponent.has("badgeUrls") && opponent.get("badgeUrls").isJsonObject()) {
                JsonObject badges = opponent.getAsJsonObject("badgeUrls");
                data.opponentBannerUrl = UtilsJson.getJsonString(badges, "large");
            }
        }
        
        return data;
    }
    
    /**
     * Inserts a war into the database
     */
    private void insertWar(WarData war, String currentDateTime) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "INSERT INTO " + TABLE_NAME + " (" +
                     "dateLogged, cwSeason, result, teamsize, attacksPerMember, battleModifier, endTime, " +
                     "clanTag, clanName, clanLevel, clanAttacks, clanStars, clanDestructionPercent, clanXPGained, " +
                     "opponentTag, opponentName, opponentBannerUrl, opponentLevel, opponentStars, opponentDestructionPercent" +
                     ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, currentDateTime);
            pstmt.setString(2, war.cwSeason);
            pstmt.setString(3, war.result);
            UtilsDatabase.setIntOrNull(pstmt, 4, war.teamSize);
            UtilsDatabase.setIntOrNull(pstmt, 5, war.attacksPerMember);
            pstmt.setString(6, war.battleModifier);
            pstmt.setString(7, war.endTime);
            pstmt.setString(8, war.clanTag);
            pstmt.setString(9, war.clanName);
            UtilsDatabase.setIntOrNull(pstmt, 10, war.clanLevel);
            UtilsDatabase.setIntOrNull(pstmt, 11, war.clanAttacks);
            UtilsDatabase.setIntOrNull(pstmt, 12, war.clanStars);
            UtilsDatabase.setDoubleOrNull(pstmt, 13, war.clanDestructionPercent);
            UtilsDatabase.setIntOrNull(pstmt, 14, war.clanXPGained);
            pstmt.setString(15, war.opponentTag);
            pstmt.setString(16, war.opponentName);
            pstmt.setString(17, war.opponentBannerUrl);
            UtilsDatabase.setIntOrNull(pstmt, 18, war.opponentLevel);
            UtilsDatabase.setIntOrNull(pstmt, 19, war.opponentStars);
            UtilsDatabase.setDoubleOrNull(pstmt, 20, war.opponentDestructionPercent);
            
            pstmt.executeUpdate();
            
            String infoMsg = "Inserted war: " + war.cwSeason + " (ended: " + war.endTime + ")";
            System.out.println(infoMsg);
            discordLogger.logInfo(infoMsg);
        }
    }
    
    /**
     * Main update method
     */
    public void updateDatabase() throws SQLException, IOException, InterruptedException {
        discordLogger.logInfo("Starting clan war log update for clan: #" + clanTag);
        
        if (!UtilsDatabase.databaseExists(dbName)) {
            String errMsg = "Database does not exist: " + dbName;
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new SQLException(errMsg);
        }
        
        // Get current date time
        String currentDateTime = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC)
                                .format(java.time.format.DateTimeFormatter.ISO_INSTANT);
        
        // Check if table is empty
        boolean isEmpty = isTableEmpty();
        boolean useLimit = !isEmpty;
        
        String infoMsg = isEmpty ? "Table is empty. Fetching full war log." : "Table has data. Fetching recent wars (limit=5).";
        System.out.println(infoMsg);
        discordLogger.logInfo(infoMsg);
        
        // Fetch war log
        JsonObject warLogResponse = fetchWarLog(useLimit);
        JsonArray items = warLogResponse.has("items") ? warLogResponse.getAsJsonArray("items") : null;
        
        if (items == null || items.size() == 0) {
            String warnMsg = "No wars found in war log";
            System.err.println(warnMsg);
            discordLogger.logWarning(warnMsg);
            return;
        }
        
        // If using limit, check if the last item (oldest war) exists in database
        if (useLimit) {
            JsonObject lastItem = items.get(items.size() - 1).getAsJsonObject();
            String lastEndTime = UtilsJson.getJsonString(lastItem, "endTime");
            
            if (lastEndTime != null && !warExists(lastEndTime)) {
                // Last item not in database, need to fetch full war log
                String infoMsg2 = "Recent war not in database. Fetching full war log.";
                System.out.println(infoMsg2);
                discordLogger.logInfo(infoMsg2);
                
                warLogResponse = fetchWarLog(false);
                items = warLogResponse.has("items") ? warLogResponse.getAsJsonArray("items") : null;
                
                if (items == null || items.size() == 0) {
                    String warnMsg = "No wars found in full war log";
                    System.err.println(warnMsg);
                    discordLogger.logWarning(warnMsg);
                    return;
                }
            }
        }
        
        // Process wars from last to first (reverse order)
        // This ensures we add wars in chronological order (oldest to newest)
        List<JsonObject> warsToAdd = new ArrayList<>();
        
        for (int i = items.size() - 1; i >= 0; i--) {
            JsonObject warItem = items.get(i).getAsJsonObject();
            String endTime = UtilsJson.getJsonString(warItem, "endTime");
            
            if (endTime == null) {
                continue; // Skip wars without endTime
            }
            
            if (!warExists(endTime)) {
                warsToAdd.add(warItem);
            }
        }
        
        if (warsToAdd.isEmpty()) {
            String infoMsg2 = "No new wars to add. Database is up to date.";
            System.out.println(infoMsg2);
            discordLogger.logInfo(infoMsg2);
            return;
        }
        
        // List is already in oldest-to-newest order from the backwards iteration
        
        // Get last cwSeason from database
        String lastCwSeason = getLastCwSeason();
        
        // Process and insert wars
        int totalInserted = 0;
        int cwlWarsDetected = 0;
        
        for (JsonObject warItem : warsToAdd) {
            String endTime = UtilsJson.getJsonString(warItem, "endTime");
            
            // Pre-parse to detect CWL war
            // CWL wars have opponentLevel as null or 0
            Integer opponentLevel = null;
            if (warItem.has("opponent") && warItem.get("opponent").isJsonObject()) {
                JsonObject opponent = warItem.getAsJsonObject("opponent");
                opponentLevel = UtilsJson.getJsonInt(opponent, "clanLevel");
            }
            boolean isCwlWar = (opponentLevel == null || opponentLevel == 0);
            
            // Calculate cwSeason
            String cwSeason = calculateCwSeason(endTime, lastCwSeason, isCwlWar);
            
            // If this is a CWL war, check previous wars
            if (isCwlWar) {
                cwlWarsDetected++;
                
                // Extract season from cwSeason (YYYY-MM)
                String currentSeason = cwSeason; // For CWL wars, cwSeason is already just "YYYY-MM"
                
                // Get previous 2 wars
                List<PreviousWarInfo> previousWars = getPreviousWars(2);
                List<PreviousWarUpdate> updates = new ArrayList<>();
                
                // Find last season ID from previous wars that don't match current season
                String lastValidSeason = null;
                int lastSeasonId = 0;
                
                for (PreviousWarInfo prevWar : previousWars) {
                    String prevCwSeason = prevWar.cwSeason;
                    
                    // Check if this war's season matches current CWL season
                    String prevSeason = prevCwSeason.contains("-") && prevCwSeason.split("-").length >= 2 ?
                                       prevCwSeason.substring(0, prevCwSeason.lastIndexOf("-")) : prevCwSeason;
                    
                    if (!prevSeason.equals(currentSeason)) {
                        // This war is from a different season, use it as reference
                        if (lastValidSeason == null) {
                            lastValidSeason = prevSeason;
                            // Extract ID if it exists
                            if (prevCwSeason.split("-").length == 3) {
                                lastSeasonId = Integer.parseInt(prevCwSeason.split("-")[2]);
                            }
                        }
                        break; // Stop at first different season
                    }
                }
                
                // Update wars that match the current CWL season
                for (PreviousWarInfo prevWar : previousWars) {
                    String prevCwSeason = prevWar.cwSeason;
                    String prevSeason = prevCwSeason.contains("-") && prevCwSeason.split("-").length >= 2 ?
                                       prevCwSeason.substring(0, prevCwSeason.lastIndexOf("-")) : prevCwSeason;
                    
                    if (prevSeason.equals(currentSeason)) {
                        // This war needs to be moved to previous season
                        if (lastValidSeason != null) {
                            lastSeasonId++;
                            String newCwSeason = lastValidSeason + "-" + lastSeasonId;
                            updates.add(new PreviousWarUpdate(prevWar.id, prevWar.cwSeason, newCwSeason));
                        }
                    }
                }
                
                if (!updates.isEmpty()) {
                    updatePreviousWarSeasons(updates);
                    String infoMsg2 = "Corrected " + updates.size() + " previous war(s) due to CWL detection";
                    System.out.println(infoMsg2);
                    discordLogger.logInfo(infoMsg2);
                }
                
                // After CWL war, reset cwSeasonId to 1 for next regular war in this season
                // We'll do this by updating the config
                updateCwSeasonIdInConfig(1);
            }
            
            WarData warData = parseWarData(warItem, cwSeason, isCwlWar);
            insertWar(warData, currentDateTime);
            
            lastCwSeason = cwSeason;
            totalInserted++;
        }
        
        // Update config with next cwSeasonId (only if last war was not CWL)
        if (lastCwSeason != null && lastCwSeason.split("-").length == 3) {
            String[] parts = lastCwSeason.split("-");
            int lastId = Integer.parseInt(parts[2]);
            updateCwSeasonIdInConfig(lastId + 1);
        }
        
        String summaryMsg = String.format(
            "Clan war log update complete. Inserted: %d wars (CWL: %d)",
            totalInserted, cwlWarsDetected
        );
        System.out.println(summaryMsg);
        discordLogger.logSuccess(summaryMsg);
    }
    
    /**
     * Data container for previous war information from database
     */
    private static class PreviousWarInfo {
        int id;
        String cwSeason;
        String endTime;
    }
    
    /**
     * Data container for war season update
     */
    private static class PreviousWarUpdate {
        int warId;
        String oldCwSeason;
        String newCwSeason;
        
        PreviousWarUpdate(int warId, String oldCwSeason, String newCwSeason) {
            this.warId = warId;
            this.oldCwSeason = oldCwSeason;
            this.newCwSeason = newCwSeason;
        }
    }
    
    /**
     * Data container class for war information
     */
    private static class WarData {
        String cwSeason;
        String result;
        String endTime;
        Integer teamSize;
        Integer attacksPerMember;
        String battleModifier;
        
        String clanTag;
        String clanName;
        Integer clanLevel;
        Integer clanAttacks;
        Integer clanStars;
        Double clanDestructionPercent;
        Integer clanXPGained;
        
        String opponentTag;
        String opponentName;
        String opponentBannerUrl;
        Integer opponentLevel;
        Integer opponentStars;
        Double opponentDestructionPercent;
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
        
        A05_ClanWarLog updater = null;
        try {
            updater = new A05_ClanWarLog(dbName);
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
