package com.calplus.cmo.databaseupdater;

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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.calplus.cmo.calculations.CalcWarQuality;
import com.calplus.cmo.discordbot.logs.DiscordLog;
import com.calplus.cmo.utils.UtilsConfig;
import com.calplus.cmo.utils.UtilsDatabase;
import com.calplus.cmo.utils.UtilsErrorInterceptor;
import com.calplus.cmo.utils.UtilsJson;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Updates the A06_ClanWarAttackDetails table with clan war attack information from the Clash of Clans API.
 * Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/currentwar
 * DB format: Stores detailed attack information for each player in regular clan wars (not CWL)
 * DB Ordering: First to last war > Home clan player map position 1-n > Opponent clan player map position 1-n
 */
public class A06_ClanWarAttackDetails {
    
    private static final String CURRENT_WAR_API = "https://api.clashofclans.com/v1/clans/";
    private static final String TABLE_NAME = "A06_ClanWarAttackDetails";
    
    private String apiKey;
    private String dbName;
    private String clanTag;
    private DiscordLog discordLogger;
    
    public A06_ClanWarAttackDetails(String dbName) {
        this.dbName = dbName;
        this.clanTag = dbName.replace(".db", "");
        this.discordLogger = new DiscordLog();
        
        // Setup error interception to log uncaught exceptions
        UtilsErrorInterceptor.setupErrorInterception(this.discordLogger);
        
        this.apiKey = UtilsConfig.loadApiKey();
    }
    
    /**
     * Fetches current war data from the API
     * @return JsonObject containing war data, or null if not in war
     */
    private JsonObject fetchCurrentWar() throws IOException, InterruptedException {
        String url = CURRENT_WAR_API + "%23" + clanTag + "/currentwar";
        
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + apiKey)
                .header("Accept", "application/json")
                .GET()
                .build();
        
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() == 403) {
            String errMsg = "API request failed: Private war log (403 Forbidden) for #" + clanTag;
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            return null;
        }
        
        if (response.statusCode() != 200) {
            String errMsg = "Current war API request failed with status code: " + response.statusCode() + " for #" + clanTag;
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new IOException(errMsg);
        }
        
        String successMsg = "API fetch successful (200 OK) for current war: #" + clanTag;
        System.out.println(successMsg);
        discordLogger.logSuccess(successMsg);
        
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }
    
    /**
     * Gets the last war's cwSeason and endTime from the database
     * @return WarInfo object with last war data, or null if table is empty
     */
    private WarInfo getLastWarInfo() throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT cwSeason, dateLogged FROM " + TABLE_NAME + " ORDER BY id DESC LIMIT 1";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            if (rs.next()) {
                WarInfo info = new WarInfo();
                info.cwSeason = rs.getString("cwSeason");
                info.dateLogged = rs.getString("dateLogged");
                return info;
            }
        }
        
        return null;
    }
    
    /**
     * Checks if a war with the given cwSeason exists in the database
     * @param cwSeason The cwSeason to check for
     * @return true if war exists, false otherwise
     */
    private boolean warExists(String cwSeason) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT COUNT(*) as count FROM " + TABLE_NAME + " WHERE cwSeason = ?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, cwSeason);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("count") > 0;
                }
            }
        }
        
        return false;
    }
    
    /**
     * Gets all unique cwSeasons from wars that don't have a "state" field set to "ended"
     * Note: Since A06 table doesn't have a state column, we need to check against A05
     * @return List of cwSeasons that need to be marked as ended
     */
    private List<String> getUnendedWars() throws SQLException {
        List<String> unendedWars = new ArrayList<>();
        String url = UtilsDatabase.getConnectionUrl(dbName);
        
        // Get all distinct cwSeasons from A06 that don't have a corresponding "ended" state in A05
        String sql = "SELECT DISTINCT a6.cwSeason FROM " + TABLE_NAME + " a6 " +
                     "WHERE NOT EXISTS (" +
                     "  SELECT 1 FROM A05_ClanWarLog a5 " +
                     "  WHERE a5.cwSeason = a6.cwSeason AND a5.state = 'ended'" +
                     ")";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            while (rs.next()) {
                unendedWars.add(rs.getString("cwSeason"));
            }
        }
        
        return unendedWars;
    }
    
    /**
     * Parses endTime string to epoch seconds for comparison
     * @param endTime Format: "20260117T033938.000Z"
     * @return Epoch seconds
     */
    private long parseEndTimeToEpoch(String endTime) {
        // Format: 20260117T033938.000Z -> 2026-01-17T03:39:38.000Z
        String formatted = endTime.substring(0, 4) + "-" + 
                          endTime.substring(4, 6) + "-" + 
                          endTime.substring(6, 11) + ":" + 
                          endTime.substring(11, 13) + ":" + 
                          endTime.substring(13, 15) + 
                          endTime.substring(15);
        return LocalDateTime.parse(formatted.replace("Z", ""), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .toEpochSecond(ZoneOffset.UTC);
    }
    
    /**
     * Looks up cwSeason from A05_ClanWarLog with time tolerance
     * Searches for wars with endTime within ±60 seconds of the target
     * @param endTime The endTime from the current war
     * @return The cwSeason value from A05_ClanWarLog, or null if not found
     */
    private String lookupCwSeasonFromA05(String endTime) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        
        // First try exact match
        String sql = "SELECT cwSeason FROM A05_ClanWarLog WHERE endTime = ?";
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, endTime);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("cwSeason");
                }
            }
        }
        
        // If exact match fails, try with time tolerance (±60 seconds)
        long targetEpoch = parseEndTimeToEpoch(endTime);
        long minEpoch = targetEpoch - 60;
        long maxEpoch = targetEpoch + 60;
        
        sql = "SELECT cwSeason, endTime FROM A05_ClanWarLog";
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            while (rs.next()) {
                String dbEndTime = rs.getString("endTime");
                long dbEpoch = parseEndTimeToEpoch(dbEndTime);
                
                if (dbEpoch >= minEpoch && dbEpoch <= maxEpoch) {
                    String cwSeason = rs.getString("cwSeason");
                    String infoMsg = "Matched war with time tolerance: " + endTime + " ~ " + dbEndTime + 
                                    " (diff: " + Math.abs(targetEpoch - dbEpoch) + "s)";
                    System.out.println(infoMsg);
                    discordLogger.logInfo(infoMsg);
                    return cwSeason;
                }
            }
        }
        
        return null; // Not found
    }
    
    /**
     * Calculates cwSeason independently for ongoing wars not yet in A05
     * This is used when the current war hasn't ended yet and isn't in the war log
     * @param endTime The endTime from the war data
     * @return Calculated cwSeason value
     */
    private String calculateCwSeasonForOngoingWar(String endTime) throws IOException, SQLException {
        // Read config file for season and cwSeasonId
        String configDir = "config/clans";
        String configFileName = clanTag + ".json";
        String configFilePath = configDir + "/" + configFileName;
        
        String season = null;
        Integer cwSeasonId = null;
        
        if (Files.exists(Paths.get(configFilePath))) {
            String content = Files.readString(Paths.get(configFilePath));
            JsonObject config = JsonParser.parseString(content).getAsJsonObject();
            
            if (config.has("season") && !config.get("season").isJsonNull()) {
                season = config.get("season").getAsString();
            }
            if (config.has("cwSeasonId") && !config.get("cwSeasonId").isJsonNull()) {
                cwSeasonId = config.get("cwSeasonId").getAsInt();
            }
        }
        
        // Derive season from endTime if not in config
        if (season == null) {
            String yearMonth = endTime.substring(0, 6);
            season = yearMonth.substring(0, 4) + "-" + yearMonth.substring(4, 6);
        }
        
        // Check last war in A06 database to determine next ID
        WarInfo lastWar = getLastWarInfo();
        
        if (lastWar != null && lastWar.cwSeason != null) {
            String[] parts = lastWar.cwSeason.split("-");
            if (parts.length == 3) {
                String lastSeason = parts[0] + "-" + parts[1];
                int lastId = Integer.parseInt(parts[2]);
                
                if (lastSeason.equals(season)) {
                    // Same season, increment ID
                    return season + "-" + (lastId + 1);
                } else {
                    // New season, start from 1
                    return season + "-1";
                }
            }
        }
        
        // No previous wars, use config cwSeasonId or start from 1
        int startId = (cwSeasonId != null) ? cwSeasonId : 1;
        return season + "-" + startId;
    }
    
    /**
     * Gets cwSeason for the current war
     * First tries to lookup from A05 (for ended wars), then calculates for ongoing wars
     * @param endTime The endTime from the war data
     * @return The cwSeason value
     */
    private String getCwSeasonFromA05(String endTime) throws SQLException, IOException {
        // Try to lookup from A05 first (with time tolerance)
        String cwSeason = lookupCwSeasonFromA05(endTime);
        
        if (cwSeason != null) {
            return cwSeason;
        }
        
        // Not found in A05 - this is an ongoing war (preparation/inWar)
        // Calculate cwSeason independently
        String infoMsg = "War with endTime " + endTime + " not found in A05_ClanWarLog. " +
                        "This is likely an ongoing war. Calculating cwSeason independently...";
        System.out.println(infoMsg);
        discordLogger.logInfo(infoMsg);
        
        return calculateCwSeasonForOngoingWar(endTime);
    }
    
    /**
     * Data container for war information
     */
    private static class WarInfo {
        String cwSeason;
        String dateLogged;
    }
    
    /**
     * Data container for player attack data
     */
    private static class PlayerAttackData {
        // Player details
        String attackerTag;
        String attackerName;
        String attackerClanTag;
        Integer attackerThLevel;
        Integer attackerMapPosition;
        
        // Attack 1 details
        String defender1Tag;
        String defender1Name;
        Integer defender1ThLevel;
        Integer defender1MapPosition;
        Integer attack1Stars;
        Integer attack1DestructionPercentage;
        Integer attack1Order;
        Integer attack1Duration;
        Double attack1Score;
        Double attack1ThModifier;
        
        // Attack 2 details
        String defender2Tag;
        String defender2Name;
        Integer defender2ThLevel;
        Integer defender2MapPosition;
        Integer attack2Stars;
        Integer attack2DestructionPercentage;
        Integer attack2Order;
        Integer attack2Duration;
        Double attack2Score;
        Double attack2ThModifier;
        
        // Best defense details
        String defenseAttackerTag;
        Integer defenseStars;
        Integer defenseDestructionPercentage;
        Integer defenseOrder;
        Integer defenseDuration;
        
        // Scores
        Double attacksUsed;
        Double totalWarScore;
    }
    
    /**
     * Parses war data and extracts player attack details for both clans
     * @param warData The war JSON data from API
     * @param cwSeason The calculated cwSeason for this war
     * @return List of PlayerAttackData ordered by clan then map position
     */
    private List<PlayerAttackData> parseWarData(JsonObject warData, String cwSeason) {
        List<PlayerAttackData> allPlayers = new ArrayList<>();
        
        // Get clan and opponent data
        JsonObject clanData = warData.getAsJsonObject("clan");
        JsonObject opponentData = warData.getAsJsonObject("opponent");
        
        String clanTag = clanData.get("tag").getAsString();
        String opponentTag = opponentData.get("tag").getAsString();
        
        // Parse home clan members (sorted by map position)
        JsonArray clanMembers = clanData.getAsJsonArray("members");
        List<PlayerAttackData> clanPlayers = parseClanMembers(clanMembers, clanTag, opponentData);
        allPlayers.addAll(clanPlayers);
        
        // Parse opponent clan members (sorted by map position)
        JsonArray opponentMembers = opponentData.getAsJsonArray("members");
        List<PlayerAttackData> opponentPlayers = parseClanMembers(opponentMembers, opponentTag, clanData);
        allPlayers.addAll(opponentPlayers);
        
        return allPlayers;
    }
    
    /**
     * Parses members from a clan and creates PlayerAttackData for each
     * @param members JsonArray of member data
     * @param attackerClanTag The clan tag these members belong to
     * @param opposingClanData The opposing clan's data (for defender lookups)
     * @return List of PlayerAttackData sorted by map position
     */
    private List<PlayerAttackData> parseClanMembers(JsonArray members, String attackerClanTag, JsonObject opposingClanData) {
        List<PlayerAttackData> players = new ArrayList<>();
        
        // Create a map of opponent players for quick lookup by tag
        Map<String, JsonObject> opponentMap = new HashMap<>();
        JsonArray opponentMembers = opposingClanData.getAsJsonArray("members");
        for (JsonElement elem : opponentMembers) {
            JsonObject member = elem.getAsJsonObject();
            String tag = member.get("tag").getAsString();
            opponentMap.put(tag, member);
        }
        
        // Sort members by map position
        List<JsonObject> sortedMembers = new ArrayList<>();
        for (JsonElement elem : members) {
            sortedMembers.add(elem.getAsJsonObject());
        }
        sortedMembers.sort((a, b) -> {
            int posA = a.get("mapPosition").getAsInt();
            int posB = b.get("mapPosition").getAsInt();
            return Integer.compare(posA, posB);
        });
        
        // Process each member
        for (JsonObject member : sortedMembers) {
            PlayerAttackData player = new PlayerAttackData();
            
            // Set player details
            player.attackerTag = member.get("tag").getAsString();
            player.attackerName = member.get("name").getAsString();
            player.attackerClanTag = attackerClanTag;
            player.attackerThLevel = member.get("townhallLevel").getAsInt();
            player.attackerMapPosition = member.get("mapPosition").getAsInt();
            
            // Process attacks (if any)
            if (member.has("attacks") && !member.get("attacks").isJsonNull()) {
                JsonArray attacks = member.getAsJsonArray("attacks");
                
                // Attack 1
                if (attacks.size() >= 1) {
                    JsonObject attack1 = attacks.get(0).getAsJsonObject();
                    String defenderTag = attack1.get("defenderTag").getAsString();
                    JsonObject defender = opponentMap.get(defenderTag);
                    
                    player.defender1Tag = defenderTag;
                    player.attack1Stars = attack1.get("stars").getAsInt();
                    player.attack1DestructionPercentage = attack1.get("destructionPercentage").getAsInt();
                    player.attack1Order = attack1.get("order").getAsInt();
                    player.attack1Duration = attack1.get("duration").getAsInt();
                    
                    if (defender != null) {
                        player.defender1Name = defender.get("name").getAsString();
                        player.defender1ThLevel = defender.get("townhallLevel").getAsInt();
                        player.defender1MapPosition = defender.get("mapPosition").getAsInt();
                    }
                }
                
                // Attack 2
                if (attacks.size() >= 2) {
                    JsonObject attack2 = attacks.get(1).getAsJsonObject();
                    String defenderTag = attack2.get("defenderTag").getAsString();
                    JsonObject defender = opponentMap.get(defenderTag);
                    
                    player.defender2Tag = defenderTag;
                    player.attack2Stars = attack2.get("stars").getAsInt();
                    player.attack2DestructionPercentage = attack2.get("destructionPercentage").getAsInt();
                    player.attack2Order = attack2.get("order").getAsInt();
                    player.attack2Duration = attack2.get("duration").getAsInt();
                    
                    if (defender != null) {
                        player.defender2Name = defender.get("name").getAsString();
                        player.defender2ThLevel = defender.get("townhallLevel").getAsInt();
                        player.defender2MapPosition = defender.get("mapPosition").getAsInt();
                    }
                }
            }
            
            // Process best defense (if any opponent attacks on this player)
            if (member.has("bestOpponentAttack") && !member.get("bestOpponentAttack").isJsonNull()) {
                JsonObject bestDefense = member.getAsJsonObject("bestOpponentAttack");
                player.defenseAttackerTag = bestDefense.get("attackerTag").getAsString();
                player.defenseStars = bestDefense.get("stars").getAsInt();
                player.defenseDestructionPercentage = bestDefense.get("destructionPercentage").getAsInt();
                player.defenseOrder = bestDefense.get("order").getAsInt();
                player.defenseDuration = bestDefense.get("duration").getAsInt();
            }
            
            players.add(player);
        }
        
        return players;
    }
    
    /**
     * Calculates attack scores for all players using CalcWarQuality methods
     * @param players List of PlayerAttackData to calculate scores for
     */
    private void calculateScores(List<PlayerAttackData> players) {
        for (PlayerAttackData player : players) {
            double totalScore = 0.0;
            int attacksUsed = 0;
            
            // Calculate Attack 1 scores
            if (player.attack1Stars != null && player.attack1DestructionPercentage != null) {
                attacksUsed++;
                
                // Calculate stars/percentage quality score
                double starsPercentQuality = CalcWarQuality.calculateStarsPercentageQuality(
                    player.attack1Stars,
                    player.attack1DestructionPercentage
                );
                player.attack1Score = starsPercentQuality;
                
                // Calculate TH modifier
                if (player.attackerThLevel != null && player.defender1ThLevel != null) {
                    double thModifier = CalcWarQuality.calculateThModifier(
                        player.attackerThLevel,
                        player.defender1ThLevel
                    );
                    player.attack1ThModifier = thModifier;
                    
                    // Calculate war score for this attack (without attacksUsed modifier yet)
                    double attackScore = starsPercentQuality * thModifier;
                    totalScore += attackScore;
                }
            }
            
            // Calculate Attack 2 scores
            if (player.attack2Stars != null && player.attack2DestructionPercentage != null) {
                attacksUsed++;
                
                // Calculate stars/percentage quality score
                double starsPercentQuality = CalcWarQuality.calculateStarsPercentageQuality(
                    player.attack2Stars,
                    player.attack2DestructionPercentage
                );
                player.attack2Score = starsPercentQuality;
                
                // Calculate TH modifier
                if (player.attackerThLevel != null && player.defender2ThLevel != null) {
                    double thModifier = CalcWarQuality.calculateThModifier(
                        player.attackerThLevel,
                        player.defender2ThLevel
                    );
                    player.attack2ThModifier = thModifier;
                    
                    // Calculate war score for this attack (without attacksUsed modifier yet)
                    double attackScore = starsPercentQuality * thModifier;
                    totalScore += attackScore;
                }
            }
            
            // Calculate attacks used modifier (based on number of attacks: 0, 1, or 2)
            double attacksUsedModifier = CalcWarQuality.calculateAttacksUsedModifier(attacksUsed);
            player.attacksUsed = (double) attacksUsed;
            
            // Calculate total war score: sum of attack scores * attacksUsed modifier
            player.totalWarScore = totalScore * attacksUsedModifier;
        }
    }
    
    /**
     * Inserts a player's attack data into the database
     * @param player The PlayerAttackData to insert
     * @param cwSeason The cwSeason for this war
     * @param currentDateTime The current date/time for dateLogged field
     */
    private void insertPlayerData(PlayerAttackData player, String cwSeason, String currentDateTime) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "INSERT INTO " + TABLE_NAME + " (" +
                     "dateLogged, cwSeason, " +
                     "attackerTag, attackerName, attackerClanTag, attackerThLevel, attackerMapPosition, " +
                     "defender1Tag, defender1Name, defender1ThLevel, defender1MapPosition, " +
                     "attack1Stars, attack1DestructionPercentage, attack1Order, attack1Duration, " +
                     "attack1Score, attack1ThModifier, " +
                     "defender2Tag, defender2Name, defender2ThLevel, defender2MapPosition, " +
                     "attack2Stars, attack2DestructionPercentage, attack2Order, attack2Duration, " +
                     "attack2Score, attack2ThModifier, " +
                     "defenseAttackerTag, defenseStars, defenseDestructionPercentage, defenseOrder, defenseDuration, " +
                     "attacksUsed, totalWarScore" +
                     ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, currentDateTime);
            pstmt.setString(2, cwSeason);
            
            // Player details
            pstmt.setString(3, player.attackerTag);
            pstmt.setString(4, player.attackerName);
            pstmt.setString(5, player.attackerClanTag);
            pstmt.setObject(6, player.attackerThLevel);
            pstmt.setObject(7, player.attackerMapPosition);
            
            // Attack 1
            pstmt.setString(8, player.defender1Tag);
            pstmt.setString(9, player.defender1Name);
            pstmt.setObject(10, player.defender1ThLevel);
            pstmt.setObject(11, player.defender1MapPosition);
            pstmt.setObject(12, player.attack1Stars);
            pstmt.setObject(13, player.attack1DestructionPercentage);
            pstmt.setObject(14, player.attack1Order);
            pstmt.setObject(15, player.attack1Duration);
            pstmt.setObject(16, player.attack1Score);
            pstmt.setObject(17, player.attack1ThModifier);
            
            // Attack 2
            pstmt.setString(18, player.defender2Tag);
            pstmt.setString(19, player.defender2Name);
            pstmt.setObject(20, player.defender2ThLevel);
            pstmt.setObject(21, player.defender2MapPosition);
            pstmt.setObject(22, player.attack2Stars);
            pstmt.setObject(23, player.attack2DestructionPercentage);
            pstmt.setObject(24, player.attack2Order);
            pstmt.setObject(25, player.attack2Duration);
            pstmt.setObject(26, player.attack2Score);
            pstmt.setObject(27, player.attack2ThModifier);
            
            // Best defense
            pstmt.setString(28, player.defenseAttackerTag);
            pstmt.setObject(29, player.defenseStars);
            pstmt.setObject(30, player.defenseDestructionPercentage);
            pstmt.setObject(31, player.defenseOrder);
            pstmt.setObject(32, player.defenseDuration);
            
            // Scores
            pstmt.setObject(33, player.attacksUsed);
            pstmt.setObject(34, player.totalWarScore);
            
            pstmt.executeUpdate();
        }
    }
    
    /**
     * Inserts all player data for a war into the database
     * @param players List of PlayerAttackData (already sorted)
     * @param cwSeason The cwSeason for this war
     * @param currentDateTime The current date/time for dateLogged field
     */
    private void insertWarData(List<PlayerAttackData> players, String cwSeason, String currentDateTime) throws SQLException {
        String infoMsg = "Inserting " + players.size() + " player records for cwSeason: " + cwSeason;
        System.out.println(infoMsg);
        discordLogger.logInfo(infoMsg);
        
        for (PlayerAttackData player : players) {
            insertPlayerData(player, cwSeason, currentDateTime);
        }
        
        String successMsg = "Successfully inserted " + players.size() + " player records";
        System.out.println(successMsg);
        discordLogger.logInfo(successMsg);
    }
    
    /**
     * Updates existing player data in the database
     * @param player The PlayerAttackData to update
     * @param cwSeason The cwSeason for this war
     * @param currentDateTime The current date/time for dateLogged field
     */
    private void updatePlayerData(PlayerAttackData player, String cwSeason, String currentDateTime) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "UPDATE " + TABLE_NAME + " SET " +
                     "dateLogged = ?, " +
                     "attackerName = ?, attackerClanTag = ?, attackerThLevel = ?, attackerMapPosition = ?, " +
                     "defender1Tag = ?, defender1Name = ?, defender1ThLevel = ?, defender1MapPosition = ?, " +
                     "attack1Stars = ?, attack1DestructionPercentage = ?, attack1Order = ?, attack1Duration = ?, " +
                     "attack1Score = ?, attack1ThModifier = ?, " +
                     "defender2Tag = ?, defender2Name = ?, defender2ThLevel = ?, defender2MapPosition = ?, " +
                     "attack2Stars = ?, attack2DestructionPercentage = ?, attack2Order = ?, attack2Duration = ?, " +
                     "attack2Score = ?, attack2ThModifier = ?, " +
                     "defenseAttackerTag = ?, defenseStars = ?, defenseDestructionPercentage = ?, defenseOrder = ?, defenseDuration = ?, " +
                     "attacksUsed = ?, totalWarScore = ? " +
                     "WHERE cwSeason = ? AND attackerTag = ?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, currentDateTime);
            
            // Player details
            pstmt.setString(2, player.attackerName);
            pstmt.setString(3, player.attackerClanTag);
            pstmt.setObject(4, player.attackerThLevel);
            pstmt.setObject(5, player.attackerMapPosition);
            
            // Attack 1
            pstmt.setString(6, player.defender1Tag);
            pstmt.setString(7, player.defender1Name);
            pstmt.setObject(8, player.defender1ThLevel);
            pstmt.setObject(9, player.defender1MapPosition);
            pstmt.setObject(10, player.attack1Stars);
            pstmt.setObject(11, player.attack1DestructionPercentage);
            pstmt.setObject(12, player.attack1Order);
            pstmt.setObject(13, player.attack1Duration);
            pstmt.setObject(14, player.attack1Score);
            pstmt.setObject(15, player.attack1ThModifier);
            
            // Attack 2
            pstmt.setString(16, player.defender2Tag);
            pstmt.setString(17, player.defender2Name);
            pstmt.setObject(18, player.defender2ThLevel);
            pstmt.setObject(19, player.defender2MapPosition);
            pstmt.setObject(20, player.attack2Stars);
            pstmt.setObject(21, player.attack2DestructionPercentage);
            pstmt.setObject(22, player.attack2Order);
            pstmt.setObject(23, player.attack2Duration);
            pstmt.setObject(24, player.attack2Score);
            pstmt.setObject(25, player.attack2ThModifier);
            
            // Best defense
            pstmt.setString(26, player.defenseAttackerTag);
            pstmt.setObject(27, player.defenseStars);
            pstmt.setObject(28, player.defenseDestructionPercentage);
            pstmt.setObject(29, player.defenseOrder);
            pstmt.setObject(30, player.defenseDuration);
            
            // Scores
            pstmt.setObject(31, player.attacksUsed);
            pstmt.setObject(32, player.totalWarScore);
            
            // WHERE clause
            pstmt.setString(33, cwSeason);
            pstmt.setString(34, player.attackerTag);
            
            pstmt.executeUpdate();
        }
    }
    
    /**
     * Updates all player data for an existing war in the database
     * @param players List of PlayerAttackData (already sorted)
     * @param cwSeason The cwSeason for this war
     * @param currentDateTime The current date/time for dateLogged field
     */
    private void updateWarData(List<PlayerAttackData> players, String cwSeason, String currentDateTime) throws SQLException {
        String infoMsg = "Updating " + players.size() + " player records for cwSeason: " + cwSeason;
        System.out.println(infoMsg);
        discordLogger.logInfo(infoMsg);
        
        for (PlayerAttackData player : players) {
            updatePlayerData(player, cwSeason, currentDateTime);
        }
        
        String successMsg = "Successfully updated " + players.size() + " player records";
        System.out.println(successMsg);
        discordLogger.logInfo(successMsg);
    }
    
    /**
     * Main update method
     */
    public void updateDatabase() throws SQLException, IOException, InterruptedException {
        String startMsg = "Starting clan war attack details update for #" + clanTag;
        System.out.println(startMsg);
        discordLogger.logInfo(startMsg);
        
        // Get current date/time in UTC
        String currentDateTime = LocalDateTime.now(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        
        // Fetch current war data from API
        JsonObject warData = fetchCurrentWar();
        
        if (warData == null) {
            String errorMsg = "Failed to fetch current war data (may be private war log)";
            System.err.println(errorMsg);
            discordLogger.logWarning(errorMsg);
            return;
        }
        
        // Check war state
        String state = warData.get("state").getAsString();
        
        // Check if this is a CWL war (A03/A04 handle CWL wars, not A06)
        // During CWL, the currentwar endpoint still works but we should skip it
        // CWL wars are identified by the warTag field and processed separately
        if (warData.has("warTag") && !warData.get("warTag").isJsonNull()) {
            String warTag = warData.get("warTag").getAsString();
            if (warTag != null && warTag.startsWith("#")) {
                String infoMsg = "Skipping CWL war (warTag: " + warTag + "). CWL wars are handled by A03_CWLWarDetails.";
                System.out.println(infoMsg);
                discordLogger.logInfo(infoMsg);
                
                String successMsg = "Clan war attack details check completed (CWL war skipped)";
                System.out.println(successMsg);
                discordLogger.logSuccess(successMsg);
                return;
            }
        }
        
        if ("notInWar".equals(state)) {
            String infoMsg = "Clan is not currently in war. No attack details to update.";
            System.out.println(infoMsg);
            discordLogger.logInfo(infoMsg);
            
            // Note: We don't need to mark wars as "ended" here because A05 handles the state
            // A06 only runs when A05 runs, and A05 manages the war state in A05_ClanWarLog table
            
            String successMsg = "Clan war attack details check completed (not in war)";
            System.out.println(successMsg);
            discordLogger.logSuccess(successMsg);
            return;
        }
        
        // Get endTime from war data to determine cwSeason
        String endTime = warData.get("endTime").getAsString();
        
        // Get cwSeason from A05_ClanWarLog (A05 should have already processed this war)
        String cwSeason = getCwSeasonFromA05(endTime);
        
        String infoMsg = "Processing war with state: " + state + ", cwSeason: " + cwSeason;
        System.out.println(infoMsg);
        discordLogger.logInfo(infoMsg);
        
        // Parse war data for both clans
        List<PlayerAttackData> players = parseWarData(warData, cwSeason);
        
        // Calculate scores for all players
        calculateScores(players);
        
        // Check if this war already exists in database
        boolean exists = warExists(cwSeason);
        
        if (exists) {
            // War exists - update with latest data (handles preparation -> inWar -> ended progression)
            String updateMsg = "War already exists in database. Updating with latest data...";
            System.out.println(updateMsg);
            discordLogger.logInfo(updateMsg);
            
            updateWarData(players, cwSeason, currentDateTime);
        } else {
            // New war - insert all data
            String insertMsg = "New war detected. Inserting war data...";
            System.out.println(insertMsg);
            discordLogger.logInfo(insertMsg);
            
            insertWarData(players, cwSeason, currentDateTime);
        }
        
        String successMsg = "Clan war attack details update completed for #" + clanTag;
        System.out.println(successMsg);
        discordLogger.logSuccess(successMsg);
    }
    
    /**
     * Main method for testing or standalone execution
     */
    public static void main(String[] args) {
        String dbName = "20CG8UURL.db";
        
        if (args.length > 0) {
            dbName = args[0];
        }
        
        try {
            A06_ClanWarAttackDetails updater = new A06_ClanWarAttackDetails(dbName);
            updater.updateDatabase();
        } catch (Exception e) {
            System.err.println("Failed to update clan war attack details: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
