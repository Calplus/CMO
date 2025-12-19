package databaseupdater;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import discordbot.logs.DiscordLog;
import utils.UtilsDatabase;
import utils.UtilsJson;
/**
 * Updates the A04_CWLAttackDetails table with attack information from CWL wars.
 * This class is called by A03_CWLWarDetails after fetching war data.
 * DB format: Each row represents one attack in a CWL war
 * DB Ordering: War tag > Attack order
 */
public class A04_CWLAttackDetails {
    
    private static final String TABLE_NAME = "A04_CWLAttackDetails";
    
    private String dbName;
    private DiscordLog discordLogger;
    
    public A04_CWLAttackDetails(String dbName, DiscordLog discordLogger) {
        this.dbName = dbName;
        this.discordLogger = discordLogger;
    }
    
    /**
     * Gets existing attacks from the database for a given warTag
     * Returns a map keyed by attackerTag-defenderTag-attackOrder
     */
    private Map<String, AttackRecord> getExistingAttacks(String warTag) throws SQLException {
        Map<String, AttackRecord> attacks = new HashMap<>();
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT * FROM " + TABLE_NAME + " WHERE warTag = ?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, warTag);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    AttackRecord record = new AttackRecord();
                    record.id = rs.getInt("id");
                    record.season = rs.getString("season");
                    record.warTag = rs.getString("warTag");
                    record.clanTag = rs.getString("clanTag");
                    record.opponentTag = rs.getString("opponentTag");
                    record.attackerTag = rs.getString("attackerTag");
                    record.attackerName = rs.getString("attackerName");
                    record.attackerThLevel = (Integer) rs.getObject("attackerThLevel");
                    record.attackerMapPosition = (Integer) rs.getObject("attackerMapPosition");
                    record.defenderTag = rs.getString("defenderTag");
                    record.defenderName = rs.getString("defenderName");
                    record.defenderThLevel = (Integer) rs.getObject("defenderThLevel");
                    record.defenderMapPosition = (Integer) rs.getObject("defenderMapPosition");
                    record.stars = (Integer) rs.getObject("stars");
                    record.destructionPercentage = (Integer) rs.getObject("destructionPercentage");
                    record.attackOrder = (Integer) rs.getObject("attackOrder");
                    record.duration = (Integer) rs.getObject("duration");
                    record.opponentAttacks = (Integer) rs.getObject("opponentAttacks");
                    record.bestDefenseAttackerTag = rs.getString("BestDefenseAttackerTag");
                    record.bestDefenseStars = (Integer) rs.getObject("BestDefenseStars");
                    record.bestDefensePercentage = (Integer) rs.getObject("BestDefensePercentage");
                    record.bestDefenseOrder = (Integer) rs.getObject("BestDefenseOrder");
                    record.bestDefenseDuration = (Integer) rs.getObject("BestDefenseDuration");
                    
                    // Use same key logic - players without attacks get special key
                    String key;
                    if (record.attackOrder == null || record.attackOrder == 0) {
                        key = record.attackerTag + "-NO_ATTACK";
                    } else {
                        key = record.attackerTag + "-" + record.defenderTag + "-" + record.attackOrder;
                    }
                    
                    attacks.put(key, record);
                }
            }
        }
        
        return attacks;
    }
    
    /**
     * Checks if attack data has changed
     */
    private boolean hasAttackDataChanged(AttackData newData, AttackRecord existingRecord) {
        return !Objects.equals(newData.attackerName, existingRecord.attackerName) ||
               !Objects.equals(newData.attackerThLevel, existingRecord.attackerThLevel) ||
               !Objects.equals(newData.attackerMapPosition, existingRecord.attackerMapPosition) ||
               !Objects.equals(newData.defenderName, existingRecord.defenderName) ||
               !Objects.equals(newData.defenderThLevel, existingRecord.defenderThLevel) ||
               !Objects.equals(newData.defenderMapPosition, existingRecord.defenderMapPosition) ||
               !Objects.equals(newData.stars, existingRecord.stars) ||
               !Objects.equals(newData.destructionPercentage, existingRecord.destructionPercentage) ||
               !Objects.equals(newData.duration, existingRecord.duration) ||
               !Objects.equals(newData.opponentAttacks, existingRecord.opponentAttacks) ||
               !Objects.equals(newData.bestDefenseAttackerTag, existingRecord.bestDefenseAttackerTag) ||
               !Objects.equals(newData.bestDefenseStars, existingRecord.bestDefenseStars) ||
               !Objects.equals(newData.bestDefensePercentage, existingRecord.bestDefensePercentage) ||
               !Objects.equals(newData.bestDefenseOrder, existingRecord.bestDefenseOrder) ||
               !Objects.equals(newData.bestDefenseDuration, existingRecord.bestDefenseDuration);
    }
    
    /**
     * Inserts or updates attack details in the database
     */
    private void upsertAttackDetails(AttackData attack, String currentDateTime, 
                                     boolean dataChanged, AttackRecord existingRecord) throws SQLException {
        
        String url = UtilsDatabase.getConnectionUrl(dbName);
        
        if (existingRecord == null) {
            // Insert new attack
            String sql = "INSERT INTO " + TABLE_NAME + " (" +
                         "dateLogged, season, warTag, clanTag, opponentTag, " +
                         "attackerTag, attackerName, attackerThLevel, attackerMapPosition, " +
                         "defenderTag, defenderName, defenderThLevel, defenderMapPosition, " +
                         "stars, destructionPercentage, attackOrder, duration, " +
                         "opponentAttacks, BestDefenseAttackerTag, BestDefenseStars, BestDefensePercentage, " +
                         "BestDefenseOrder, BestDefenseDuration" +
                         ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            try (Connection conn = DriverManager.getConnection(url);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                
                pstmt.setString(1, currentDateTime);
                pstmt.setString(2, attack.season);
                pstmt.setString(3, attack.warTag);
                pstmt.setString(4, attack.clanTag);
                pstmt.setString(5, attack.opponentTag);
                pstmt.setString(6, attack.attackerTag);
                pstmt.setString(7, attack.attackerName);
                UtilsDatabase.setIntOrNull(pstmt, 8, attack.attackerThLevel);
                UtilsDatabase.setIntOrNull(pstmt, 9, attack.attackerMapPosition);
                pstmt.setString(10, attack.defenderTag);
                pstmt.setString(11, attack.defenderName);
                UtilsDatabase.setIntOrNull(pstmt, 12, attack.defenderThLevel);
                UtilsDatabase.setIntOrNull(pstmt, 13, attack.defenderMapPosition);
                UtilsDatabase.setIntOrNull(pstmt, 14, attack.stars);
                UtilsDatabase.setIntOrNull(pstmt, 15, attack.destructionPercentage);
                UtilsDatabase.setIntOrNull(pstmt, 16, attack.attackOrder);
                UtilsDatabase.setIntOrNull(pstmt, 17, attack.duration);
                UtilsDatabase.setIntOrNull(pstmt, 18, attack.opponentAttacks);
                pstmt.setString(19, attack.bestDefenseAttackerTag);
                UtilsDatabase.setIntOrNull(pstmt, 20, attack.bestDefenseStars);
                UtilsDatabase.setIntOrNull(pstmt, 21, attack.bestDefensePercentage);
                UtilsDatabase.setIntOrNull(pstmt, 22, attack.bestDefenseOrder);
                UtilsDatabase.setIntOrNull(pstmt, 23, attack.bestDefenseDuration);
                
                pstmt.executeUpdate();
            }
        } else if (dataChanged) {
            // Update existing attack
            String sql = "UPDATE " + TABLE_NAME + " SET " +
                         "dateLogged = ?, attackerName = ?, attackerThLevel = ?, attackerMapPosition = ?, " +
                         "defenderName = ?, defenderThLevel = ?, defenderMapPosition = ?, " +
                         "stars = ?, destructionPercentage = ?, duration = ?, " +
                         "opponentAttacks = ?, BestDefenseAttackerTag = ?, BestDefenseStars = ?, " +
                         "BestDefensePercentage = ?, BestDefenseOrder = ?, BestDefenseDuration = ? " +
                         "WHERE warTag = ? AND attackerTag = ? AND defenderTag = ? AND attackOrder = ?";
            
            try (Connection conn = DriverManager.getConnection(url);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                
                pstmt.setString(1, currentDateTime);
                pstmt.setString(2, attack.attackerName);
                UtilsDatabase.setIntOrNull(pstmt, 3, attack.attackerThLevel);
                UtilsDatabase.setIntOrNull(pstmt, 4, attack.attackerMapPosition);
                pstmt.setString(5, attack.defenderName);
                UtilsDatabase.setIntOrNull(pstmt, 6, attack.defenderThLevel);
                UtilsDatabase.setIntOrNull(pstmt, 7, attack.defenderMapPosition);
                UtilsDatabase.setIntOrNull(pstmt, 8, attack.stars);
                UtilsDatabase.setIntOrNull(pstmt, 9, attack.destructionPercentage);
                UtilsDatabase.setIntOrNull(pstmt, 10, attack.duration);
                UtilsDatabase.setIntOrNull(pstmt, 11, attack.opponentAttacks);
                pstmt.setString(12, attack.bestDefenseAttackerTag);
                UtilsDatabase.setIntOrNull(pstmt, 13, attack.bestDefenseStars);
                UtilsDatabase.setIntOrNull(pstmt, 14, attack.bestDefensePercentage);
                UtilsDatabase.setIntOrNull(pstmt, 15, attack.bestDefenseOrder);
                UtilsDatabase.setIntOrNull(pstmt, 16, attack.bestDefenseDuration);
                pstmt.setString(17, attack.warTag);
                pstmt.setString(18, attack.attackerTag);
                pstmt.setString(19, attack.defenderTag);
                UtilsDatabase.setIntOrNull(pstmt, 20, attack.attackOrder);
                
                pstmt.executeUpdate();
            }
        }
    }
    
    /**
     * Process war attacks from war details JSON
     * Called by A03_CWLWarDetails after fetching war data
     */
    public void processWarAttacks(JsonObject warDetails, String season, String warTag, 
                                   String currentDateTime) throws SQLException, IOException {
        
        if (warDetails == null) {
            return;
        }
        
        // Get existing attacks from database
        Map<String, AttackRecord> existingAttacks = getExistingAttacks(warTag);
        
        List<AttackData> allAttacks = new ArrayList<>();
        
        // Process clan's attacks (clan is clanTag, opponent is opponentTag)
        if (warDetails.has("clan") && warDetails.get("clan").isJsonObject()) {
            JsonObject clan = warDetails.getAsJsonObject("clan");
            String clanTag = UtilsJson.getJsonString(clan, "tag");
            
            JsonObject opponent = warDetails.has("opponent") ? warDetails.getAsJsonObject("opponent") : null;
            String opponentTag = opponent != null ? UtilsJson.getJsonString(opponent, "tag") : null;
            
            List<AttackData> clanAttacks = processClanMembers(clan, opponent, season, warTag, 
                                                               clanTag, opponentTag);
            allAttacks.addAll(clanAttacks);
        }
        
        // Process opponent's attacks (opponent is clanTag, clan is opponentTag - swapped!)
        if (warDetails.has("opponent") && warDetails.get("opponent").isJsonObject()) {
            JsonObject opponent = warDetails.getAsJsonObject("opponent");
            String opponentAsClanTag = UtilsJson.getJsonString(opponent, "tag");
            
            JsonObject clan = warDetails.has("clan") ? warDetails.getAsJsonObject("clan") : null;
            String clanAsOpponentTag = clan != null ? UtilsJson.getJsonString(clan, "tag") : null;
            
            List<AttackData> opponentAttacks = processClanMembers(opponent, clan, season, warTag, 
                                                                   opponentAsClanTag, clanAsOpponentTag);
            allAttacks.addAll(opponentAttacks);
        }
        
        // Upsert all attacks
        int inserted = 0;
        int updated = 0;
        int skipped = 0;
        
        for (AttackData attack : allAttacks) {
            // For players without attacks, use a special key with "NO_ATTACK"
            String key;
            if (attack.attackOrder == null) {
                key = attack.attackerTag + "-NO_ATTACK";
            } else {
                key = attack.attackerTag + "-" + attack.defenderTag + "-" + attack.attackOrder;
            }
            
            AttackRecord existingRecord = existingAttacks.get(key);
            
            boolean dataChanged = existingRecord == null || hasAttackDataChanged(attack, existingRecord);
            
            if (existingRecord == null) {
                upsertAttackDetails(attack, currentDateTime, true, null);
                inserted++;
            } else if (dataChanged) {
                upsertAttackDetails(attack, currentDateTime, true, existingRecord);
                updated++;
            } else {
                skipped++;
            }
        }
        
        // Always log summary
        String infoMsg = String.format(
            "Attack details for war %s: Inserted %d, Updated %d, Skipped %d",
            warTag, inserted, updated, skipped
        );
        System.out.println(infoMsg);
        discordLogger.logInfo(infoMsg);
    }
    
    /**
     * Process members from a clan to extract attack data
     */
    private List<AttackData> processClanMembers(JsonObject clan, JsonObject opponent, 
                                                 String season, String warTag, 
                                                 String clanTag, String opponentTag) {
        List<AttackData> attacks = new ArrayList<>();
        
        if (!clan.has("members") || !clan.get("members").isJsonArray()) {
            return attacks;
        }
        
        JsonArray members = clan.getAsJsonArray("members");
        
        // Create a map of opponent members by tag for quick lookup
        Map<String, JsonObject> opponentMembers = new HashMap<>();
        if (opponent != null && opponent.has("members") && opponent.get("members").isJsonArray()) {
            JsonArray oppMembers = opponent.getAsJsonArray("members");
            for (JsonElement elem : oppMembers) {
                JsonObject member = elem.getAsJsonObject();
                String tag = UtilsJson.getJsonString(member, "tag");
                if (tag != null) {
                    opponentMembers.put(tag, member);
                }
            }
        }
        
        for (JsonElement memberElem : members) {
            JsonObject member = memberElem.getAsJsonObject();
            
            // Get attacker info
            String attackerTag = UtilsJson.getJsonString(member, "tag");
            String attackerName = UtilsJson.getJsonString(member, "name");
            Integer attackerThLevel = UtilsJson.getJsonInt(member, "townhallLevel");
            Integer attackerMapPosition = UtilsJson.getJsonInt(member, "mapPosition");
            
            // Get best opponent attack (defense info)
            Integer opponentAttacks = UtilsJson.getJsonInt(member, "opponentAttacks");
            JsonObject bestOpponentAttack = null;
            if (member.has("bestOpponentAttack") && member.get("bestOpponentAttack").isJsonObject()) {
                bestOpponentAttack = member.getAsJsonObject("bestOpponentAttack");
            }
            
            // Process each attack by this member
            if (member.has("attacks") && member.get("attacks").isJsonArray()) {
                JsonArray attacksArray = member.getAsJsonArray("attacks");
                
                // Check if player has no attacks - still add them with null values
                if (attacksArray.size() == 0) {
                    AttackData attack = new AttackData();
                    attack.season = season;
                    attack.warTag = warTag;
                    attack.clanTag = clanTag;
                    attack.opponentTag = opponentTag;
                    
                    // Attacker info
                    attack.attackerTag = attackerTag;
                    attack.attackerName = attackerName;
                    attack.attackerThLevel = attackerThLevel;
                    attack.attackerMapPosition = attackerMapPosition;
                    
                    // All attack-specific fields remain null
                    attack.defenderTag = null;
                    attack.defenderName = null;
                    attack.defenderThLevel = null;
                    attack.defenderMapPosition = null;
                    attack.stars = null;
                    attack.destructionPercentage = null;
                    attack.attackOrder = null;
                    attack.duration = null;
                    
                    // Best defense details
                    attack.opponentAttacks = opponentAttacks;
                    if (bestOpponentAttack != null) {
                        attack.bestDefenseAttackerTag = UtilsJson.getJsonString(bestOpponentAttack, "attackerTag");
                        attack.bestDefenseStars = UtilsJson.getJsonInt(bestOpponentAttack, "stars");
                        attack.bestDefensePercentage = UtilsJson.getJsonInt(bestOpponentAttack, "destructionPercentage");
                        attack.bestDefenseOrder = UtilsJson.getJsonInt(bestOpponentAttack, "order");
                        attack.bestDefenseDuration = UtilsJson.getJsonInt(bestOpponentAttack, "duration");
                    }
                    
                    attacks.add(attack);
                } else {
                    // Player has attacks - process them
                    for (JsonElement attackElem : attacksArray) {
                        JsonObject attackJson = attackElem.getAsJsonObject();
                        
                        AttackData attack = new AttackData();
                        attack.season = season;
                        attack.warTag = warTag;
                        attack.clanTag = clanTag;
                        attack.opponentTag = opponentTag;
                        
                        // Attacker info
                        attack.attackerTag = attackerTag;
                        attack.attackerName = attackerName;
                        attack.attackerThLevel = attackerThLevel;
                        attack.attackerMapPosition = attackerMapPosition;
                        
                        // Defender info
                        String defenderTag = UtilsJson.getJsonString(attackJson, "defenderTag");
                        attack.defenderTag = defenderTag;
                        
                        // Try to find defender details from opponent members
                        if (defenderTag != null && opponentMembers.containsKey(defenderTag)) {
                            JsonObject defenderMember = opponentMembers.get(defenderTag);
                            attack.defenderName = UtilsJson.getJsonString(defenderMember, "name");
                            attack.defenderThLevel = UtilsJson.getJsonInt(defenderMember, "townhallLevel");
                            attack.defenderMapPosition = UtilsJson.getJsonInt(defenderMember, "mapPosition");
                        }
                        
                        // Attack details
                        attack.stars = UtilsJson.getJsonInt(attackJson, "stars");
                        attack.destructionPercentage = UtilsJson.getJsonInt(attackJson, "destructionPercentage");
                        attack.attackOrder = UtilsJson.getJsonInt(attackJson, "order");
                        attack.duration = UtilsJson.getJsonInt(attackJson, "duration");
                        
                        // Best defense details
                        attack.opponentAttacks = opponentAttacks;
                        if (bestOpponentAttack != null) {
                            attack.bestDefenseAttackerTag = UtilsJson.getJsonString(bestOpponentAttack, "attackerTag");
                            attack.bestDefenseStars = UtilsJson.getJsonInt(bestOpponentAttack, "stars");
                            attack.bestDefensePercentage = UtilsJson.getJsonInt(bestOpponentAttack, "destructionPercentage");
                            attack.bestDefenseOrder = UtilsJson.getJsonInt(bestOpponentAttack, "order");
                            attack.bestDefenseDuration = UtilsJson.getJsonInt(bestOpponentAttack, "duration");
                        }
                        
                        attacks.add(attack);
                    }
                }
            } else {
                // Player has no attacks array at all - still add them with null values
                AttackData attack = new AttackData();
                attack.season = season;
                attack.warTag = warTag;
                attack.clanTag = clanTag;
                attack.opponentTag = opponentTag;
                
                // Attacker info
                attack.attackerTag = attackerTag;
                attack.attackerName = attackerName;
                attack.attackerThLevel = attackerThLevel;
                attack.attackerMapPosition = attackerMapPosition;
                
                // All attack-specific fields remain null
                attack.defenderTag = null;
                attack.defenderName = null;
                attack.defenderThLevel = null;
                attack.defenderMapPosition = null;
                attack.stars = null;
                attack.destructionPercentage = null;
                attack.attackOrder = null;
                attack.duration = null;
                
                // Best defense details
                attack.opponentAttacks = opponentAttacks;
                if (bestOpponentAttack != null) {
                    attack.bestDefenseAttackerTag = UtilsJson.getJsonString(bestOpponentAttack, "attackerTag");
                    attack.bestDefenseStars = UtilsJson.getJsonInt(bestOpponentAttack, "stars");
                    attack.bestDefensePercentage = UtilsJson.getJsonInt(bestOpponentAttack, "destructionPercentage");
                    attack.bestDefenseOrder = UtilsJson.getJsonInt(bestOpponentAttack, "order");
                    attack.bestDefenseDuration = UtilsJson.getJsonInt(bestOpponentAttack, "duration");
                }
                
                attacks.add(attack);
            }
        }
        
        return attacks;
    }
    
    /**
     * Data container class for attack information
     */
    private static class AttackData {
        String season;
        String warTag;
        String clanTag;
        String opponentTag;
        
        String attackerTag;
        String attackerName;
        Integer attackerThLevel;
        Integer attackerMapPosition;
        
        String defenderTag;
        String defenderName;
        Integer defenderThLevel;
        Integer defenderMapPosition;
        
        Integer stars;
        Integer destructionPercentage;
        Integer attackOrder;
        Integer duration;
        
        Integer opponentAttacks;
        String bestDefenseAttackerTag;
        Integer bestDefenseStars;
        Integer bestDefensePercentage;
        Integer bestDefenseOrder;
        Integer bestDefenseDuration;
    }
    
    /**
     * Database record class for existing attack data
     */
    private static class AttackRecord {
        int id;
        String season;
        String warTag;
        String clanTag;
        String opponentTag;
        
        String attackerTag;
        String attackerName;
        Integer attackerThLevel;
        Integer attackerMapPosition;
        
        String defenderTag;
        String defenderName;
        Integer defenderThLevel;
        Integer defenderMapPosition;
        
        Integer stars;
        Integer destructionPercentage;
        Integer attackOrder;
        Integer duration;
        
        Integer opponentAttacks;
        String bestDefenseAttackerTag;
        Integer bestDefenseStars;
        Integer bestDefensePercentage;
        Integer bestDefenseOrder;
        Integer bestDefenseDuration;
    }
}
