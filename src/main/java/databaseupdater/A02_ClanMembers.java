package databaseupdater;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
 * Updates the A02_ClanMembers table with member information from the Clash of Clans API.
 * Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/members
 * Secondary endpoint: https://api.clashofclans.com/v1/players/%23{playerTag}
 * DB format: New rows only for new members; old members updated
 * DB Ordering: Date of first join
 */
public class A02_ClanMembers {
    
    private static final String CLAN_MEMBERS_API = "https://api.clashofclans.com/v1/clans/";
    private static final String PLAYER_API = "https://api.clashofclans.com/v1/players/";
    private static final String TABLE_NAME = "A02_ClanMembers";
    
    private String apiKey;
    private String dbName;
    private String clanTag;
    private DiscordLog discordLogger;
    
    public A02_ClanMembers(String dbName) {
        this.dbName = dbName;
        this.clanTag = dbName.replace(".db", "");
        this.discordLogger = new DiscordLog();
        
        // Setup error interception to log uncaught exceptions
        UtilsErrorInterceptor.setupErrorInterception(this.discordLogger);
        
        this.apiKey = UtilsConfig.loadApiKey();
    }
    
    /**
     * Fetches the list of current clan member tags from the API
     */
    private List<String> fetchClanMemberTags() throws IOException, InterruptedException {
        String url = CLAN_MEMBERS_API + "%23" + clanTag + "/members";
        
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + apiKey)
                .header("Accept", "application/json")
                .GET()
                .build();
        
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() != 200) {
            String errMsg = "Clan members API request failed with status code: " + response.statusCode();
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new IOException(errMsg);
        }
        
        JsonObject clanData = JsonParser.parseString(response.body()).getAsJsonObject();
        List<String> memberTags = new ArrayList<>();
        
        if (clanData.has("items") && clanData.get("items").isJsonArray()) {
            JsonArray items = clanData.getAsJsonArray("items");
            for (JsonElement member : items) {
                JsonObject memberObj = member.getAsJsonObject();
                String tag = UtilsJson.getJsonString(memberObj, "tag");
                if (tag != null) {
                    memberTags.add(tag.replace("#", ""));
                }
            }
        }
        
        String successMsg = "Fetched " + memberTags.size() + " member tags for clan: " + clanTag;
        System.out.println(successMsg);
        discordLogger.logSuccess(successMsg);
        
        return memberTags;
    }
    
    /**
     * Fetches detailed player data from the API
     */
    private JsonObject fetchPlayerData(String playerTag) throws IOException, InterruptedException {
        String url = PLAYER_API + "%23" + playerTag;
        
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + apiKey)
                .header("Accept", "application/json")
                .GET()
                .build();
        
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() != 200) {
            String errMsg = "Player API request failed for " + playerTag + " with status code: " + response.statusCode();
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            throw new IOException(errMsg);
        }
        
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }
    
    /**
     * Gets all player tags currently in the clan (dateLeft IS NULL)
     */
    private Set<String> getCurrentMembersFromDB() throws SQLException {
        Set<String> currentMembers = new HashSet<>();
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT playerTag FROM " + TABLE_NAME + " WHERE dateLeft IS NULL";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            while (rs.next()) {
                currentMembers.add(rs.getString("playerTag"));
            }
        }
        
        return currentMembers;
    }
    
    /**
     * Checks if a player tag exists anywhere in the database
     */
    private boolean playerExistsInDB(String playerTag) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT 1 FROM " + TABLE_NAME + " WHERE playerTag = ?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, playerTag);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    
    /**
     * Gets existing player data from database
     */
    private PlayerRecord getPlayerFromDB(String playerTag) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "SELECT * FROM " + TABLE_NAME + " WHERE playerTag = ?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, playerTag);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new PlayerRecord(rs);
                }
            }
        }
        return null;
    }
    
    /**
     * Gets the value of an achievement by name
     */
    private Integer getAchievementValue(JsonArray achievements, String achievementName) {
        if (achievements == null) return null;
        
        for (JsonElement elem : achievements) {
            JsonObject achievement = elem.getAsJsonObject();
            String name = UtilsJson.getJsonString(achievement, "name");
            if (achievementName.equals(name)) {
                return UtilsJson.getJsonInt(achievement, "value");
            }
        }
        return null;
    }
    
    /**
     * Gets troop/spell/hero/equipment level by name
     */
    private Integer getItemLevel(JsonArray items, String itemName, String village) {
        if (items == null) return null;
        
        for (JsonElement elem : items) {
            JsonObject item = elem.getAsJsonObject();
            String name = UtilsJson.getJsonString(item, "name");
            String itemVillage = UtilsJson.getJsonString(item, "village");
            
            if (itemName.equals(name) && (village == null || village.equals(itemVillage))) {
                return UtilsJson.getJsonInt(item, "level");
            }
        }
        return null;
    }
    
    /**
     * Parses player data from API and fills missing values from database
     */
    private PlayerData parsePlayerData(JsonObject playerJson, PlayerRecord existingRecord) {
        PlayerData data = new PlayerData();
        
        // Basic info
        data.name = UtilsJson.getJsonString(playerJson, "name");
        data.thLevel = UtilsJson.getJsonInt(playerJson, "townHallLevel");
        data.bhLevel = UtilsJson.getJsonInt(playerJson, "builderHallLevel");
        data.xpLevel = UtilsJson.getJsonInt(playerJson, "expLevel");
        
        // Trophies
        data.trophies = UtilsJson.getJsonInt(playerJson, "trophies");
        data.bestTrophies = UtilsJson.getJsonInt(playerJson, "bestTrophies");
        data.bbTrophies = UtilsJson.getJsonInt(playerJson, "builderBaseTrophies");
        data.bestBbTrophies = UtilsJson.getJsonInt(playerJson, "bestBuilderBaseTrophies");
        
        // Legend trophies
        if (playerJson.has("legendStatistics") && playerJson.get("legendStatistics").isJsonObject()) {
            JsonObject legendStats = playerJson.getAsJsonObject("legendStatistics");
            data.legendTrophies = UtilsJson.getJsonInt(legendStats, "legendTrophies");
        }
        
        // War & attacks
        data.warStars = UtilsJson.getJsonInt(playerJson, "warStars");
        data.attackWins = UtilsJson.getJsonInt(playerJson, "attackWins");
        data.defenseWins = UtilsJson.getJsonInt(playerJson, "defenseWins");
        
        // Clan info
        data.clanRole = UtilsJson.getJsonString(playerJson, "role");
        String warPref = UtilsJson.getJsonString(playerJson, "warPreference");
        if ("in".equals(warPref)) {
            data.warPreference = true;
        } else if ("out".equals(warPref)) {
            data.warPreference = false;
        }
        data.donations = UtilsJson.getJsonInt(playerJson, "donations");
        data.donationsReceived = UtilsJson.getJsonInt(playerJson, "donationsReceived");
        data.clanCapitalContributions = UtilsJson.getJsonInt(playerJson, "clanCapitalContributions");
        
        // Leagues - only get from "league", not "leagueTier"
        if (playerJson.has("league") && playerJson.get("league").isJsonObject()) {
            JsonObject league = playerJson.getAsJsonObject("league");
            data.legacyLeagueName = UtilsJson.getJsonString(league, "name");
        }
        
        // League tier - get last 2 digits
        if (playerJson.has("leagueTier") && playerJson.get("leagueTier").isJsonObject()) {
            JsonObject leagueTier = playerJson.getAsJsonObject("leagueTier");
            Integer leagueId = UtilsJson.getJsonInt(leagueTier, "id");
            if (leagueId != null) {
                data.leagueInt = leagueId % 100; // Get last 2 digits
            }
        }
        
        // Builder base league
        if (playerJson.has("builderBaseLeague") && playerJson.get("builderBaseLeague").isJsonObject()) {
            JsonObject bbLeague = playerJson.getAsJsonObject("builderBaseLeague");
            data.bbLeagueName = UtilsJson.getJsonString(bbLeague, "name");
        }
        
        // Extract arrays
        JsonArray achievements = playerJson.has("achievements") ? playerJson.getAsJsonArray("achievements") : null;
        JsonArray troops = playerJson.has("troops") ? playerJson.getAsJsonArray("troops") : null;
        JsonArray spells = playerJson.has("spells") ? playerJson.getAsJsonArray("spells") : null;
        JsonArray heroes = playerJson.has("heroes") ? playerJson.getAsJsonArray("heroes") : null;
        JsonArray heroEquipment = playerJson.has("heroEquipment") ? playerJson.getAsJsonArray("heroEquipment") : null;
        
        // Parse all achievements, troops, spells, heroes, equipment
        // (Simplified - in production you'd parse all ~200 fields)
        parseAchievements(data, achievements);
        parseTroops(data, troops);
        parseSpells(data, spells);
        parseHeroes(data, heroes);
        parseHeroEquipment(data, heroEquipment);
        
        // Fill in missing values from existing database data
        if (existingRecord != null) {
            data.fillMissingValues(existingRecord);
        }
        
        return data;
    }
    
    private void parseAchievements(PlayerData data, JsonArray achievements) {
        // Parse all 53 achievements
        data.achievementBiggerCoffers = getAchievementValue(achievements, "Bigger Coffers");
        data.achievementGetThoseGoblins = getAchievementValue(achievements, "Get those Goblins!");
        data.achievementBiggerBetter = getAchievementValue(achievements, "Bigger & Better");
        data.achievementNiceAndTidy = getAchievementValue(achievements, "Nice and Tidy");
        data.achievementDiscoverNewTroops = getAchievementValue(achievements, "Discover New Troops");
        data.achievementGoldGrab = getAchievementValue(achievements, "Gold Grab");
        data.achievementElixirEscapade = getAchievementValue(achievements, "Elixir Escapade");
        data.achievementSweetVictory = getAchievementValue(achievements, "Sweet Victory!");
        data.achievementEmpireBuilder = getAchievementValue(achievements, "Empire Builder");
        data.achievementWallBuster = getAchievementValue(achievements, "Wall Buster");
        data.achievementHumiliator = getAchievementValue(achievements, "Humiliator");
        data.achievementUnionBuster = getAchievementValue(achievements, "Union Buster");
        data.achievementConqueror = getAchievementValue(achievements, "Conqueror");
        data.achievementUnbreakable = getAchievementValue(achievements, "Unbreakable");
        data.achievementFriendInNeed = getAchievementValue(achievements, "Friend in Need");
        data.achievementMortarMauler = getAchievementValue(achievements, "Mortar Mauler");
        data.achievementHeroicHeist = getAchievementValue(achievements, "Heroic Heist");
        data.achievementLeagueAllStar = getAchievementValue(achievements, "League All-Star");
        data.achievementXBowExterminator = getAchievementValue(achievements, "X-Bow Exterminator");
        data.achievementFirefighter = getAchievementValue(achievements, "Firefighter");
        data.achievementWarHero = getAchievementValue(achievements, "War Hero");
        data.achievementClanWarWealth = getAchievementValue(achievements, "Clan War Wealth");
        data.achievementAntiArtillery = getAchievementValue(achievements, "Anti-Artillery");
        data.achievementSharingIsCaring = getAchievementValue(achievements, "Sharing is caring");
        data.achievementKeepYourAccountSafe = getAchievementValue(achievements, "Keep your account safe!");
        data.achievementMasterEngineering = getAchievementValue(achievements, "Master Engineering");
        data.achievementNextGenerationModel = getAchievementValue(achievements, "Next Generation Model");
        data.achievementUnBuildIt = getAchievementValue(achievements, "Un-Build It");
        data.achievementChampionBuilder = getAchievementValue(achievements, "Champion Builder");
        data.achievementHighGear = getAchievementValue(achievements, "High Gear");
        data.achievementHiddenTreasures = getAchievementValue(achievements, "Hidden Treasures");
        data.achievementGamesChampion = getAchievementValue(achievements, "Games Champion");
        data.achievementDragonSlayer = getAchievementValue(achievements, "Dragon Slayer");
        data.achievementWarLeagueLegend = getAchievementValue(achievements, "War League Legend");
        data.achievementWellSeasoned = getAchievementValue(achievements, "Well Seasoned");
        data.achievementShatteredAndScattered = getAchievementValue(achievements, "Shattered and Scattered");
        data.achievementNotSoEasyThisTime = getAchievementValue(achievements, "Not So Easy This Time");
        data.achievementBustThis = getAchievementValue(achievements, "Bust This!");
        data.achievementSuperbWork = getAchievementValue(achievements, "Superb Work");
        data.achievementSiegeSharer = getAchievementValue(achievements, "Siege Sharer");
        data.achievementAggressiveCapitalism = getAchievementValue(achievements, "Aggressive Capitalism");
        data.achievementMostValuableClanmate = getAchievementValue(achievements, "Most Valuable Clanmate");
        data.achievementCounterspell = getAchievementValue(achievements, "Counterspell");
        data.achievementMonolithMasher = getAchievementValue(achievements, "Monolith Masher");
        data.achievementUngratefulChild = getAchievementValue(achievements, "Ungrateful Child");
        data.achievementSupercharger = getAchievementValue(achievements, "Supercharger");
        data.achievementMultiArcherTowerTerminator = getAchievementValue(achievements, "Multi-Archer Tower Terminator");
        data.achievementRicochetCannonCrusher = getAchievementValue(achievements, "Ricochet Cannon Crusher");
        data.achievementFirespitterFinisher = getAchievementValue(achievements, "Firespitter Finisher");
        data.achievementMultiGearTowerTrampler = getAchievementValue(achievements, "Multi-Gear Tower Trampler");
        data.achievementCraftingConnoisseur = getAchievementValue(achievements, "Crafting Connoisseur");
        data.achievementCraftersNightmare = getAchievementValue(achievements, "Crafter's Nightmare");
        data.achievementLeagueFollower = getAchievementValue(achievements, "League Follower");
    }
    
    private void parseTroops(PlayerData data, JsonArray troops) {
        // Elixir Troops (18)
        data.lvlTroopElixirBarbarian = getItemLevel(troops, "Barbarian", "home");
        data.lvlTroopElixirArcher = getItemLevel(troops, "Archer", "home");
        data.lvlTroopElixirGiant = getItemLevel(troops, "Giant", "home");
        data.lvlTroopElixirGoblin = getItemLevel(troops, "Goblin", "home");
        data.lvlTroopElixirWallBreaker = getItemLevel(troops, "Wall Breaker", "home");
        data.lvlTroopElixirBalloon = getItemLevel(troops, "Balloon", "home");
        data.lvlTroopElixirWizard = getItemLevel(troops, "Wizard", "home");
        data.lvlTroopElixirHealer = getItemLevel(troops, "Healer", "home");
        data.lvlTroopElixirDragon = getItemLevel(troops, "Dragon", "home");
        data.lvlTroopElixirPEKKA = getItemLevel(troops, "P.E.K.K.A", "home");
        data.lvlTroopElixirBabyDragon = getItemLevel(troops, "Baby Dragon", "home");
        data.lvlTroopElixirMiner = getItemLevel(troops, "Miner", "home");
        data.lvlTroopElixirElectroDragon = getItemLevel(troops, "Electro Dragon", "home");
        data.lvlTroopElixirYeti = getItemLevel(troops, "Yeti", "home");
        data.lvlTroopElixirDragonRider = getItemLevel(troops, "Dragon Rider", "home");
        data.lvlTroopElixirElectroTitan = getItemLevel(troops, "Electro Titan", "home");
        data.lvlTroopElixirRootRider = getItemLevel(troops, "Root Rider", "home");
        data.lvlTroopElixirThrower = getItemLevel(troops, "Thrower", "home");
        
        // Dark Elixir Troops (12)
        data.lvlTroopDarkElixirMinion = getItemLevel(troops, "Minion", "home");
        data.lvlTroopDarkElixirHogRider = getItemLevel(troops, "Hog Rider", "home");
        data.lvlTroopDarkElixirValkyrie = getItemLevel(troops, "Valkyrie", "home");
        data.lvlTroopDarkElixirGolem = getItemLevel(troops, "Golem", "home");
        data.lvlTroopDarkElixirWitch = getItemLevel(troops, "Witch", "home");
        data.lvlTroopDarkElixirLavaHound = getItemLevel(troops, "Lava Hound", "home");
        data.lvlTroopDarkElixirBowler = getItemLevel(troops, "Bowler", "home");
        data.lvlTroopDarkElixirIceGolem = getItemLevel(troops, "Ice Golem", "home");
        data.lvlTroopDarkElixirHeadhunter = getItemLevel(troops, "Headhunter", "home");
        data.lvlTroopDarkElixirApprenticeWarden = getItemLevel(troops, "Apprentice Warden", "home");
        data.lvlTroopDarkElixirDruid = getItemLevel(troops, "Druid", "home");
        data.lvlTroopDarkElixirFurnace = getItemLevel(troops, "Furnace", "home");
        
        // Builder Base Troops (12)
        data.lvlTroopBuilderBaseRagedBarbarian = getItemLevel(troops, "Raged Barbarian", "builderBase");
        data.lvlTroopBuilderBaseSneakyArcher = getItemLevel(troops, "Sneaky Archer", "builderBase");
        data.lvlTroopBuilderBaseBoxerGiant = getItemLevel(troops, "Boxer Giant", "builderBase");
        data.lvlTroopBuilderBaseBetaMinion = getItemLevel(troops, "Beta Minion", "builderBase");
        data.lvlTroopBuilderBaseBomber = getItemLevel(troops, "Bomber", "builderBase");
        data.lvlTroopBuilderBaseBabyDragon = getItemLevel(troops, "Baby Dragon", "builderBase");
        data.lvlTroopBuilderBaseCannonCart = getItemLevel(troops, "Cannon Cart", "builderBase");
        data.lvlTroopBuilderBaseNightWitch = getItemLevel(troops, "Night Witch", "builderBase");
        data.lvlTroopBuilderBaseDropShip = getItemLevel(troops, "Drop Ship", "builderBase");
        data.lvlTroopBuilderBasePowerPekka = getItemLevel(troops, "Power P.E.K.K.A", "builderBase");
        data.lvlTroopBuilderBaseHogGlider = getItemLevel(troops, "Hog Glider", "builderBase");
        data.lvlTroopBuilderBaseElectrofireWizard = getItemLevel(troops, "Electrofire Wizard", "builderBase");
        
        // Siege Machines (8)
        data.lvlSiegeWallWrecker = getItemLevel(troops, "Wall Wrecker", "home");
        data.lvlSiegeBattleBlimp = getItemLevel(troops, "Battle Blimp", "home");
        data.lvlSiegeStoneSlammer = getItemLevel(troops, "Stone Slammer", "home");
        data.lvlSiegeSiegeBarracks = getItemLevel(troops, "Siege Barracks", "home");
        data.lvlSiegeLogLauncher = getItemLevel(troops, "Log Launcher", "home");
        data.lvlSiegeFlameFlinger = getItemLevel(troops, "Flame Flinger", "home");
        data.lvlSiegeBattleDrill = getItemLevel(troops, "Battle Drill", "home");
        data.lvlSiegeTroopLauncher = getItemLevel(troops, "Troop Launcher", "home");
        
        // Pets (11)
        data.lvlPetLASSI = getItemLevel(troops, "L.A.S.S.I", "home");
        data.lvlPetMightyYak = getItemLevel(troops, "Mighty Yak", "home");
        data.lvlPetElectroOwl = getItemLevel(troops, "Electro Owl", "home");
        data.lvlPetUnicorn = getItemLevel(troops, "Unicorn", "home");
        data.lvlPetPhoenix = getItemLevel(troops, "Phoenix", "home");
        data.lvlPetPoisonLizard = getItemLevel(troops, "Poison Lizard", "home");
        data.lvlPetDiggy = getItemLevel(troops, "Diggy", "home");
        data.lvlPetFrosty = getItemLevel(troops, "Frosty", "home");
        data.lvlPetSpiritFox = getItemLevel(troops, "Spirit Fox", "home");
        data.lvlPetAngryJelly = getItemLevel(troops, "Angry Jelly", "home");
        data.lvlPetSneezy = getItemLevel(troops, "Sneezy", "home");
    }
    
    private void parseSpells(PlayerData data, JsonArray spells) {
        // Spells (9)
        data.lvlSpellLightning = getItemLevel(spells, "Lightning Spell", "home");
        data.lvlSpellHealing = getItemLevel(spells, "Healing Spell", "home");
        data.lvlSpellRage = getItemLevel(spells, "Rage Spell", "home");
        data.lvlSpellJump = getItemLevel(spells, "Jump Spell", "home");
        data.lvlSpellFreeze = getItemLevel(spells, "Freeze Spell", "home");
        data.lvlSpellClone = getItemLevel(spells, "Clone Spell", "home");
        data.lvlSpellInvisibility = getItemLevel(spells, "Invisibility Spell", "home");
        data.lvlSpellRecall = getItemLevel(spells, "Recall Spell", "home");
        data.lvlSpellRevive = getItemLevel(spells, "Revive Spell", "home");
        
        // Dark Spells (7)
        data.lvlDarkSpellPoison = getItemLevel(spells, "Poison Spell", "home");
        data.lvlDarkSpellEarthquake = getItemLevel(spells, "Earthquake Spell", "home");
        data.lvlDarkSpellHaste = getItemLevel(spells, "Haste Spell", "home");
        data.lvlDarkSpellSkeleton = getItemLevel(spells, "Skeleton Spell", "home");
        data.lvlDarkSpellBat = getItemLevel(spells, "Bat Spell", "home");
        data.lvlDarkSpellOvergrowth = getItemLevel(spells, "Overgrowth Spell", "home");
        data.lvlDarkSpellIceBlock = getItemLevel(spells, "Ice Block Spell", "home");
    }
    
    private void parseHeroes(PlayerData data, JsonArray heroes) {
        // Heroes (7)
        data.lvlHeroBarbarianKing = getItemLevel(heroes, "Barbarian King", null);
        data.lvlHeroArcherQueen = getItemLevel(heroes, "Archer Queen", null);
        data.lvlHeroMinionPrince = getItemLevel(heroes, "Minion Prince", null);
        data.lvlHeroGrandWarden = getItemLevel(heroes, "Grand Warden", null);
        data.lvlHeroRoyalChampion = getItemLevel(heroes, "Royal Champion", null);
        data.lvlHeroBattleMachine = getItemLevel(heroes, "Battle Machine", null);
        data.lvlHeroBattleCopter = getItemLevel(heroes, "Battle Copter", null);
    }
    
    private void parseHeroEquipment(PlayerData data, JsonArray heroEquipment) {
        // Hero Equipment (35)
        data.lvlHeroEquipmentBarbarianPuppet = getItemLevel(heroEquipment, "Barbarian Puppet", null);
        data.lvlHeroEquipmentRageVial = getItemLevel(heroEquipment, "Rage Vial", null);
        data.lvlHeroEquipmentEarthquakeBoots = getItemLevel(heroEquipment, "Earthquake Boots", null);
        data.lvlHeroEquipmentVampstache = getItemLevel(heroEquipment, "Vampstache", null);
        data.lvlHeroEquipmentGiantGauntlet = getItemLevel(heroEquipment, "Giant Gauntlet", null);
        data.lvlHeroEquipmentSpikyBall = getItemLevel(heroEquipment, "Spiky Ball", null);
        data.lvlHeroEquipmentSnakeBracelet = getItemLevel(heroEquipment, "Snake Bracelet", null);
        data.lvlHeroEquipmentStickHorse = getItemLevel(heroEquipment, "Stick Horse", null);
        
        data.lvlHeroEquipmentArcherPuppet = getItemLevel(heroEquipment, "Archer Puppet", null);
        data.lvlHeroEquipmentInvisibilityVial = getItemLevel(heroEquipment, "Invisibility Vial", null);
        data.lvlHeroEquipmentGiantArrow = getItemLevel(heroEquipment, "Giant Arrow", null);
        data.lvlHeroEquipmentHealerPuppet = getItemLevel(heroEquipment, "Healer Puppet", null);
        data.lvlHeroEquipmentFrozenArrow = getItemLevel(heroEquipment, "Frozen Arrow", null);
        data.lvlHeroEquipmentMagicMirror = getItemLevel(heroEquipment, "Magic Mirror", null);
        data.lvlHeroEquipmentActionFigure = getItemLevel(heroEquipment, "Action Figure", null);
        
        data.lvlHeroEquipmentHenchmenPuppet = getItemLevel(heroEquipment, "Henchmen Puppet", null);
        data.lvlHeroEquipmentDarkOrb = getItemLevel(heroEquipment, "Dark Orb", null);
        data.lvlHeroEquipmentMetalPants = getItemLevel(heroEquipment, "Metal Pants", null);
        data.lvlHeroEquipmentNobleIron = getItemLevel(heroEquipment, "Noble Iron", null);
        data.lvlHeroEquipmentDarkCrown = getItemLevel(heroEquipment, "Dark Crown", null);
        data.lvlHeroEquipmentMeteorStaff = getItemLevel(heroEquipment, "Meteor Staff", null);
        
        data.lvlHeroEquipmentEternalTome = getItemLevel(heroEquipment, "Eternal Tome", null);
        data.lvlHeroEquipmentLifeGem = getItemLevel(heroEquipment, "Life Gem", null);
        data.lvlHeroEquipmentRageGem = getItemLevel(heroEquipment, "Rage Gem", null);
        data.lvlHeroEquipmentHealingTome = getItemLevel(heroEquipment, "Healing Tome", null);
        data.lvlHeroEquipmentFireball = getItemLevel(heroEquipment, "Fireball", null);
        data.lvlHeroEquipmentLavaloonPuppet = getItemLevel(heroEquipment, "Lavaloon Puppet", null);
        data.lvlHeroEquipmentHeroicTorch = getItemLevel(heroEquipment, "Heroic Torch", null);
        
        data.lvlHeroEquipmentRoyalGem = getItemLevel(heroEquipment, "Royal Gem", null);
        data.lvlHeroEquipmentSeekingShield = getItemLevel(heroEquipment, "Seeking Shield", null);
        data.lvlHeroEquipmentHogRiderPuppet = getItemLevel(heroEquipment, "Hog Rider Puppet", null);
        data.lvlHeroEquipmentHasteVial = getItemLevel(heroEquipment, "Haste Vial", null);
        data.lvlHeroEquipmentRocketSpear = getItemLevel(heroEquipment, "Rocket Spear", null);
        data.lvlHeroEquipmentElectroBoots = getItemLevel(heroEquipment, "Electro Boots", null);
        data.lvlHeroEquipmentFrostFlake = getItemLevel(heroEquipment, "Frost Flake", null);
    }
    
    /**
     * Checks if any non-datetime field changed
     */
    private boolean hasDataChanged(PlayerData newData, PlayerRecord existingRecord) {
        if (existingRecord == null) return true;
        
        // Compare all non-datetime fields
        return !Objects.equals(newData.name, existingRecord.name) ||
               !Objects.equals(newData.thLevel, existingRecord.thLevel) ||
               !Objects.equals(newData.bhLevel, existingRecord.bhLevel) ||
               !Objects.equals(newData.xpLevel, existingRecord.xpLevel) ||
               !Objects.equals(newData.trophies, existingRecord.trophies) ||
               !Objects.equals(newData.bestTrophies, existingRecord.bestTrophies) ||
               !Objects.equals(newData.legendTrophies, existingRecord.legendTrophies) ||
               !Objects.equals(newData.bbTrophies, existingRecord.bbTrophies) ||
               !Objects.equals(newData.bestBbTrophies, existingRecord.bestBbTrophies) ||
               !Objects.equals(newData.warStars, existingRecord.warStars) ||
               !Objects.equals(newData.attackWins, existingRecord.attackWins) ||
               !Objects.equals(newData.defenseWins, existingRecord.defenseWins) ||
               !Objects.equals(newData.clanRole, existingRecord.clanRole) ||
               !Objects.equals(newData.warPreference, existingRecord.warPreference) ||
               !Objects.equals(newData.donations, existingRecord.donations) ||
               !Objects.equals(newData.donationsReceived, existingRecord.donationsReceived) ||
               !Objects.equals(newData.clanCapitalContributions, existingRecord.clanCapitalContributions) ||
               !Objects.equals(newData.legacyLeagueName, existingRecord.legacyLeagueName) ||
               !Objects.equals(newData.leagueInt, existingRecord.leagueInt) ||
               !Objects.equals(newData.bbLeagueName, existingRecord.bbLeagueName) ||
               
               // Achievements
               !Objects.equals(newData.achievementBiggerCoffers, existingRecord.achievementBiggerCoffers) ||
               !Objects.equals(newData.achievementGetThoseGoblins, existingRecord.achievementGetThoseGoblins) ||
               !Objects.equals(newData.achievementBiggerBetter, existingRecord.achievementBiggerBetter) ||
               !Objects.equals(newData.achievementNiceAndTidy, existingRecord.achievementNiceAndTidy) ||
               !Objects.equals(newData.achievementDiscoverNewTroops, existingRecord.achievementDiscoverNewTroops) ||
               !Objects.equals(newData.achievementGoldGrab, existingRecord.achievementGoldGrab) ||
               !Objects.equals(newData.achievementElixirEscapade, existingRecord.achievementElixirEscapade) ||
               !Objects.equals(newData.achievementSweetVictory, existingRecord.achievementSweetVictory) ||
               !Objects.equals(newData.achievementEmpireBuilder, existingRecord.achievementEmpireBuilder) ||
               !Objects.equals(newData.achievementWallBuster, existingRecord.achievementWallBuster) ||
               !Objects.equals(newData.achievementHumiliator, existingRecord.achievementHumiliator) ||
               !Objects.equals(newData.achievementUnionBuster, existingRecord.achievementUnionBuster) ||
               !Objects.equals(newData.achievementConqueror, existingRecord.achievementConqueror) ||
               !Objects.equals(newData.achievementUnbreakable, existingRecord.achievementUnbreakable) ||
               !Objects.equals(newData.achievementFriendInNeed, existingRecord.achievementFriendInNeed) ||
               !Objects.equals(newData.achievementMortarMauler, existingRecord.achievementMortarMauler) ||
               !Objects.equals(newData.achievementHeroicHeist, existingRecord.achievementHeroicHeist) ||
               !Objects.equals(newData.achievementLeagueAllStar, existingRecord.achievementLeagueAllStar) ||
               !Objects.equals(newData.achievementXBowExterminator, existingRecord.achievementXBowExterminator) ||
               !Objects.equals(newData.achievementFirefighter, existingRecord.achievementFirefighter) ||
               !Objects.equals(newData.achievementWarHero, existingRecord.achievementWarHero) ||
               !Objects.equals(newData.achievementClanWarWealth, existingRecord.achievementClanWarWealth) ||
               !Objects.equals(newData.achievementAntiArtillery, existingRecord.achievementAntiArtillery) ||
               !Objects.equals(newData.achievementSharingIsCaring, existingRecord.achievementSharingIsCaring) ||
               !Objects.equals(newData.achievementKeepYourAccountSafe, existingRecord.achievementKeepYourAccountSafe) ||
               !Objects.equals(newData.achievementMasterEngineering, existingRecord.achievementMasterEngineering) ||
               !Objects.equals(newData.achievementNextGenerationModel, existingRecord.achievementNextGenerationModel) ||
               !Objects.equals(newData.achievementUnBuildIt, existingRecord.achievementUnBuildIt) ||
               !Objects.equals(newData.achievementChampionBuilder, existingRecord.achievementChampionBuilder) ||
               !Objects.equals(newData.achievementHighGear, existingRecord.achievementHighGear) ||
               !Objects.equals(newData.achievementHiddenTreasures, existingRecord.achievementHiddenTreasures) ||
               !Objects.equals(newData.achievementGamesChampion, existingRecord.achievementGamesChampion) ||
               !Objects.equals(newData.achievementDragonSlayer, existingRecord.achievementDragonSlayer) ||
               !Objects.equals(newData.achievementWarLeagueLegend, existingRecord.achievementWarLeagueLegend) ||
               !Objects.equals(newData.achievementWellSeasoned, existingRecord.achievementWellSeasoned) ||
               !Objects.equals(newData.achievementShatteredAndScattered, existingRecord.achievementShatteredAndScattered) ||
               !Objects.equals(newData.achievementNotSoEasyThisTime, existingRecord.achievementNotSoEasyThisTime) ||
               !Objects.equals(newData.achievementBustThis, existingRecord.achievementBustThis) ||
               !Objects.equals(newData.achievementSuperbWork, existingRecord.achievementSuperbWork) ||
               !Objects.equals(newData.achievementSiegeSharer, existingRecord.achievementSiegeSharer) ||
               !Objects.equals(newData.achievementAggressiveCapitalism, existingRecord.achievementAggressiveCapitalism) ||
               !Objects.equals(newData.achievementMostValuableClanmate, existingRecord.achievementMostValuableClanmate) ||
               !Objects.equals(newData.achievementCounterspell, existingRecord.achievementCounterspell) ||
               !Objects.equals(newData.achievementMonolithMasher, existingRecord.achievementMonolithMasher) ||
               !Objects.equals(newData.achievementUngratefulChild, existingRecord.achievementUngratefulChild) ||
               !Objects.equals(newData.achievementSupercharger, existingRecord.achievementSupercharger) ||
               !Objects.equals(newData.achievementMultiArcherTowerTerminator, existingRecord.achievementMultiArcherTowerTerminator) ||
               !Objects.equals(newData.achievementRicochetCannonCrusher, existingRecord.achievementRicochetCannonCrusher) ||
               !Objects.equals(newData.achievementFirespitterFinisher, existingRecord.achievementFirespitterFinisher) ||
               !Objects.equals(newData.achievementMultiGearTowerTrampler, existingRecord.achievementMultiGearTowerTrampler) ||
               !Objects.equals(newData.achievementCraftingConnoisseur, existingRecord.achievementCraftingConnoisseur) ||
               !Objects.equals(newData.achievementCraftersNightmare, existingRecord.achievementCraftersNightmare) ||
               !Objects.equals(newData.achievementLeagueFollower, existingRecord.achievementLeagueFollower) ||
               
               // Elixir Troops
               !Objects.equals(newData.lvlTroopElixirBarbarian, existingRecord.lvlTroopElixirBarbarian) ||
               !Objects.equals(newData.lvlTroopElixirArcher, existingRecord.lvlTroopElixirArcher) ||
               !Objects.equals(newData.lvlTroopElixirGiant, existingRecord.lvlTroopElixirGiant) ||
               !Objects.equals(newData.lvlTroopElixirGoblin, existingRecord.lvlTroopElixirGoblin) ||
               !Objects.equals(newData.lvlTroopElixirWallBreaker, existingRecord.lvlTroopElixirWallBreaker) ||
               !Objects.equals(newData.lvlTroopElixirBalloon, existingRecord.lvlTroopElixirBalloon) ||
               !Objects.equals(newData.lvlTroopElixirWizard, existingRecord.lvlTroopElixirWizard) ||
               !Objects.equals(newData.lvlTroopElixirHealer, existingRecord.lvlTroopElixirHealer) ||
               !Objects.equals(newData.lvlTroopElixirDragon, existingRecord.lvlTroopElixirDragon) ||
               !Objects.equals(newData.lvlTroopElixirPEKKA, existingRecord.lvlTroopElixirPEKKA) ||
               !Objects.equals(newData.lvlTroopElixirBabyDragon, existingRecord.lvlTroopElixirBabyDragon) ||
               !Objects.equals(newData.lvlTroopElixirMiner, existingRecord.lvlTroopElixirMiner) ||
               !Objects.equals(newData.lvlTroopElixirElectroDragon, existingRecord.lvlTroopElixirElectroDragon) ||
               !Objects.equals(newData.lvlTroopElixirYeti, existingRecord.lvlTroopElixirYeti) ||
               !Objects.equals(newData.lvlTroopElixirDragonRider, existingRecord.lvlTroopElixirDragonRider) ||
               !Objects.equals(newData.lvlTroopElixirElectroTitan, existingRecord.lvlTroopElixirElectroTitan) ||
               !Objects.equals(newData.lvlTroopElixirRootRider, existingRecord.lvlTroopElixirRootRider) ||
               !Objects.equals(newData.lvlTroopElixirThrower, existingRecord.lvlTroopElixirThrower) ||
               
               // Dark Elixir Troops
               !Objects.equals(newData.lvlTroopDarkElixirMinion, existingRecord.lvlTroopDarkElixirMinion) ||
               !Objects.equals(newData.lvlTroopDarkElixirHogRider, existingRecord.lvlTroopDarkElixirHogRider) ||
               !Objects.equals(newData.lvlTroopDarkElixirValkyrie, existingRecord.lvlTroopDarkElixirValkyrie) ||
               !Objects.equals(newData.lvlTroopDarkElixirGolem, existingRecord.lvlTroopDarkElixirGolem) ||
               !Objects.equals(newData.lvlTroopDarkElixirWitch, existingRecord.lvlTroopDarkElixirWitch) ||
               !Objects.equals(newData.lvlTroopDarkElixirLavaHound, existingRecord.lvlTroopDarkElixirLavaHound) ||
               !Objects.equals(newData.lvlTroopDarkElixirBowler, existingRecord.lvlTroopDarkElixirBowler) ||
               !Objects.equals(newData.lvlTroopDarkElixirIceGolem, existingRecord.lvlTroopDarkElixirIceGolem) ||
               !Objects.equals(newData.lvlTroopDarkElixirHeadhunter, existingRecord.lvlTroopDarkElixirHeadhunter) ||
               !Objects.equals(newData.lvlTroopDarkElixirApprenticeWarden, existingRecord.lvlTroopDarkElixirApprenticeWarden) ||
               !Objects.equals(newData.lvlTroopDarkElixirDruid, existingRecord.lvlTroopDarkElixirDruid) ||
               !Objects.equals(newData.lvlTroopDarkElixirFurnace, existingRecord.lvlTroopDarkElixirFurnace) ||
               
               // Spells
               !Objects.equals(newData.lvlSpellLightning, existingRecord.lvlSpellLightning) ||
               !Objects.equals(newData.lvlSpellHealing, existingRecord.lvlSpellHealing) ||
               !Objects.equals(newData.lvlSpellRage, existingRecord.lvlSpellRage) ||
               !Objects.equals(newData.lvlSpellJump, existingRecord.lvlSpellJump) ||
               !Objects.equals(newData.lvlSpellFreeze, existingRecord.lvlSpellFreeze) ||
               !Objects.equals(newData.lvlSpellClone, existingRecord.lvlSpellClone) ||
               !Objects.equals(newData.lvlSpellInvisibility, existingRecord.lvlSpellInvisibility) ||
               !Objects.equals(newData.lvlSpellRecall, existingRecord.lvlSpellRecall) ||
               !Objects.equals(newData.lvlSpellRevive, existingRecord.lvlSpellRevive) ||
               
               // Dark Spells
               !Objects.equals(newData.lvlDarkSpellPoison, existingRecord.lvlDarkSpellPoison) ||
               !Objects.equals(newData.lvlDarkSpellEarthquake, existingRecord.lvlDarkSpellEarthquake) ||
               !Objects.equals(newData.lvlDarkSpellHaste, existingRecord.lvlDarkSpellHaste) ||
               !Objects.equals(newData.lvlDarkSpellSkeleton, existingRecord.lvlDarkSpellSkeleton) ||
               !Objects.equals(newData.lvlDarkSpellBat, existingRecord.lvlDarkSpellBat) ||
               !Objects.equals(newData.lvlDarkSpellOvergrowth, existingRecord.lvlDarkSpellOvergrowth) ||
               !Objects.equals(newData.lvlDarkSpellIceBlock, existingRecord.lvlDarkSpellIceBlock) ||
               
               // Builder Base Troops
               !Objects.equals(newData.lvlTroopBuilderBaseRagedBarbarian, existingRecord.lvlTroopBuilderBaseRagedBarbarian) ||
               !Objects.equals(newData.lvlTroopBuilderBaseSneakyArcher, existingRecord.lvlTroopBuilderBaseSneakyArcher) ||
               !Objects.equals(newData.lvlTroopBuilderBaseBoxerGiant, existingRecord.lvlTroopBuilderBaseBoxerGiant) ||
               !Objects.equals(newData.lvlTroopBuilderBaseBetaMinion, existingRecord.lvlTroopBuilderBaseBetaMinion) ||
               !Objects.equals(newData.lvlTroopBuilderBaseBomber, existingRecord.lvlTroopBuilderBaseBomber) ||
               !Objects.equals(newData.lvlTroopBuilderBaseBabyDragon, existingRecord.lvlTroopBuilderBaseBabyDragon) ||
               !Objects.equals(newData.lvlTroopBuilderBaseCannonCart, existingRecord.lvlTroopBuilderBaseCannonCart) ||
               !Objects.equals(newData.lvlTroopBuilderBaseNightWitch, existingRecord.lvlTroopBuilderBaseNightWitch) ||
               !Objects.equals(newData.lvlTroopBuilderBaseDropShip, existingRecord.lvlTroopBuilderBaseDropShip) ||
               !Objects.equals(newData.lvlTroopBuilderBasePowerPekka, existingRecord.lvlTroopBuilderBasePowerPekka) ||
               !Objects.equals(newData.lvlTroopBuilderBaseHogGlider, existingRecord.lvlTroopBuilderBaseHogGlider) ||
               !Objects.equals(newData.lvlTroopBuilderBaseElectrofireWizard, existingRecord.lvlTroopBuilderBaseElectrofireWizard) ||
               
               // Siege Machines
               !Objects.equals(newData.lvlSiegeWallWrecker, existingRecord.lvlSiegeWallWrecker) ||
               !Objects.equals(newData.lvlSiegeBattleBlimp, existingRecord.lvlSiegeBattleBlimp) ||
               !Objects.equals(newData.lvlSiegeStoneSlammer, existingRecord.lvlSiegeStoneSlammer) ||
               !Objects.equals(newData.lvlSiegeSiegeBarracks, existingRecord.lvlSiegeSiegeBarracks) ||
               !Objects.equals(newData.lvlSiegeLogLauncher, existingRecord.lvlSiegeLogLauncher) ||
               !Objects.equals(newData.lvlSiegeFlameFlinger, existingRecord.lvlSiegeFlameFlinger) ||
               !Objects.equals(newData.lvlSiegeBattleDrill, existingRecord.lvlSiegeBattleDrill) ||
               !Objects.equals(newData.lvlSiegeTroopLauncher, existingRecord.lvlSiegeTroopLauncher) ||
               
               // Pets
               !Objects.equals(newData.lvlPetLASSI, existingRecord.lvlPetLASSI) ||
               !Objects.equals(newData.lvlPetMightyYak, existingRecord.lvlPetMightyYak) ||
               !Objects.equals(newData.lvlPetElectroOwl, existingRecord.lvlPetElectroOwl) ||
               !Objects.equals(newData.lvlPetUnicorn, existingRecord.lvlPetUnicorn) ||
               !Objects.equals(newData.lvlPetPhoenix, existingRecord.lvlPetPhoenix) ||
               !Objects.equals(newData.lvlPetPoisonLizard, existingRecord.lvlPetPoisonLizard) ||
               !Objects.equals(newData.lvlPetDiggy, existingRecord.lvlPetDiggy) ||
               !Objects.equals(newData.lvlPetFrosty, existingRecord.lvlPetFrosty) ||
               !Objects.equals(newData.lvlPetSpiritFox, existingRecord.lvlPetSpiritFox) ||
               !Objects.equals(newData.lvlPetAngryJelly, existingRecord.lvlPetAngryJelly) ||
               !Objects.equals(newData.lvlPetSneezy, existingRecord.lvlPetSneezy) ||
               
               // Heroes
               !Objects.equals(newData.lvlHeroBarbarianKing, existingRecord.lvlHeroBarbarianKing) ||
               !Objects.equals(newData.lvlHeroArcherQueen, existingRecord.lvlHeroArcherQueen) ||
               !Objects.equals(newData.lvlHeroMinionPrince, existingRecord.lvlHeroMinionPrince) ||
               !Objects.equals(newData.lvlHeroGrandWarden, existingRecord.lvlHeroGrandWarden) ||
               !Objects.equals(newData.lvlHeroRoyalChampion, existingRecord.lvlHeroRoyalChampion) ||
               !Objects.equals(newData.lvlHeroBattleMachine, existingRecord.lvlHeroBattleMachine) ||
               !Objects.equals(newData.lvlHeroBattleCopter, existingRecord.lvlHeroBattleCopter) ||
               
               // Hero Equipment - Barbarian King
               !Objects.equals(newData.lvlHeroEquipmentBarbarianPuppet, existingRecord.lvlHeroEquipmentBarbarianPuppet) ||
               !Objects.equals(newData.lvlHeroEquipmentRageVial, existingRecord.lvlHeroEquipmentRageVial) ||
               !Objects.equals(newData.lvlHeroEquipmentEarthquakeBoots, existingRecord.lvlHeroEquipmentEarthquakeBoots) ||
               !Objects.equals(newData.lvlHeroEquipmentVampstache, existingRecord.lvlHeroEquipmentVampstache) ||
               !Objects.equals(newData.lvlHeroEquipmentGiantGauntlet, existingRecord.lvlHeroEquipmentGiantGauntlet) ||
               !Objects.equals(newData.lvlHeroEquipmentSpikyBall, existingRecord.lvlHeroEquipmentSpikyBall) ||
               !Objects.equals(newData.lvlHeroEquipmentSnakeBracelet, existingRecord.lvlHeroEquipmentSnakeBracelet) ||
               !Objects.equals(newData.lvlHeroEquipmentStickHorse, existingRecord.lvlHeroEquipmentStickHorse) ||
               
               // Hero Equipment - Archer Queen
               !Objects.equals(newData.lvlHeroEquipmentArcherPuppet, existingRecord.lvlHeroEquipmentArcherPuppet) ||
               !Objects.equals(newData.lvlHeroEquipmentInvisibilityVial, existingRecord.lvlHeroEquipmentInvisibilityVial) ||
               !Objects.equals(newData.lvlHeroEquipmentGiantArrow, existingRecord.lvlHeroEquipmentGiantArrow) ||
               !Objects.equals(newData.lvlHeroEquipmentHealerPuppet, existingRecord.lvlHeroEquipmentHealerPuppet) ||
               !Objects.equals(newData.lvlHeroEquipmentFrozenArrow, existingRecord.lvlHeroEquipmentFrozenArrow) ||
               !Objects.equals(newData.lvlHeroEquipmentMagicMirror, existingRecord.lvlHeroEquipmentMagicMirror) ||
               !Objects.equals(newData.lvlHeroEquipmentActionFigure, existingRecord.lvlHeroEquipmentActionFigure) ||
               
               // Hero Equipment - Minion Prince
               !Objects.equals(newData.lvlHeroEquipmentHenchmenPuppet, existingRecord.lvlHeroEquipmentHenchmenPuppet) ||
               !Objects.equals(newData.lvlHeroEquipmentDarkOrb, existingRecord.lvlHeroEquipmentDarkOrb) ||
               !Objects.equals(newData.lvlHeroEquipmentMetalPants, existingRecord.lvlHeroEquipmentMetalPants) ||
               !Objects.equals(newData.lvlHeroEquipmentNobleIron, existingRecord.lvlHeroEquipmentNobleIron) ||
               !Objects.equals(newData.lvlHeroEquipmentDarkCrown, existingRecord.lvlHeroEquipmentDarkCrown) ||
               !Objects.equals(newData.lvlHeroEquipmentMeteorStaff, existingRecord.lvlHeroEquipmentMeteorStaff) ||
               
               // Hero Equipment - Grand Warden
               !Objects.equals(newData.lvlHeroEquipmentEternalTome, existingRecord.lvlHeroEquipmentEternalTome) ||
               !Objects.equals(newData.lvlHeroEquipmentLifeGem, existingRecord.lvlHeroEquipmentLifeGem) ||
               !Objects.equals(newData.lvlHeroEquipmentRageGem, existingRecord.lvlHeroEquipmentRageGem) ||
               !Objects.equals(newData.lvlHeroEquipmentHealingTome, existingRecord.lvlHeroEquipmentHealingTome) ||
               !Objects.equals(newData.lvlHeroEquipmentFireball, existingRecord.lvlHeroEquipmentFireball) ||
               !Objects.equals(newData.lvlHeroEquipmentLavaloonPuppet, existingRecord.lvlHeroEquipmentLavaloonPuppet) ||
               !Objects.equals(newData.lvlHeroEquipmentHeroicTorch, existingRecord.lvlHeroEquipmentHeroicTorch) ||
               
               // Hero Equipment - Royal Champion
               !Objects.equals(newData.lvlHeroEquipmentRoyalGem, existingRecord.lvlHeroEquipmentRoyalGem) ||
               !Objects.equals(newData.lvlHeroEquipmentSeekingShield, existingRecord.lvlHeroEquipmentSeekingShield) ||
               !Objects.equals(newData.lvlHeroEquipmentHogRiderPuppet, existingRecord.lvlHeroEquipmentHogRiderPuppet) ||
               !Objects.equals(newData.lvlHeroEquipmentHasteVial, existingRecord.lvlHeroEquipmentHasteVial) ||
               !Objects.equals(newData.lvlHeroEquipmentRocketSpear, existingRecord.lvlHeroEquipmentRocketSpear) ||
               !Objects.equals(newData.lvlHeroEquipmentElectroBoots, existingRecord.lvlHeroEquipmentElectroBoots) ||
               !Objects.equals(newData.lvlHeroEquipmentFrostFlake, existingRecord.lvlHeroEquipmentFrostFlake);
    }
    
    /**
     * Updates or inserts player data
     */
    private void upsertPlayer(String playerTag, PlayerData data, boolean isNewPlayer, boolean isRejoin, boolean dataChanged, String currentDateTime) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        
        if (isNewPlayer) {
            insertNewPlayer(playerTag, data, currentDateTime);
        } else if (isRejoin) {
            rejoinPlayer(playerTag, data, currentDateTime);
        } else {
            updateExistingPlayer(playerTag, data, dataChanged, currentDateTime);
        }
    }
    
    private void insertNewPlayer(String playerTag, PlayerData data, String currentDateTime) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "INSERT INTO " + TABLE_NAME + " (playerTag, name, lastUpdated, dateJoin, lastActive, " +
                     "thLevel, bhLevel, xpLevel, trophies, bestTrophies, legendTrophies, bbTrophies, bestBbTrophies, " +
                     "warStars, attackWins, defenseWins, clanRole, warPreference, donations, donationsReceived, " +
                     "clanCapitalContributions, legacyLeagueName, leagueInt, bbLeagueName, " +
                     "achievementBiggerCoffers, achievementGetThoseGoblins, achievementBiggerBetter, achievementNiceAndTidy, " +
                     "achievementDiscoverNewTroops, achievementGoldGrab, achievementElixirEscapade, achievementSweetVictory, " +
                     "achievementEmpireBuilder, achievementWallBuster, achievementHumiliator, achievementUnionBuster, " +
                     "achievementConqueror, achievementUnbreakable, achievementFriendInNeed, achievementMortarMauler, " +
                     "achievementHeroicHeist, achievementLeagueAllStar, achievementXBowExterminator, achievementFirefighter, " +
                     "achievementWarHero, achievementClanWarWealth, achievementAntiArtillery, achievementSharingIsCaring, " +
                     "achievementKeepYourAccountSafe, achievementMasterEngineering, achievementNextGenerationModel, achievementUnBuildIt, " +
                     "achievementChampionBuilder, achievementHighGear, achievementHiddenTreasures, achievementGamesChampion, " +
                     "achievementDragonSlayer, achievementWarLeagueLegend, achievementWellSeasoned, achievementShatteredAndScattered, " +
                     "achievementNotSoEasyThisTime, achievementBustThis, achievementSuperbWork, achievementSiegeSharer, " +
                     "achievementAggressiveCapitalism, achievementMostValuableClanmate, achievementCounterspell, achievementMonolithMasher, " +
                     "achievementUngratefulChild, achievementSupercharger, achievementMultiArcherTowerTerminator, achievementRicochetCannonCrusher, " +
                     "achievementFirespitterFinisher, achievementMultiGearTowerTrampler, achievementCraftingConnoisseur, achievementCraftersNightmare, " +
                     "achievementLeagueFollower, " +
                     "lvlTroopElixirBarbarian, lvlTroopElixirArcher, lvlTroopElixirGiant, lvlTroopElixirGoblin, " +
                     "lvlTroopElixirWallBreaker, lvlTroopElixirBalloon, lvlTroopElixirWizard, lvlTroopElixirHealer, " +
                     "lvlTroopElixirDragon, lvlTroopElixirPEKKA, lvlTroopElixirBabyDragon, lvlTroopElixirMiner, " +
                     "lvlTroopElixirElectroDragon, lvlTroopElixirYeti, lvlTroopElixirDragonRider, lvlTroopElixirElectroTitan, " +
                     "lvlTroopElixirRootRider, lvlTroopElixirThrower, " +
                     "lvlTroopDarkElixirMinion, lvlTroopDarkElixirHogRider, lvlTroopDarkElixirValkyrie, lvlTroopDarkElixirGolem, " +
                     "lvlTroopDarkElixirWitch, lvlTroopDarkElixirLavaHound, lvlTroopDarkElixirBowler, lvlTroopDarkElixirIceGolem, " +
                     "lvlTroopDarkElixirHeadhunter, lvlTroopDarkElixirApprenticeWarden, lvlTroopDarkElixirDruid, lvlTroopDarkElixirFurnace, " +
                     "lvlSpellLightning, lvlSpellHealing, lvlSpellRage, lvlSpellJump, lvlSpellFreeze, lvlSpellClone, " +
                     "lvlSpellInvisibility, lvlSpellRecall, lvlSpellRevive, " +
                     "lvlDarkSpellPoison, lvlDarkSpellEarthquake, lvlDarkSpellHaste, lvlDarkSpellSkeleton, lvlDarkSpellBat, " +
                     "lvlDarkSpellOvergrowth, lvlDarkSpellIceBlock, " +
                     "lvlTroopBuilderBaseRagedBarbarian, lvlTroopBuilderBaseSneakyArcher, lvlTroopBuilderBaseBoxerGiant, lvlTroopBuilderBaseBetaMinion, " +
                     "lvlTroopBuilderBaseBomber, lvlTroopBuilderBaseBabyDragon, lvlTroopBuilderBaseCannonCart, lvlTroopBuilderBaseNightWitch, " +
                     "lvlTroopBuilderBaseDropShip, lvlTroopBuilderBasePowerPekka, lvlTroopBuilderBaseHogGlider, lvlTroopBuilderBaseElectrofireWizard, " +
                     "lvlSiegeWallWrecker, lvlSiegeBattleBlimp, lvlSiegeStoneSlammer, lvlSiegeSiegeBarracks, " +
                     "lvlSiegeLogLauncher, lvlSiegeFlameFlinger, lvlSiegeBattleDrill, lvlSiegeTroopLauncher, " +
                     "lvlPetLASSI, lvlPetMightyYak, lvlPetElectroOwl, lvlPetUnicorn, lvlPetPhoenix, " +
                     "lvlPetPoisonLizard, lvlPetDiggy, lvlPetFrosty, lvlPetSpiritFox, lvlPetAngryJelly, lvlPetSneezy, " +
                     "lvlHeroBarbarianKing, lvlHeroArcherQueen, lvlHeroMinionPrince, lvlHeroGrandWarden, lvlHeroRoyalChampion, " +
                     "lvlHeroBattleMachine, lvlHeroBattleCopter, " +
                     "lvlHeroEquipmentBarbarianPuppet, lvlHeroEquipmentRageVial, lvlHeroEquipmentEarthquakeBoots, lvlHeroEquipmentVampstache, " +
                     "lvlHeroEquipmentGiantGauntlet, lvlHeroEquipmentSpikyBall, lvlHeroEquipmentSnakeBracelet, lvlHeroEquipmentStickHorse, " +
                     "lvlHeroEquipmentArcherPuppet, lvlHeroEquipmentInvisibilityVial, lvlHeroEquipmentGiantArrow, lvlHeroEquipmentHealerPuppet, " +
                     "lvlHeroEquipmentFrozenArrow, lvlHeroEquipmentMagicMirror, lvlHeroEquipmentActionFigure, " +
                     "lvlHeroEquipmentHenchmenPuppet, lvlHeroEquipmentDarkOrb, lvlHeroEquipmentMetalPants, lvlHeroEquipmentNobleIron, " +
                     "lvlHeroEquipmentDarkCrown, lvlHeroEquipmentMeteorStaff, " +
                     "lvlHeroEquipmentEternalTome, lvlHeroEquipmentLifeGem, lvlHeroEquipmentRageGem, lvlHeroEquipmentHealingTome, " +
                     "lvlHeroEquipmentFireball, lvlHeroEquipmentLavaloonPuppet, lvlHeroEquipmentHeroicTorch, " +
                     "lvlHeroEquipmentRoyalGem, lvlHeroEquipmentSeekingShield, lvlHeroEquipmentHogRiderPuppet, lvlHeroEquipmentHasteVial, " +
                     "lvlHeroEquipmentRocketSpear, lvlHeroEquipmentElectroBoots, lvlHeroEquipmentFrostFlake" +
                     ") VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, " +
                     "?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, " +
                     "?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, " +
                     "?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, " +
                     "?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, " +
                     "?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, " +
                     "?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?)";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            int idx = 1;
            pstmt.setString(idx++, playerTag);
            UtilsDatabase.setNullableString(pstmt, idx++, data.name);
            pstmt.setString(idx++, currentDateTime); // lastUpdated
            pstmt.setString(idx++, currentDateTime); // dateJoin
            pstmt.setString(idx++, currentDateTime); // lastActive
            
            UtilsDatabase.setNullableInt(pstmt, idx++, data.thLevel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bhLevel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.xpLevel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.trophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bestTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.legendTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bbTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bestBbTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.warStars);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.attackWins);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.defenseWins);
            UtilsDatabase.setNullableString(pstmt, idx++, data.clanRole);
            UtilsDatabase.setNullableBoolean(pstmt, idx++, data.warPreference);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.donations);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.donationsReceived);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.clanCapitalContributions);
            UtilsDatabase.setNullableString(pstmt, idx++, data.legacyLeagueName);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.leagueInt);
            UtilsDatabase.setNullableString(pstmt, idx++, data.bbLeagueName);
            
            // Achievements (53)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementBiggerCoffers);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementGetThoseGoblins);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementBiggerBetter);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementNiceAndTidy);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementDiscoverNewTroops);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementGoldGrab);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementElixirEscapade);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSweetVictory);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementEmpireBuilder);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWallBuster);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHumiliator);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUnionBuster);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementConqueror);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUnbreakable);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementFriendInNeed);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMortarMauler);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHeroicHeist);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementLeagueAllStar);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementXBowExterminator);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementFirefighter);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWarHero);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementClanWarWealth);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementAntiArtillery);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSharingIsCaring);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementKeepYourAccountSafe);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMasterEngineering);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementNextGenerationModel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUnBuildIt);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementChampionBuilder);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHighGear);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHiddenTreasures);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementGamesChampion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementDragonSlayer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWarLeagueLegend);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWellSeasoned);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementShatteredAndScattered);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementNotSoEasyThisTime);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementBustThis);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSuperbWork);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSiegeSharer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementAggressiveCapitalism);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMostValuableClanmate);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementCounterspell);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMonolithMasher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUngratefulChild);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSupercharger);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMultiArcherTowerTerminator);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementRicochetCannonCrusher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementFirespitterFinisher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMultiGearTowerTrampler);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementCraftingConnoisseur);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementCraftersNightmare);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementLeagueFollower);
            
            // Elixir Troops (18)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirBarbarian);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirArcher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirGiant);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirGoblin);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirWallBreaker);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirBalloon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirWizard);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirHealer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirPEKKA);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirBabyDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirMiner);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirElectroDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirYeti);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirDragonRider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirElectroTitan);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirRootRider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirThrower);
            
            // Dark Elixir Troops (12)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirMinion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirHogRider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirValkyrie);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirGolem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirWitch);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirLavaHound);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirBowler);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirIceGolem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirHeadhunter);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirApprenticeWarden);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirDruid);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirFurnace);
            
            // Spells (9)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellLightning);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellHealing);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellRage);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellJump);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellFreeze);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellClone);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellInvisibility);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellRecall);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellRevive);
            
            // Dark Spells (7)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellPoison);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellEarthquake);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellHaste);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellSkeleton);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellBat);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellOvergrowth);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellIceBlock);
            
            // Builder Base Troops (12)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseRagedBarbarian);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseSneakyArcher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBoxerGiant);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBetaMinion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBomber);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBabyDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseCannonCart);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseNightWitch);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseDropShip);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBasePowerPekka);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseHogGlider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseElectrofireWizard);
            
            // Siege Machines (8)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeWallWrecker);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeBattleBlimp);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeStoneSlammer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeSiegeBarracks);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeLogLauncher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeFlameFlinger);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeBattleDrill);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeTroopLauncher);
            
            // Pets (11)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetLASSI);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetMightyYak);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetElectroOwl);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetUnicorn);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetPhoenix);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetPoisonLizard);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetDiggy);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetFrosty);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetSpiritFox);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetAngryJelly);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetSneezy);
            
            // Heroes (7)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroBarbarianKing);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroArcherQueen);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroMinionPrince);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroGrandWarden);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroRoyalChampion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroBattleMachine);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroBattleCopter);
            
            // Hero Equipment (35)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentBarbarianPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRageVial);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentEarthquakeBoots);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentVampstache);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentGiantGauntlet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentSpikyBall);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentSnakeBracelet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentStickHorse);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentArcherPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentInvisibilityVial);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentGiantArrow);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHealerPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentFrozenArrow);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentMagicMirror);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentActionFigure);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHenchmenPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentDarkOrb);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentMetalPants);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentNobleIron);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentDarkCrown);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentMeteorStaff);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentEternalTome);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentLifeGem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRageGem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHealingTome);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentFireball);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentLavaloonPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHeroicTorch);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRoyalGem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentSeekingShield);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHogRiderPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHasteVial);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRocketSpear);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentElectroBoots);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentFrostFlake);
            
            pstmt.executeUpdate();
            
            String msg = "Inserted new player: " + playerTag;
            System.out.println(msg);
            discordLogger.logSuccess(msg);
        }
    }
    
    private void rejoinPlayer(String playerTag, PlayerData data, String currentDateTime) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "UPDATE " + TABLE_NAME + " SET name=?, lastUpdated=?, dateJoin=?, dateLeft=NULL, lastActive=?, " +
                     "thLevel=?, bhLevel=?, xpLevel=?, trophies=?, bestTrophies=?, legendTrophies=?, bbTrophies=?, bestBbTrophies=?, " +
                     "warStars=?, attackWins=?, defenseWins=?, clanRole=?, warPreference=?, donations=?, donationsReceived=?, " +
                     "clanCapitalContributions=?, legacyLeagueName=?, leagueInt=?, bbLeagueName=?, " +
                     "achievementBiggerCoffers=?, achievementGetThoseGoblins=?, achievementBiggerBetter=?, achievementNiceAndTidy=?, " +
                     "achievementDiscoverNewTroops=?, achievementGoldGrab=?, achievementElixirEscapade=?, achievementSweetVictory=?, " +
                     "achievementEmpireBuilder=?, achievementWallBuster=?, achievementHumiliator=?, achievementUnionBuster=?, " +
                     "achievementConqueror=?, achievementUnbreakable=?, achievementFriendInNeed=?, achievementMortarMauler=?, " +
                     "achievementHeroicHeist=?, achievementLeagueAllStar=?, achievementXBowExterminator=?, achievementFirefighter=?, " +
                     "achievementWarHero=?, achievementClanWarWealth=?, achievementAntiArtillery=?, achievementSharingIsCaring=?, " +
                     "achievementKeepYourAccountSafe=?, achievementMasterEngineering=?, achievementNextGenerationModel=?, achievementUnBuildIt=?, " +
                     "achievementChampionBuilder=?, achievementHighGear=?, achievementHiddenTreasures=?, achievementGamesChampion=?, " +
                     "achievementDragonSlayer=?, achievementWarLeagueLegend=?, achievementWellSeasoned=?, achievementShatteredAndScattered=?, " +
                     "achievementNotSoEasyThisTime=?, achievementBustThis=?, achievementSuperbWork=?, achievementSiegeSharer=?, " +
                     "achievementAggressiveCapitalism=?, achievementMostValuableClanmate=?, achievementCounterspell=?, achievementMonolithMasher=?, " +
                     "achievementUngratefulChild=?, achievementSupercharger=?, achievementMultiArcherTowerTerminator=?, achievementRicochetCannonCrusher=?, " +
                     "achievementFirespitterFinisher=?, achievementMultiGearTowerTrampler=?, achievementCraftingConnoisseur=?, achievementCraftersNightmare=?, " +
                     "achievementLeagueFollower=?, " +
                     "lvlTroopElixirBarbarian=?, lvlTroopElixirArcher=?, lvlTroopElixirGiant=?, lvlTroopElixirGoblin=?, " +
                     "lvlTroopElixirWallBreaker=?, lvlTroopElixirBalloon=?, lvlTroopElixirWizard=?, lvlTroopElixirHealer=?, " +
                     "lvlTroopElixirDragon=?, lvlTroopElixirPEKKA=?, lvlTroopElixirBabyDragon=?, lvlTroopElixirMiner=?, " +
                     "lvlTroopElixirElectroDragon=?, lvlTroopElixirYeti=?, lvlTroopElixirDragonRider=?, lvlTroopElixirElectroTitan=?, " +
                     "lvlTroopElixirRootRider=?, lvlTroopElixirThrower=?, " +
                     "lvlTroopDarkElixirMinion=?, lvlTroopDarkElixirHogRider=?, lvlTroopDarkElixirValkyrie=?, lvlTroopDarkElixirGolem=?, " +
                     "lvlTroopDarkElixirWitch=?, lvlTroopDarkElixirLavaHound=?, lvlTroopDarkElixirBowler=?, lvlTroopDarkElixirIceGolem=?, " +
                     "lvlTroopDarkElixirHeadhunter=?, lvlTroopDarkElixirApprenticeWarden=?, lvlTroopDarkElixirDruid=?, lvlTroopDarkElixirFurnace=?, " +
                     "lvlSpellLightning=?, lvlSpellHealing=?, lvlSpellRage=?, lvlSpellJump=?, lvlSpellFreeze=?, lvlSpellClone=?, " +
                     "lvlSpellInvisibility=?, lvlSpellRecall=?, lvlSpellRevive=?, " +
                     "lvlDarkSpellPoison=?, lvlDarkSpellEarthquake=?, lvlDarkSpellHaste=?, lvlDarkSpellSkeleton=?, lvlDarkSpellBat=?, " +
                     "lvlDarkSpellOvergrowth=?, lvlDarkSpellIceBlock=?, " +
                     "lvlTroopBuilderBaseRagedBarbarian=?, lvlTroopBuilderBaseSneakyArcher=?, lvlTroopBuilderBaseBoxerGiant=?, lvlTroopBuilderBaseBetaMinion=?, " +
                     "lvlTroopBuilderBaseBomber=?, lvlTroopBuilderBaseBabyDragon=?, lvlTroopBuilderBaseCannonCart=?, lvlTroopBuilderBaseNightWitch=?, " +
                     "lvlTroopBuilderBaseDropShip=?, lvlTroopBuilderBasePowerPekka=?, lvlTroopBuilderBaseHogGlider=?, lvlTroopBuilderBaseElectrofireWizard=?, " +
                     "lvlSiegeWallWrecker=?, lvlSiegeBattleBlimp=?, lvlSiegeStoneSlammer=?, lvlSiegeSiegeBarracks=?, " +
                     "lvlSiegeLogLauncher=?, lvlSiegeFlameFlinger=?, lvlSiegeBattleDrill=?, lvlSiegeTroopLauncher=?, " +
                     "lvlPetLASSI=?, lvlPetMightyYak=?, lvlPetElectroOwl=?, lvlPetUnicorn=?, lvlPetPhoenix=?, " +
                     "lvlPetPoisonLizard=?, lvlPetDiggy=?, lvlPetFrosty=?, lvlPetSpiritFox=?, lvlPetAngryJelly=?, lvlPetSneezy=?, " +
                     "lvlHeroBarbarianKing=?, lvlHeroArcherQueen=?, lvlHeroMinionPrince=?, lvlHeroGrandWarden=?, lvlHeroRoyalChampion=?, " +
                     "lvlHeroBattleMachine=?, lvlHeroBattleCopter=?, " +
                     "lvlHeroEquipmentBarbarianPuppet=?, lvlHeroEquipmentRageVial=?, lvlHeroEquipmentEarthquakeBoots=?, lvlHeroEquipmentVampstache=?, " +
                     "lvlHeroEquipmentGiantGauntlet=?, lvlHeroEquipmentSpikyBall=?, lvlHeroEquipmentSnakeBracelet=?, lvlHeroEquipmentStickHorse=?, " +
                     "lvlHeroEquipmentArcherPuppet=?, lvlHeroEquipmentInvisibilityVial=?, lvlHeroEquipmentGiantArrow=?, lvlHeroEquipmentHealerPuppet=?, " +
                     "lvlHeroEquipmentFrozenArrow=?, lvlHeroEquipmentMagicMirror=?, lvlHeroEquipmentActionFigure=?, " +
                     "lvlHeroEquipmentHenchmenPuppet=?, lvlHeroEquipmentDarkOrb=?, lvlHeroEquipmentMetalPants=?, lvlHeroEquipmentNobleIron=?, " +
                     "lvlHeroEquipmentDarkCrown=?, lvlHeroEquipmentMeteorStaff=?, " +
                     "lvlHeroEquipmentEternalTome=?, lvlHeroEquipmentLifeGem=?, lvlHeroEquipmentRageGem=?, lvlHeroEquipmentHealingTome=?, " +
                     "lvlHeroEquipmentFireball=?, lvlHeroEquipmentLavaloonPuppet=?, lvlHeroEquipmentHeroicTorch=?, " +
                     "lvlHeroEquipmentRoyalGem=?, lvlHeroEquipmentSeekingShield=?, lvlHeroEquipmentHogRiderPuppet=?, lvlHeroEquipmentHasteVial=?, " +
                     "lvlHeroEquipmentRocketSpear=?, lvlHeroEquipmentElectroBoots=?, lvlHeroEquipmentFrostFlake=? " +
                     "WHERE playerTag=?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            int idx = 1;
            UtilsDatabase.setNullableString(pstmt, idx++, data.name);
            pstmt.setString(idx++, currentDateTime); // lastUpdated
            pstmt.setString(idx++, currentDateTime); // dateJoin reset
            pstmt.setString(idx++, currentDateTime); // lastActive
            
            UtilsDatabase.setNullableInt(pstmt, idx++, data.thLevel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bhLevel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.xpLevel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.trophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bestTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.legendTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bbTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bestBbTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.warStars);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.attackWins);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.defenseWins);
            UtilsDatabase.setNullableString(pstmt, idx++, data.clanRole);
            UtilsDatabase.setNullableBoolean(pstmt, idx++, data.warPreference);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.donations);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.donationsReceived);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.clanCapitalContributions);
            UtilsDatabase.setNullableString(pstmt, idx++, data.legacyLeagueName);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.leagueInt);
            UtilsDatabase.setNullableString(pstmt, idx++, data.bbLeagueName);
            
            // Achievements (53)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementBiggerCoffers);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementGetThoseGoblins);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementBiggerBetter);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementNiceAndTidy);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementDiscoverNewTroops);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementGoldGrab);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementElixirEscapade);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSweetVictory);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementEmpireBuilder);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWallBuster);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHumiliator);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUnionBuster);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementConqueror);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUnbreakable);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementFriendInNeed);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMortarMauler);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHeroicHeist);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementLeagueAllStar);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementXBowExterminator);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementFirefighter);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWarHero);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementClanWarWealth);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementAntiArtillery);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSharingIsCaring);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementKeepYourAccountSafe);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMasterEngineering);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementNextGenerationModel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUnBuildIt);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementChampionBuilder);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHighGear);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHiddenTreasures);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementGamesChampion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementDragonSlayer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWarLeagueLegend);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWellSeasoned);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementShatteredAndScattered);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementNotSoEasyThisTime);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementBustThis);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSuperbWork);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSiegeSharer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementAggressiveCapitalism);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMostValuableClanmate);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementCounterspell);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMonolithMasher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUngratefulChild);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSupercharger);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMultiArcherTowerTerminator);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementRicochetCannonCrusher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementFirespitterFinisher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMultiGearTowerTrampler);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementCraftingConnoisseur);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementCraftersNightmare);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementLeagueFollower);
            
            // Elixir Troops (18)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirBarbarian);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirArcher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirGiant);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirGoblin);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirWallBreaker);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirBalloon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirWizard);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirHealer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirPEKKA);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirBabyDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirMiner);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirElectroDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirYeti);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirDragonRider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirElectroTitan);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirRootRider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirThrower);
            
            // Dark Elixir Troops (12)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirMinion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirHogRider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirValkyrie);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirGolem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirWitch);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirLavaHound);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirBowler);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirIceGolem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirHeadhunter);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirApprenticeWarden);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirDruid);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirFurnace);
            
            // Spells (9)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellLightning);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellHealing);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellRage);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellJump);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellFreeze);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellClone);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellInvisibility);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellRecall);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellRevive);
            
            // Dark Spells (7)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellPoison);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellEarthquake);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellHaste);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellSkeleton);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellBat);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellOvergrowth);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellIceBlock);
            
            // Builder Base Troops (12)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseRagedBarbarian);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseSneakyArcher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBoxerGiant);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBetaMinion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBomber);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBabyDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseCannonCart);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseNightWitch);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseDropShip);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBasePowerPekka);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseHogGlider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseElectrofireWizard);
            
            // Siege Machines (8)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeWallWrecker);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeBattleBlimp);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeStoneSlammer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeSiegeBarracks);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeLogLauncher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeFlameFlinger);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeBattleDrill);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeTroopLauncher);
            
            // Pets (11)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetLASSI);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetMightyYak);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetElectroOwl);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetUnicorn);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetPhoenix);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetPoisonLizard);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetDiggy);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetFrosty);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetSpiritFox);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetAngryJelly);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetSneezy);
            
            // Heroes (7)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroBarbarianKing);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroArcherQueen);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroMinionPrince);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroGrandWarden);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroRoyalChampion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroBattleMachine);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroBattleCopter);
            
            // Hero Equipment (35)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentBarbarianPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRageVial);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentEarthquakeBoots);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentVampstache);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentGiantGauntlet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentSpikyBall);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentSnakeBracelet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentStickHorse);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentArcherPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentInvisibilityVial);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentGiantArrow);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHealerPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentFrozenArrow);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentMagicMirror);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentActionFigure);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHenchmenPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentDarkOrb);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentMetalPants);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentNobleIron);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentDarkCrown);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentMeteorStaff);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentEternalTome);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentLifeGem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRageGem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHealingTome);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentFireball);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentLavaloonPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHeroicTorch);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRoyalGem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentSeekingShield);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHogRiderPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHasteVial);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRocketSpear);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentElectroBoots);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentFrostFlake);
            
            pstmt.setString(idx++, playerTag); // WHERE clause
            pstmt.executeUpdate();
            
            String msg = "Rejoined player: " + playerTag;
            System.out.println(msg);
            discordLogger.logSuccess(msg);
        }
    }
    
    private void updateExistingPlayer(String playerTag, PlayerData data, boolean dataChanged, String currentDateTime) throws SQLException {
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "UPDATE " + TABLE_NAME + " SET name=?, lastUpdated=?, " +
                     (dataChanged ? "lastActive=?, " : "") +
                     "thLevel=?, bhLevel=?, xpLevel=?, trophies=?, bestTrophies=?, legendTrophies=?, bbTrophies=?, bestBbTrophies=?, " +
                     "warStars=?, attackWins=?, defenseWins=?, clanRole=?, warPreference=?, donations=?, donationsReceived=?, " +
                     "clanCapitalContributions=?, legacyLeagueName=?, leagueInt=?, bbLeagueName=?, " +
                     "achievementBiggerCoffers=?, achievementGetThoseGoblins=?, achievementBiggerBetter=?, achievementNiceAndTidy=?, " +
                     "achievementDiscoverNewTroops=?, achievementGoldGrab=?, achievementElixirEscapade=?, achievementSweetVictory=?, " +
                     "achievementEmpireBuilder=?, achievementWallBuster=?, achievementHumiliator=?, achievementUnionBuster=?, " +
                     "achievementConqueror=?, achievementUnbreakable=?, achievementFriendInNeed=?, achievementMortarMauler=?, " +
                     "achievementHeroicHeist=?, achievementLeagueAllStar=?, achievementXBowExterminator=?, achievementFirefighter=?, " +
                     "achievementWarHero=?, achievementClanWarWealth=?, achievementAntiArtillery=?, achievementSharingIsCaring=?, " +
                     "achievementKeepYourAccountSafe=?, achievementMasterEngineering=?, achievementNextGenerationModel=?, achievementUnBuildIt=?, " +
                     "achievementChampionBuilder=?, achievementHighGear=?, achievementHiddenTreasures=?, achievementGamesChampion=?, " +
                     "achievementDragonSlayer=?, achievementWarLeagueLegend=?, achievementWellSeasoned=?, achievementShatteredAndScattered=?, " +
                     "achievementNotSoEasyThisTime=?, achievementBustThis=?, achievementSuperbWork=?, achievementSiegeSharer=?, " +
                     "achievementAggressiveCapitalism=?, achievementMostValuableClanmate=?, achievementCounterspell=?, achievementMonolithMasher=?, " +
                     "achievementUngratefulChild=?, achievementSupercharger=?, achievementMultiArcherTowerTerminator=?, achievementRicochetCannonCrusher=?, " +
                     "achievementFirespitterFinisher=?, achievementMultiGearTowerTrampler=?, achievementCraftingConnoisseur=?, achievementCraftersNightmare=?, " +
                     "achievementLeagueFollower=?, " +
                     "lvlTroopElixirBarbarian=?, lvlTroopElixirArcher=?, lvlTroopElixirGiant=?, lvlTroopElixirGoblin=?, " +
                     "lvlTroopElixirWallBreaker=?, lvlTroopElixirBalloon=?, lvlTroopElixirWizard=?, lvlTroopElixirHealer=?, " +
                     "lvlTroopElixirDragon=?, lvlTroopElixirPEKKA=?, lvlTroopElixirBabyDragon=?, lvlTroopElixirMiner=?, " +
                     "lvlTroopElixirElectroDragon=?, lvlTroopElixirYeti=?, lvlTroopElixirDragonRider=?, lvlTroopElixirElectroTitan=?, " +
                     "lvlTroopElixirRootRider=?, lvlTroopElixirThrower=?, " +
                     "lvlTroopDarkElixirMinion=?, lvlTroopDarkElixirHogRider=?, lvlTroopDarkElixirValkyrie=?, lvlTroopDarkElixirGolem=?, " +
                     "lvlTroopDarkElixirWitch=?, lvlTroopDarkElixirLavaHound=?, lvlTroopDarkElixirBowler=?, lvlTroopDarkElixirIceGolem=?, " +
                     "lvlTroopDarkElixirHeadhunter=?, lvlTroopDarkElixirApprenticeWarden=?, lvlTroopDarkElixirDruid=?, lvlTroopDarkElixirFurnace=?, " +
                     "lvlSpellLightning=?, lvlSpellHealing=?, lvlSpellRage=?, lvlSpellJump=?, lvlSpellFreeze=?, lvlSpellClone=?, " +
                     "lvlSpellInvisibility=?, lvlSpellRecall=?, lvlSpellRevive=?, " +
                     "lvlDarkSpellPoison=?, lvlDarkSpellEarthquake=?, lvlDarkSpellHaste=?, lvlDarkSpellSkeleton=?, lvlDarkSpellBat=?, " +
                     "lvlDarkSpellOvergrowth=?, lvlDarkSpellIceBlock=?, " +
                     "lvlTroopBuilderBaseRagedBarbarian=?, lvlTroopBuilderBaseSneakyArcher=?, lvlTroopBuilderBaseBoxerGiant=?, lvlTroopBuilderBaseBetaMinion=?, " +
                     "lvlTroopBuilderBaseBomber=?, lvlTroopBuilderBaseBabyDragon=?, lvlTroopBuilderBaseCannonCart=?, lvlTroopBuilderBaseNightWitch=?, " +
                     "lvlTroopBuilderBaseDropShip=?, lvlTroopBuilderBasePowerPekka=?, lvlTroopBuilderBaseHogGlider=?, lvlTroopBuilderBaseElectrofireWizard=?, " +
                     "lvlSiegeWallWrecker=?, lvlSiegeBattleBlimp=?, lvlSiegeStoneSlammer=?, lvlSiegeSiegeBarracks=?, " +
                     "lvlSiegeLogLauncher=?, lvlSiegeFlameFlinger=?, lvlSiegeBattleDrill=?, lvlSiegeTroopLauncher=?, " +
                     "lvlPetLASSI=?, lvlPetMightyYak=?, lvlPetElectroOwl=?, lvlPetUnicorn=?, lvlPetPhoenix=?, " +
                     "lvlPetPoisonLizard=?, lvlPetDiggy=?, lvlPetFrosty=?, lvlPetSpiritFox=?, lvlPetAngryJelly=?, lvlPetSneezy=?, " +
                     "lvlHeroBarbarianKing=?, lvlHeroArcherQueen=?, lvlHeroMinionPrince=?, lvlHeroGrandWarden=?, lvlHeroRoyalChampion=?, " +
                     "lvlHeroBattleMachine=?, lvlHeroBattleCopter=?, " +
                     "lvlHeroEquipmentBarbarianPuppet=?, lvlHeroEquipmentRageVial=?, lvlHeroEquipmentEarthquakeBoots=?, lvlHeroEquipmentVampstache=?, " +
                     "lvlHeroEquipmentGiantGauntlet=?, lvlHeroEquipmentSpikyBall=?, lvlHeroEquipmentSnakeBracelet=?, lvlHeroEquipmentStickHorse=?, " +
                     "lvlHeroEquipmentArcherPuppet=?, lvlHeroEquipmentInvisibilityVial=?, lvlHeroEquipmentGiantArrow=?, lvlHeroEquipmentHealerPuppet=?, " +
                     "lvlHeroEquipmentFrozenArrow=?, lvlHeroEquipmentMagicMirror=?, lvlHeroEquipmentActionFigure=?, " +
                     "lvlHeroEquipmentHenchmenPuppet=?, lvlHeroEquipmentDarkOrb=?, lvlHeroEquipmentMetalPants=?, lvlHeroEquipmentNobleIron=?, " +
                     "lvlHeroEquipmentDarkCrown=?, lvlHeroEquipmentMeteorStaff=?, " +
                     "lvlHeroEquipmentEternalTome=?, lvlHeroEquipmentLifeGem=?, lvlHeroEquipmentRageGem=?, lvlHeroEquipmentHealingTome=?, " +
                     "lvlHeroEquipmentFireball=?, lvlHeroEquipmentLavaloonPuppet=?, lvlHeroEquipmentHeroicTorch=?, " +
                     "lvlHeroEquipmentRoyalGem=?, lvlHeroEquipmentSeekingShield=?, lvlHeroEquipmentHogRiderPuppet=?, lvlHeroEquipmentHasteVial=?, " +
                     "lvlHeroEquipmentRocketSpear=?, lvlHeroEquipmentElectroBoots=?, lvlHeroEquipmentFrostFlake=? " +
                     "WHERE playerTag=?";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            int idx = 1;
            UtilsDatabase.setNullableString(pstmt, idx++, data.name);
            pstmt.setString(idx++, currentDateTime); // lastUpdated always updates
            
            if (dataChanged) {
                pstmt.setString(idx++, currentDateTime); // lastActive only if data changed
            }
            
            UtilsDatabase.setNullableInt(pstmt, idx++, data.thLevel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bhLevel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.xpLevel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.trophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bestTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.legendTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bbTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.bestBbTrophies);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.warStars);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.attackWins);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.defenseWins);
            UtilsDatabase.setNullableString(pstmt, idx++, data.clanRole);
            UtilsDatabase.setNullableBoolean(pstmt, idx++, data.warPreference);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.donations);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.donationsReceived);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.clanCapitalContributions);
            UtilsDatabase.setNullableString(pstmt, idx++, data.legacyLeagueName);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.leagueInt);
            UtilsDatabase.setNullableString(pstmt, idx++, data.bbLeagueName);
            
            // Achievements (53)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementBiggerCoffers);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementGetThoseGoblins);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementBiggerBetter);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementNiceAndTidy);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementDiscoverNewTroops);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementGoldGrab);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementElixirEscapade);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSweetVictory);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementEmpireBuilder);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWallBuster);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHumiliator);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUnionBuster);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementConqueror);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUnbreakable);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementFriendInNeed);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMortarMauler);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHeroicHeist);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementLeagueAllStar);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementXBowExterminator);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementFirefighter);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWarHero);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementClanWarWealth);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementAntiArtillery);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSharingIsCaring);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementKeepYourAccountSafe);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMasterEngineering);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementNextGenerationModel);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUnBuildIt);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementChampionBuilder);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHighGear);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementHiddenTreasures);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementGamesChampion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementDragonSlayer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWarLeagueLegend);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementWellSeasoned);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementShatteredAndScattered);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementNotSoEasyThisTime);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementBustThis);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSuperbWork);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSiegeSharer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementAggressiveCapitalism);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMostValuableClanmate);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementCounterspell);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMonolithMasher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementUngratefulChild);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementSupercharger);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMultiArcherTowerTerminator);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementRicochetCannonCrusher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementFirespitterFinisher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementMultiGearTowerTrampler);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementCraftingConnoisseur);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementCraftersNightmare);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.achievementLeagueFollower);
            
            // Elixir Troops (18)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirBarbarian);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirArcher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirGiant);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirGoblin);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirWallBreaker);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirBalloon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirWizard);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirHealer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirPEKKA);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirBabyDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirMiner);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirElectroDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirYeti);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirDragonRider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirElectroTitan);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirRootRider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopElixirThrower);
            
            // Dark Elixir Troops (12)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirMinion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirHogRider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirValkyrie);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirGolem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirWitch);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirLavaHound);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirBowler);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirIceGolem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirHeadhunter);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirApprenticeWarden);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirDruid);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopDarkElixirFurnace);
            
            // Spells (9)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellLightning);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellHealing);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellRage);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellJump);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellFreeze);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellClone);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellInvisibility);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellRecall);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSpellRevive);
            
            // Dark Spells (7)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellPoison);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellEarthquake);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellHaste);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellSkeleton);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellBat);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellOvergrowth);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlDarkSpellIceBlock);
            
            // Builder Base Troops (12)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseRagedBarbarian);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseSneakyArcher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBoxerGiant);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBetaMinion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBomber);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseBabyDragon);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseCannonCart);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseNightWitch);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseDropShip);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBasePowerPekka);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseHogGlider);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlTroopBuilderBaseElectrofireWizard);
            
            // Siege Machines (8)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeWallWrecker);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeBattleBlimp);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeStoneSlammer);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeSiegeBarracks);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeLogLauncher);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeFlameFlinger);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeBattleDrill);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlSiegeTroopLauncher);
            
            // Pets (11)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetLASSI);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetMightyYak);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetElectroOwl);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetUnicorn);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetPhoenix);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetPoisonLizard);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetDiggy);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetFrosty);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetSpiritFox);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetAngryJelly);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlPetSneezy);
            
            // Heroes (7)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroBarbarianKing);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroArcherQueen);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroMinionPrince);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroGrandWarden);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroRoyalChampion);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroBattleMachine);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroBattleCopter);
            
            // Hero Equipment (35)
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentBarbarianPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRageVial);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentEarthquakeBoots);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentVampstache);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentGiantGauntlet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentSpikyBall);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentSnakeBracelet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentStickHorse);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentArcherPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentInvisibilityVial);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentGiantArrow);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHealerPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentFrozenArrow);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentMagicMirror);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentActionFigure);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHenchmenPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentDarkOrb);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentMetalPants);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentNobleIron);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentDarkCrown);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentMeteorStaff);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentEternalTome);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentLifeGem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRageGem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHealingTome);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentFireball);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentLavaloonPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHeroicTorch);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRoyalGem);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentSeekingShield);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHogRiderPuppet);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentHasteVial);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentRocketSpear);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentElectroBoots);
            UtilsDatabase.setNullableInt(pstmt, idx++, data.lvlHeroEquipmentFrostFlake);
            
            pstmt.setString(idx++, playerTag); // WHERE clause
            pstmt.executeUpdate();
        }
    }
    
    /**
     * Marks players who left the clan
     */
    private void markPlayersWhoLeft(Set<String> currentMembersInClan, Set<String> currentMembersInDB, String currentDateTime) throws SQLException {
        Set<String> playersWhoLeft = new HashSet<>(currentMembersInDB);
        playersWhoLeft.removeAll(currentMembersInClan);
        
        if (playersWhoLeft.isEmpty()) return;
        
        String url = UtilsDatabase.getConnectionUrl(dbName);
        String sql = "UPDATE " + TABLE_NAME + " SET dateLeft = ? WHERE playerTag = ? AND dateLeft IS NULL";
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            for (String playerTag : playersWhoLeft) {
                pstmt.setString(1, currentDateTime);
                pstmt.setString(2, playerTag);
                pstmt.addBatch();
            }
            
            pstmt.executeBatch();
            
            String msg = "Marked " + playersWhoLeft.size() + " player(s) as left";
            System.out.println(msg);
            discordLogger.logInfo(msg);
        }
    }
    
    /**
     * Main update method
     */
    public void updateDatabase(List<String> playerTags) throws SQLException, IOException, InterruptedException {
        discordLogger.logInfo("Starting database update for A02_ClanMembers, clan: " + clanTag);
        
        if (!UtilsDatabase.databaseExists(dbName)) {
            String errMsg = "Database file does not exist: " + UtilsDatabase.getDatabasePath(dbName);
            System.err.println(errMsg);
            discordLogger.logError(errMsg);
            return;
        }
        
        String currentDateTime = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC).format(java.time.format.DateTimeFormatter.ISO_INSTANT);
        
        // If no player tags provided, fetch from API
        List<String> currentMemberTags = (playerTags != null && !playerTags.isEmpty()) ? playerTags : fetchClanMemberTags();
        Set<String> currentMembersInClan = new HashSet<>(currentMemberTags);
        Set<String> currentMembersInDB = getCurrentMembersFromDB();
        
        int updates = 0;
        int inserts = 0;
        int rejoins = 0;
        
        // Process each current member
        for (String playerTag : currentMemberTags) {
            try {
                JsonObject playerJson = fetchPlayerData(playerTag);
                PlayerRecord existingRecord = getPlayerFromDB(playerTag);
                
                boolean isNewPlayer = existingRecord == null;
                boolean isRejoin = !isNewPlayer && existingRecord.dateLeft != null;
                
                PlayerData parsedData = parsePlayerData(playerJson, existingRecord);
                boolean dataChanged = hasDataChanged(parsedData, existingRecord);
                
                upsertPlayer(playerTag, parsedData, isNewPlayer, isRejoin, dataChanged, currentDateTime);
                
                if (isNewPlayer) inserts++;
                else if (isRejoin) rejoins++;
                else updates++;
                
            } catch (Exception e) {
                String errMsg = "Failed to process player " + playerTag + ": " + e.getMessage();
                System.err.println(errMsg);
                discordLogger.logError(errMsg);
                // Continue with next player
            }
        }
        
        // Mark players who left
        markPlayersWhoLeft(currentMembersInClan, currentMembersInDB, currentDateTime);
        
        String successMsg = "Database update complete: " + inserts + " new, " + updates + " updated, " + rejoins + " rejoined";
        System.out.println(successMsg);
        discordLogger.logSuccess(successMsg);
    }
    
    /**
     * Data container class for player information
     */
    private static class PlayerData {
        String name;
        Integer thLevel, bhLevel, xpLevel;
        Integer trophies, bestTrophies, legendTrophies, bbTrophies, bestBbTrophies;
        Integer warStars, attackWins, defenseWins;
        String clanRole;
        Boolean warPreference;
        Integer donations, donationsReceived, clanCapitalContributions;
        String legacyLeagueName;
        Integer leagueInt;
        String bbLeagueName;
        
        // Achievements (53 total)
        Integer achievementBiggerCoffers, achievementGetThoseGoblins, achievementBiggerBetter;
        Integer achievementNiceAndTidy, achievementDiscoverNewTroops, achievementGoldGrab;
        Integer achievementElixirEscapade, achievementSweetVictory, achievementEmpireBuilder;
        Integer achievementWallBuster, achievementHumiliator, achievementUnionBuster;
        Integer achievementConqueror, achievementUnbreakable, achievementFriendInNeed;
        Integer achievementMortarMauler, achievementHeroicHeist, achievementLeagueAllStar;
        Integer achievementXBowExterminator, achievementFirefighter, achievementWarHero;
        Integer achievementClanWarWealth, achievementAntiArtillery, achievementSharingIsCaring;
        Integer achievementKeepYourAccountSafe, achievementMasterEngineering, achievementNextGenerationModel;
        Integer achievementUnBuildIt, achievementChampionBuilder, achievementHighGear;
        Integer achievementHiddenTreasures, achievementGamesChampion, achievementDragonSlayer;
        Integer achievementWarLeagueLegend, achievementWellSeasoned, achievementShatteredAndScattered;
        Integer achievementNotSoEasyThisTime, achievementBustThis, achievementSuperbWork;
        Integer achievementSiegeSharer, achievementAggressiveCapitalism, achievementMostValuableClanmate;
        Integer achievementCounterspell, achievementMonolithMasher, achievementUngratefulChild;
        Integer achievementSupercharger, achievementMultiArcherTowerTerminator, achievementRicochetCannonCrusher;
        Integer achievementFirespitterFinisher, achievementMultiGearTowerTrampler, achievementCraftingConnoisseur;
        Integer achievementCraftersNightmare, achievementLeagueFollower;
        
        // Elixir Troops (18 total)
        Integer lvlTroopElixirBarbarian, lvlTroopElixirArcher, lvlTroopElixirGiant;
        Integer lvlTroopElixirGoblin, lvlTroopElixirWallBreaker, lvlTroopElixirBalloon;
        Integer lvlTroopElixirWizard, lvlTroopElixirHealer, lvlTroopElixirDragon;
        Integer lvlTroopElixirPEKKA, lvlTroopElixirBabyDragon, lvlTroopElixirMiner;
        Integer lvlTroopElixirElectroDragon, lvlTroopElixirYeti, lvlTroopElixirDragonRider;
        Integer lvlTroopElixirElectroTitan, lvlTroopElixirRootRider, lvlTroopElixirThrower;
        
        // Dark Elixir Troops (12 total)
        Integer lvlTroopDarkElixirMinion, lvlTroopDarkElixirHogRider, lvlTroopDarkElixirValkyrie;
        Integer lvlTroopDarkElixirGolem, lvlTroopDarkElixirWitch, lvlTroopDarkElixirLavaHound;
        Integer lvlTroopDarkElixirBowler, lvlTroopDarkElixirIceGolem, lvlTroopDarkElixirHeadhunter;
        Integer lvlTroopDarkElixirApprenticeWarden, lvlTroopDarkElixirDruid, lvlTroopDarkElixirFurnace;
        
        // Spells (9 total)
        Integer lvlSpellLightning, lvlSpellHealing, lvlSpellRage;
        Integer lvlSpellJump, lvlSpellFreeze, lvlSpellClone;
        Integer lvlSpellInvisibility, lvlSpellRecall, lvlSpellRevive;
        
        // Dark Spells (7 total)
        Integer lvlDarkSpellPoison, lvlDarkSpellEarthquake, lvlDarkSpellHaste;
        Integer lvlDarkSpellSkeleton, lvlDarkSpellBat, lvlDarkSpellOvergrowth;
        Integer lvlDarkSpellIceBlock;
        
        // Builder Base Troops (12 total)
        Integer lvlTroopBuilderBaseRagedBarbarian, lvlTroopBuilderBaseSneakyArcher, lvlTroopBuilderBaseBoxerGiant;
        Integer lvlTroopBuilderBaseBetaMinion, lvlTroopBuilderBaseBomber, lvlTroopBuilderBaseBabyDragon;
        Integer lvlTroopBuilderBaseCannonCart, lvlTroopBuilderBaseNightWitch, lvlTroopBuilderBaseDropShip;
        Integer lvlTroopBuilderBasePowerPekka, lvlTroopBuilderBaseHogGlider, lvlTroopBuilderBaseElectrofireWizard;
        
        // Siege Machines (8 total)
        Integer lvlSiegeWallWrecker, lvlSiegeBattleBlimp, lvlSiegeStoneSlammer;
        Integer lvlSiegeSiegeBarracks, lvlSiegeLogLauncher, lvlSiegeFlameFlinger;
        Integer lvlSiegeBattleDrill, lvlSiegeTroopLauncher;
        
        // Pets (11 total)
        Integer lvlPetLASSI, lvlPetMightyYak, lvlPetElectroOwl;
        Integer lvlPetUnicorn, lvlPetPhoenix, lvlPetPoisonLizard;
        Integer lvlPetDiggy, lvlPetFrosty, lvlPetSpiritFox;
        Integer lvlPetAngryJelly, lvlPetSneezy;
        
        // Heroes (7 total)
        Integer lvlHeroBarbarianKing, lvlHeroArcherQueen, lvlHeroMinionPrince;
        Integer lvlHeroGrandWarden, lvlHeroRoyalChampion;
        Integer lvlHeroBattleMachine, lvlHeroBattleCopter;
        
        // Hero Equipment - Barbarian King (8 items)
        Integer lvlHeroEquipmentBarbarianPuppet, lvlHeroEquipmentRageVial, lvlHeroEquipmentEarthquakeBoots;
        Integer lvlHeroEquipmentVampstache, lvlHeroEquipmentGiantGauntlet, lvlHeroEquipmentSpikyBall;
        Integer lvlHeroEquipmentSnakeBracelet, lvlHeroEquipmentStickHorse;
        
        // Hero Equipment - Archer Queen (7 items)
        Integer lvlHeroEquipmentArcherPuppet, lvlHeroEquipmentInvisibilityVial, lvlHeroEquipmentGiantArrow;
        Integer lvlHeroEquipmentHealerPuppet, lvlHeroEquipmentFrozenArrow, lvlHeroEquipmentMagicMirror;
        Integer lvlHeroEquipmentActionFigure;
        
        // Hero Equipment - Minion Prince (6 items)
        Integer lvlHeroEquipmentHenchmenPuppet, lvlHeroEquipmentDarkOrb, lvlHeroEquipmentMetalPants;
        Integer lvlHeroEquipmentNobleIron, lvlHeroEquipmentDarkCrown, lvlHeroEquipmentMeteorStaff;
        
        // Hero Equipment - Grand Warden (7 items)
        Integer lvlHeroEquipmentEternalTome, lvlHeroEquipmentLifeGem, lvlHeroEquipmentRageGem;
        Integer lvlHeroEquipmentHealingTome, lvlHeroEquipmentFireball, lvlHeroEquipmentLavaloonPuppet;
        Integer lvlHeroEquipmentHeroicTorch;
        
        // Hero Equipment - Royal Champion (7 items)
        Integer lvlHeroEquipmentRoyalGem, lvlHeroEquipmentSeekingShield, lvlHeroEquipmentHogRiderPuppet;
        Integer lvlHeroEquipmentHasteVial, lvlHeroEquipmentRocketSpear, lvlHeroEquipmentElectroBoots;
        Integer lvlHeroEquipmentFrostFlake;
        
        void fillMissingValues(PlayerRecord existingRecord) {
            if (name == null) name = existingRecord.name;
            if (thLevel == null) thLevel = existingRecord.thLevel;
            if (bhLevel == null) bhLevel = existingRecord.bhLevel;
            if (xpLevel == null) xpLevel = existingRecord.xpLevel;
            if (trophies == null) trophies = existingRecord.trophies;
            if (bestTrophies == null) bestTrophies = existingRecord.bestTrophies;
            if (legendTrophies == null) legendTrophies = existingRecord.legendTrophies;
            if (bbTrophies == null) bbTrophies = existingRecord.bbTrophies;
            if (bestBbTrophies == null) bestBbTrophies = existingRecord.bestBbTrophies;
            if (warStars == null) warStars = existingRecord.warStars;
            if (attackWins == null) attackWins = existingRecord.attackWins;
            if (defenseWins == null) defenseWins = existingRecord.defenseWins;
            if (clanRole == null) clanRole = existingRecord.clanRole;
            if (warPreference == null) warPreference = existingRecord.warPreference;
            if (donations == null) donations = existingRecord.donations;
            if (donationsReceived == null) donationsReceived = existingRecord.donationsReceived;
            if (clanCapitalContributions == null) clanCapitalContributions = existingRecord.clanCapitalContributions;
            if (legacyLeagueName == null) legacyLeagueName = existingRecord.legacyLeagueName;
            if (leagueInt == null) leagueInt = existingRecord.leagueInt;
            if (bbLeagueName == null) bbLeagueName = existingRecord.bbLeagueName;
            
            // Achievements
            if (achievementBiggerCoffers == null) achievementBiggerCoffers = existingRecord.achievementBiggerCoffers;
            if (achievementGetThoseGoblins == null) achievementGetThoseGoblins = existingRecord.achievementGetThoseGoblins;
            if (achievementBiggerBetter == null) achievementBiggerBetter = existingRecord.achievementBiggerBetter;
            if (achievementNiceAndTidy == null) achievementNiceAndTidy = existingRecord.achievementNiceAndTidy;
            if (achievementDiscoverNewTroops == null) achievementDiscoverNewTroops = existingRecord.achievementDiscoverNewTroops;
            if (achievementGoldGrab == null) achievementGoldGrab = existingRecord.achievementGoldGrab;
            if (achievementElixirEscapade == null) achievementElixirEscapade = existingRecord.achievementElixirEscapade;
            if (achievementSweetVictory == null) achievementSweetVictory = existingRecord.achievementSweetVictory;
            if (achievementEmpireBuilder == null) achievementEmpireBuilder = existingRecord.achievementEmpireBuilder;
            if (achievementWallBuster == null) achievementWallBuster = existingRecord.achievementWallBuster;
            if (achievementHumiliator == null) achievementHumiliator = existingRecord.achievementHumiliator;
            if (achievementUnionBuster == null) achievementUnionBuster = existingRecord.achievementUnionBuster;
            if (achievementConqueror == null) achievementConqueror = existingRecord.achievementConqueror;
            if (achievementUnbreakable == null) achievementUnbreakable = existingRecord.achievementUnbreakable;
            if (achievementFriendInNeed == null) achievementFriendInNeed = existingRecord.achievementFriendInNeed;
            if (achievementMortarMauler == null) achievementMortarMauler = existingRecord.achievementMortarMauler;
            if (achievementHeroicHeist == null) achievementHeroicHeist = existingRecord.achievementHeroicHeist;
            if (achievementLeagueAllStar == null) achievementLeagueAllStar = existingRecord.achievementLeagueAllStar;
            if (achievementXBowExterminator == null) achievementXBowExterminator = existingRecord.achievementXBowExterminator;
            if (achievementFirefighter == null) achievementFirefighter = existingRecord.achievementFirefighter;
            if (achievementWarHero == null) achievementWarHero = existingRecord.achievementWarHero;
            if (achievementClanWarWealth == null) achievementClanWarWealth = existingRecord.achievementClanWarWealth;
            if (achievementAntiArtillery == null) achievementAntiArtillery = existingRecord.achievementAntiArtillery;
            if (achievementSharingIsCaring == null) achievementSharingIsCaring = existingRecord.achievementSharingIsCaring;
            if (achievementKeepYourAccountSafe == null) achievementKeepYourAccountSafe = existingRecord.achievementKeepYourAccountSafe;
            if (achievementMasterEngineering == null) achievementMasterEngineering = existingRecord.achievementMasterEngineering;
            if (achievementNextGenerationModel == null) achievementNextGenerationModel = existingRecord.achievementNextGenerationModel;
            if (achievementUnBuildIt == null) achievementUnBuildIt = existingRecord.achievementUnBuildIt;
            if (achievementChampionBuilder == null) achievementChampionBuilder = existingRecord.achievementChampionBuilder;
            if (achievementHighGear == null) achievementHighGear = existingRecord.achievementHighGear;
            if (achievementHiddenTreasures == null) achievementHiddenTreasures = existingRecord.achievementHiddenTreasures;
            if (achievementGamesChampion == null) achievementGamesChampion = existingRecord.achievementGamesChampion;
            if (achievementDragonSlayer == null) achievementDragonSlayer = existingRecord.achievementDragonSlayer;
            if (achievementWarLeagueLegend == null) achievementWarLeagueLegend = existingRecord.achievementWarLeagueLegend;
            if (achievementWellSeasoned == null) achievementWellSeasoned = existingRecord.achievementWellSeasoned;
            if (achievementShatteredAndScattered == null) achievementShatteredAndScattered = existingRecord.achievementShatteredAndScattered;
            if (achievementNotSoEasyThisTime == null) achievementNotSoEasyThisTime = existingRecord.achievementNotSoEasyThisTime;
            if (achievementBustThis == null) achievementBustThis = existingRecord.achievementBustThis;
            if (achievementSuperbWork == null) achievementSuperbWork = existingRecord.achievementSuperbWork;
            if (achievementSiegeSharer == null) achievementSiegeSharer = existingRecord.achievementSiegeSharer;
            if (achievementAggressiveCapitalism == null) achievementAggressiveCapitalism = existingRecord.achievementAggressiveCapitalism;
            if (achievementMostValuableClanmate == null) achievementMostValuableClanmate = existingRecord.achievementMostValuableClanmate;
            if (achievementCounterspell == null) achievementCounterspell = existingRecord.achievementCounterspell;
            if (achievementMonolithMasher == null) achievementMonolithMasher = existingRecord.achievementMonolithMasher;
            if (achievementUngratefulChild == null) achievementUngratefulChild = existingRecord.achievementUngratefulChild;
            if (achievementSupercharger == null) achievementSupercharger = existingRecord.achievementSupercharger;
            if (achievementMultiArcherTowerTerminator == null) achievementMultiArcherTowerTerminator = existingRecord.achievementMultiArcherTowerTerminator;
            if (achievementRicochetCannonCrusher == null) achievementRicochetCannonCrusher = existingRecord.achievementRicochetCannonCrusher;
            if (achievementFirespitterFinisher == null) achievementFirespitterFinisher = existingRecord.achievementFirespitterFinisher;
            if (achievementMultiGearTowerTrampler == null) achievementMultiGearTowerTrampler = existingRecord.achievementMultiGearTowerTrampler;
            if (achievementCraftingConnoisseur == null) achievementCraftingConnoisseur = existingRecord.achievementCraftingConnoisseur;
            if (achievementCraftersNightmare == null) achievementCraftersNightmare = existingRecord.achievementCraftersNightmare;
            if (achievementLeagueFollower == null) achievementLeagueFollower = existingRecord.achievementLeagueFollower;
            
            // Elixir Troops
            if (lvlTroopElixirBarbarian == null) lvlTroopElixirBarbarian = existingRecord.lvlTroopElixirBarbarian;
            if (lvlTroopElixirArcher == null) lvlTroopElixirArcher = existingRecord.lvlTroopElixirArcher;
            if (lvlTroopElixirGiant == null) lvlTroopElixirGiant = existingRecord.lvlTroopElixirGiant;
            if (lvlTroopElixirGoblin == null) lvlTroopElixirGoblin = existingRecord.lvlTroopElixirGoblin;
            if (lvlTroopElixirWallBreaker == null) lvlTroopElixirWallBreaker = existingRecord.lvlTroopElixirWallBreaker;
            if (lvlTroopElixirBalloon == null) lvlTroopElixirBalloon = existingRecord.lvlTroopElixirBalloon;
            if (lvlTroopElixirWizard == null) lvlTroopElixirWizard = existingRecord.lvlTroopElixirWizard;
            if (lvlTroopElixirHealer == null) lvlTroopElixirHealer = existingRecord.lvlTroopElixirHealer;
            if (lvlTroopElixirDragon == null) lvlTroopElixirDragon = existingRecord.lvlTroopElixirDragon;
            if (lvlTroopElixirPEKKA == null) lvlTroopElixirPEKKA = existingRecord.lvlTroopElixirPEKKA;
            if (lvlTroopElixirBabyDragon == null) lvlTroopElixirBabyDragon = existingRecord.lvlTroopElixirBabyDragon;
            if (lvlTroopElixirMiner == null) lvlTroopElixirMiner = existingRecord.lvlTroopElixirMiner;
            if (lvlTroopElixirElectroDragon == null) lvlTroopElixirElectroDragon = existingRecord.lvlTroopElixirElectroDragon;
            if (lvlTroopElixirYeti == null) lvlTroopElixirYeti = existingRecord.lvlTroopElixirYeti;
            if (lvlTroopElixirDragonRider == null) lvlTroopElixirDragonRider = existingRecord.lvlTroopElixirDragonRider;
            if (lvlTroopElixirElectroTitan == null) lvlTroopElixirElectroTitan = existingRecord.lvlTroopElixirElectroTitan;
            if (lvlTroopElixirRootRider == null) lvlTroopElixirRootRider = existingRecord.lvlTroopElixirRootRider;
            if (lvlTroopElixirThrower == null) lvlTroopElixirThrower = existingRecord.lvlTroopElixirThrower;
            
            // Dark Elixir Troops
            if (lvlTroopDarkElixirMinion == null) lvlTroopDarkElixirMinion = existingRecord.lvlTroopDarkElixirMinion;
            if (lvlTroopDarkElixirHogRider == null) lvlTroopDarkElixirHogRider = existingRecord.lvlTroopDarkElixirHogRider;
            if (lvlTroopDarkElixirValkyrie == null) lvlTroopDarkElixirValkyrie = existingRecord.lvlTroopDarkElixirValkyrie;
            if (lvlTroopDarkElixirGolem == null) lvlTroopDarkElixirGolem = existingRecord.lvlTroopDarkElixirGolem;
            if (lvlTroopDarkElixirWitch == null) lvlTroopDarkElixirWitch = existingRecord.lvlTroopDarkElixirWitch;
            if (lvlTroopDarkElixirLavaHound == null) lvlTroopDarkElixirLavaHound = existingRecord.lvlTroopDarkElixirLavaHound;
            if (lvlTroopDarkElixirBowler == null) lvlTroopDarkElixirBowler = existingRecord.lvlTroopDarkElixirBowler;
            if (lvlTroopDarkElixirIceGolem == null) lvlTroopDarkElixirIceGolem = existingRecord.lvlTroopDarkElixirIceGolem;
            if (lvlTroopDarkElixirHeadhunter == null) lvlTroopDarkElixirHeadhunter = existingRecord.lvlTroopDarkElixirHeadhunter;
            if (lvlTroopDarkElixirApprenticeWarden == null) lvlTroopDarkElixirApprenticeWarden = existingRecord.lvlTroopDarkElixirApprenticeWarden;
            if (lvlTroopDarkElixirDruid == null) lvlTroopDarkElixirDruid = existingRecord.lvlTroopDarkElixirDruid;
            if (lvlTroopDarkElixirFurnace == null) lvlTroopDarkElixirFurnace = existingRecord.lvlTroopDarkElixirFurnace;
            
            // Spells
            if (lvlSpellLightning == null) lvlSpellLightning = existingRecord.lvlSpellLightning;
            if (lvlSpellHealing == null) lvlSpellHealing = existingRecord.lvlSpellHealing;
            if (lvlSpellRage == null) lvlSpellRage = existingRecord.lvlSpellRage;
            if (lvlSpellJump == null) lvlSpellJump = existingRecord.lvlSpellJump;
            if (lvlSpellFreeze == null) lvlSpellFreeze = existingRecord.lvlSpellFreeze;
            if (lvlSpellClone == null) lvlSpellClone = existingRecord.lvlSpellClone;
            if (lvlSpellInvisibility == null) lvlSpellInvisibility = existingRecord.lvlSpellInvisibility;
            if (lvlSpellRecall == null) lvlSpellRecall = existingRecord.lvlSpellRecall;
            if (lvlSpellRevive == null) lvlSpellRevive = existingRecord.lvlSpellRevive;
            
            // Dark Spells
            if (lvlDarkSpellPoison == null) lvlDarkSpellPoison = existingRecord.lvlDarkSpellPoison;
            if (lvlDarkSpellEarthquake == null) lvlDarkSpellEarthquake = existingRecord.lvlDarkSpellEarthquake;
            if (lvlDarkSpellHaste == null) lvlDarkSpellHaste = existingRecord.lvlDarkSpellHaste;
            if (lvlDarkSpellSkeleton == null) lvlDarkSpellSkeleton = existingRecord.lvlDarkSpellSkeleton;
            if (lvlDarkSpellBat == null) lvlDarkSpellBat = existingRecord.lvlDarkSpellBat;
            if (lvlDarkSpellOvergrowth == null) lvlDarkSpellOvergrowth = existingRecord.lvlDarkSpellOvergrowth;
            if (lvlDarkSpellIceBlock == null) lvlDarkSpellIceBlock = existingRecord.lvlDarkSpellIceBlock;
            
            // Builder Base Troops
            if (lvlTroopBuilderBaseRagedBarbarian == null) lvlTroopBuilderBaseRagedBarbarian = existingRecord.lvlTroopBuilderBaseRagedBarbarian;
            if (lvlTroopBuilderBaseSneakyArcher == null) lvlTroopBuilderBaseSneakyArcher = existingRecord.lvlTroopBuilderBaseSneakyArcher;
            if (lvlTroopBuilderBaseBoxerGiant == null) lvlTroopBuilderBaseBoxerGiant = existingRecord.lvlTroopBuilderBaseBoxerGiant;
            if (lvlTroopBuilderBaseBetaMinion == null) lvlTroopBuilderBaseBetaMinion = existingRecord.lvlTroopBuilderBaseBetaMinion;
            if (lvlTroopBuilderBaseBomber == null) lvlTroopBuilderBaseBomber = existingRecord.lvlTroopBuilderBaseBomber;
            if (lvlTroopBuilderBaseBabyDragon == null) lvlTroopBuilderBaseBabyDragon = existingRecord.lvlTroopBuilderBaseBabyDragon;
            if (lvlTroopBuilderBaseCannonCart == null) lvlTroopBuilderBaseCannonCart = existingRecord.lvlTroopBuilderBaseCannonCart;
            if (lvlTroopBuilderBaseNightWitch == null) lvlTroopBuilderBaseNightWitch = existingRecord.lvlTroopBuilderBaseNightWitch;
            if (lvlTroopBuilderBaseDropShip == null) lvlTroopBuilderBaseDropShip = existingRecord.lvlTroopBuilderBaseDropShip;
            if (lvlTroopBuilderBasePowerPekka == null) lvlTroopBuilderBasePowerPekka = existingRecord.lvlTroopBuilderBasePowerPekka;
            if (lvlTroopBuilderBaseHogGlider == null) lvlTroopBuilderBaseHogGlider = existingRecord.lvlTroopBuilderBaseHogGlider;
            if (lvlTroopBuilderBaseElectrofireWizard == null) lvlTroopBuilderBaseElectrofireWizard = existingRecord.lvlTroopBuilderBaseElectrofireWizard;
            
            // Siege Machines
            if (lvlSiegeWallWrecker == null) lvlSiegeWallWrecker = existingRecord.lvlSiegeWallWrecker;
            if (lvlSiegeBattleBlimp == null) lvlSiegeBattleBlimp = existingRecord.lvlSiegeBattleBlimp;
            if (lvlSiegeStoneSlammer == null) lvlSiegeStoneSlammer = existingRecord.lvlSiegeStoneSlammer;
            if (lvlSiegeSiegeBarracks == null) lvlSiegeSiegeBarracks = existingRecord.lvlSiegeSiegeBarracks;
            if (lvlSiegeLogLauncher == null) lvlSiegeLogLauncher = existingRecord.lvlSiegeLogLauncher;
            if (lvlSiegeFlameFlinger == null) lvlSiegeFlameFlinger = existingRecord.lvlSiegeFlameFlinger;
            if (lvlSiegeBattleDrill == null) lvlSiegeBattleDrill = existingRecord.lvlSiegeBattleDrill;
            if (lvlSiegeTroopLauncher == null) lvlSiegeTroopLauncher = existingRecord.lvlSiegeTroopLauncher;
            
            // Pets
            if (lvlPetLASSI == null) lvlPetLASSI = existingRecord.lvlPetLASSI;
            if (lvlPetMightyYak == null) lvlPetMightyYak = existingRecord.lvlPetMightyYak;
            if (lvlPetElectroOwl == null) lvlPetElectroOwl = existingRecord.lvlPetElectroOwl;
            if (lvlPetUnicorn == null) lvlPetUnicorn = existingRecord.lvlPetUnicorn;
            if (lvlPetPhoenix == null) lvlPetPhoenix = existingRecord.lvlPetPhoenix;
            if (lvlPetPoisonLizard == null) lvlPetPoisonLizard = existingRecord.lvlPetPoisonLizard;
            if (lvlPetDiggy == null) lvlPetDiggy = existingRecord.lvlPetDiggy;
            if (lvlPetFrosty == null) lvlPetFrosty = existingRecord.lvlPetFrosty;
            if (lvlPetSpiritFox == null) lvlPetSpiritFox = existingRecord.lvlPetSpiritFox;
            if (lvlPetAngryJelly == null) lvlPetAngryJelly = existingRecord.lvlPetAngryJelly;
            if (lvlPetSneezy == null) lvlPetSneezy = existingRecord.lvlPetSneezy;
            
            // Heroes
            if (lvlHeroBarbarianKing == null) lvlHeroBarbarianKing = existingRecord.lvlHeroBarbarianKing;
            if (lvlHeroArcherQueen == null) lvlHeroArcherQueen = existingRecord.lvlHeroArcherQueen;
            if (lvlHeroMinionPrince == null) lvlHeroMinionPrince = existingRecord.lvlHeroMinionPrince;
            if (lvlHeroGrandWarden == null) lvlHeroGrandWarden = existingRecord.lvlHeroGrandWarden;
            if (lvlHeroRoyalChampion == null) lvlHeroRoyalChampion = existingRecord.lvlHeroRoyalChampion;
            if (lvlHeroBattleMachine == null) lvlHeroBattleMachine = existingRecord.lvlHeroBattleMachine;
            if (lvlHeroBattleCopter == null) lvlHeroBattleCopter = existingRecord.lvlHeroBattleCopter;
            
            // Hero Equipment - Barbarian King
            if (lvlHeroEquipmentBarbarianPuppet == null) lvlHeroEquipmentBarbarianPuppet = existingRecord.lvlHeroEquipmentBarbarianPuppet;
            if (lvlHeroEquipmentRageVial == null) lvlHeroEquipmentRageVial = existingRecord.lvlHeroEquipmentRageVial;
            if (lvlHeroEquipmentEarthquakeBoots == null) lvlHeroEquipmentEarthquakeBoots = existingRecord.lvlHeroEquipmentEarthquakeBoots;
            if (lvlHeroEquipmentVampstache == null) lvlHeroEquipmentVampstache = existingRecord.lvlHeroEquipmentVampstache;
            if (lvlHeroEquipmentGiantGauntlet == null) lvlHeroEquipmentGiantGauntlet = existingRecord.lvlHeroEquipmentGiantGauntlet;
            if (lvlHeroEquipmentSpikyBall == null) lvlHeroEquipmentSpikyBall = existingRecord.lvlHeroEquipmentSpikyBall;
            if (lvlHeroEquipmentSnakeBracelet == null) lvlHeroEquipmentSnakeBracelet = existingRecord.lvlHeroEquipmentSnakeBracelet;
            if (lvlHeroEquipmentStickHorse == null) lvlHeroEquipmentStickHorse = existingRecord.lvlHeroEquipmentStickHorse;
            
            // Hero Equipment - Archer Queen
            if (lvlHeroEquipmentArcherPuppet == null) lvlHeroEquipmentArcherPuppet = existingRecord.lvlHeroEquipmentArcherPuppet;
            if (lvlHeroEquipmentInvisibilityVial == null) lvlHeroEquipmentInvisibilityVial = existingRecord.lvlHeroEquipmentInvisibilityVial;
            if (lvlHeroEquipmentGiantArrow == null) lvlHeroEquipmentGiantArrow = existingRecord.lvlHeroEquipmentGiantArrow;
            if (lvlHeroEquipmentHealerPuppet == null) lvlHeroEquipmentHealerPuppet = existingRecord.lvlHeroEquipmentHealerPuppet;
            if (lvlHeroEquipmentFrozenArrow == null) lvlHeroEquipmentFrozenArrow = existingRecord.lvlHeroEquipmentFrozenArrow;
            if (lvlHeroEquipmentMagicMirror == null) lvlHeroEquipmentMagicMirror = existingRecord.lvlHeroEquipmentMagicMirror;
            if (lvlHeroEquipmentActionFigure == null) lvlHeroEquipmentActionFigure = existingRecord.lvlHeroEquipmentActionFigure;
            
            // Hero Equipment - Minion Prince
            if (lvlHeroEquipmentHenchmenPuppet == null) lvlHeroEquipmentHenchmenPuppet = existingRecord.lvlHeroEquipmentHenchmenPuppet;
            if (lvlHeroEquipmentDarkOrb == null) lvlHeroEquipmentDarkOrb = existingRecord.lvlHeroEquipmentDarkOrb;
            if (lvlHeroEquipmentMetalPants == null) lvlHeroEquipmentMetalPants = existingRecord.lvlHeroEquipmentMetalPants;
            if (lvlHeroEquipmentNobleIron == null) lvlHeroEquipmentNobleIron = existingRecord.lvlHeroEquipmentNobleIron;
            if (lvlHeroEquipmentDarkCrown == null) lvlHeroEquipmentDarkCrown = existingRecord.lvlHeroEquipmentDarkCrown;
            if (lvlHeroEquipmentMeteorStaff == null) lvlHeroEquipmentMeteorStaff = existingRecord.lvlHeroEquipmentMeteorStaff;
            
            // Hero Equipment - Grand Warden
            if (lvlHeroEquipmentEternalTome == null) lvlHeroEquipmentEternalTome = existingRecord.lvlHeroEquipmentEternalTome;
            if (lvlHeroEquipmentLifeGem == null) lvlHeroEquipmentLifeGem = existingRecord.lvlHeroEquipmentLifeGem;
            if (lvlHeroEquipmentRageGem == null) lvlHeroEquipmentRageGem = existingRecord.lvlHeroEquipmentRageGem;
            if (lvlHeroEquipmentHealingTome == null) lvlHeroEquipmentHealingTome = existingRecord.lvlHeroEquipmentHealingTome;
            if (lvlHeroEquipmentFireball == null) lvlHeroEquipmentFireball = existingRecord.lvlHeroEquipmentFireball;
            if (lvlHeroEquipmentLavaloonPuppet == null) lvlHeroEquipmentLavaloonPuppet = existingRecord.lvlHeroEquipmentLavaloonPuppet;
            if (lvlHeroEquipmentHeroicTorch == null) lvlHeroEquipmentHeroicTorch = existingRecord.lvlHeroEquipmentHeroicTorch;
            
            // Hero Equipment - Royal Champion
            if (lvlHeroEquipmentRoyalGem == null) lvlHeroEquipmentRoyalGem = existingRecord.lvlHeroEquipmentRoyalGem;
            if (lvlHeroEquipmentSeekingShield == null) lvlHeroEquipmentSeekingShield = existingRecord.lvlHeroEquipmentSeekingShield;
            if (lvlHeroEquipmentHogRiderPuppet == null) lvlHeroEquipmentHogRiderPuppet = existingRecord.lvlHeroEquipmentHogRiderPuppet;
            if (lvlHeroEquipmentHasteVial == null) lvlHeroEquipmentHasteVial = existingRecord.lvlHeroEquipmentHasteVial;
            if (lvlHeroEquipmentRocketSpear == null) lvlHeroEquipmentRocketSpear = existingRecord.lvlHeroEquipmentRocketSpear;
            if (lvlHeroEquipmentElectroBoots == null) lvlHeroEquipmentElectroBoots = existingRecord.lvlHeroEquipmentElectroBoots;
            if (lvlHeroEquipmentFrostFlake == null) lvlHeroEquipmentFrostFlake = existingRecord.lvlHeroEquipmentFrostFlake;
        }
    }
    
    /**
     * Database record class for existing player data
     */
    private static class PlayerRecord {
        String playerTag, name;
        String lastUpdated, dateJoin, dateLeft, lastActive;
        Integer thLevel, bhLevel, xpLevel;
        Integer trophies, bestTrophies, legendTrophies, bbTrophies, bestBbTrophies;
        Integer warStars, attackWins, defenseWins;
        String clanRole;
        Boolean warPreference;
        Integer donations, donationsReceived, clanCapitalContributions;
        String legacyLeagueName;
        Integer leagueInt;
        String bbLeagueName;
        
        // Achievements (53 total)
        Integer achievementBiggerCoffers, achievementGetThoseGoblins, achievementBiggerBetter;
        Integer achievementNiceAndTidy, achievementDiscoverNewTroops, achievementGoldGrab;
        Integer achievementElixirEscapade, achievementSweetVictory, achievementEmpireBuilder;
        Integer achievementWallBuster, achievementHumiliator, achievementUnionBuster;
        Integer achievementConqueror, achievementUnbreakable, achievementFriendInNeed;
        Integer achievementMortarMauler, achievementHeroicHeist, achievementLeagueAllStar;
        Integer achievementXBowExterminator, achievementFirefighter, achievementWarHero;
        Integer achievementClanWarWealth, achievementAntiArtillery, achievementSharingIsCaring;
        Integer achievementKeepYourAccountSafe, achievementMasterEngineering, achievementNextGenerationModel;
        Integer achievementUnBuildIt, achievementChampionBuilder, achievementHighGear;
        Integer achievementHiddenTreasures, achievementGamesChampion, achievementDragonSlayer;
        Integer achievementWarLeagueLegend, achievementWellSeasoned, achievementShatteredAndScattered;
        Integer achievementNotSoEasyThisTime, achievementBustThis, achievementSuperbWork;
        Integer achievementSiegeSharer, achievementAggressiveCapitalism, achievementMostValuableClanmate;
        Integer achievementCounterspell, achievementMonolithMasher, achievementUngratefulChild;
        Integer achievementSupercharger, achievementMultiArcherTowerTerminator, achievementRicochetCannonCrusher;
        Integer achievementFirespitterFinisher, achievementMultiGearTowerTrampler, achievementCraftingConnoisseur;
        Integer achievementCraftersNightmare, achievementLeagueFollower;
        
        // Elixir Troops (18 total)
        Integer lvlTroopElixirBarbarian, lvlTroopElixirArcher, lvlTroopElixirGiant;
        Integer lvlTroopElixirGoblin, lvlTroopElixirWallBreaker, lvlTroopElixirBalloon;
        Integer lvlTroopElixirWizard, lvlTroopElixirHealer, lvlTroopElixirDragon;
        Integer lvlTroopElixirPEKKA, lvlTroopElixirBabyDragon, lvlTroopElixirMiner;
        Integer lvlTroopElixirElectroDragon, lvlTroopElixirYeti, lvlTroopElixirDragonRider;
        Integer lvlTroopElixirElectroTitan, lvlTroopElixirRootRider, lvlTroopElixirThrower;
        
        // Dark Elixir Troops (12 total)
        Integer lvlTroopDarkElixirMinion, lvlTroopDarkElixirHogRider, lvlTroopDarkElixirValkyrie;
        Integer lvlTroopDarkElixirGolem, lvlTroopDarkElixirWitch, lvlTroopDarkElixirLavaHound;
        Integer lvlTroopDarkElixirBowler, lvlTroopDarkElixirIceGolem, lvlTroopDarkElixirHeadhunter;
        Integer lvlTroopDarkElixirApprenticeWarden, lvlTroopDarkElixirDruid, lvlTroopDarkElixirFurnace;
        
        // Spells (9 total)
        Integer lvlSpellLightning, lvlSpellHealing, lvlSpellRage;
        Integer lvlSpellJump, lvlSpellFreeze, lvlSpellClone;
        Integer lvlSpellInvisibility, lvlSpellRecall, lvlSpellRevive;
        
        // Dark Spells (7 total)
        Integer lvlDarkSpellPoison, lvlDarkSpellEarthquake, lvlDarkSpellHaste;
        Integer lvlDarkSpellSkeleton, lvlDarkSpellBat, lvlDarkSpellOvergrowth;
        Integer lvlDarkSpellIceBlock;
        
        // Builder Base Troops (12 total)
        Integer lvlTroopBuilderBaseRagedBarbarian, lvlTroopBuilderBaseSneakyArcher, lvlTroopBuilderBaseBoxerGiant;
        Integer lvlTroopBuilderBaseBetaMinion, lvlTroopBuilderBaseBomber, lvlTroopBuilderBaseBabyDragon;
        Integer lvlTroopBuilderBaseCannonCart, lvlTroopBuilderBaseNightWitch, lvlTroopBuilderBaseDropShip;
        Integer lvlTroopBuilderBasePowerPekka, lvlTroopBuilderBaseHogGlider, lvlTroopBuilderBaseElectrofireWizard;
        
        // Siege Machines (8 total)
        Integer lvlSiegeWallWrecker, lvlSiegeBattleBlimp, lvlSiegeStoneSlammer;
        Integer lvlSiegeSiegeBarracks, lvlSiegeLogLauncher, lvlSiegeFlameFlinger;
        Integer lvlSiegeBattleDrill, lvlSiegeTroopLauncher;
        
        // Pets (11 total)
        Integer lvlPetLASSI, lvlPetMightyYak, lvlPetElectroOwl;
        Integer lvlPetUnicorn, lvlPetPhoenix, lvlPetPoisonLizard;
        Integer lvlPetDiggy, lvlPetFrosty, lvlPetSpiritFox;
        Integer lvlPetAngryJelly, lvlPetSneezy;
        
        // Heroes (7 total)
        Integer lvlHeroBarbarianKing, lvlHeroArcherQueen, lvlHeroMinionPrince;
        Integer lvlHeroGrandWarden, lvlHeroRoyalChampion;
        Integer lvlHeroBattleMachine, lvlHeroBattleCopter;
        
        // Hero Equipment - Barbarian King (8 items)
        Integer lvlHeroEquipmentBarbarianPuppet, lvlHeroEquipmentRageVial, lvlHeroEquipmentEarthquakeBoots;
        Integer lvlHeroEquipmentVampstache, lvlHeroEquipmentGiantGauntlet, lvlHeroEquipmentSpikyBall;
        Integer lvlHeroEquipmentSnakeBracelet, lvlHeroEquipmentStickHorse;
        
        // Hero Equipment - Archer Queen (7 items)
        Integer lvlHeroEquipmentArcherPuppet, lvlHeroEquipmentInvisibilityVial, lvlHeroEquipmentGiantArrow;
        Integer lvlHeroEquipmentHealerPuppet, lvlHeroEquipmentFrozenArrow, lvlHeroEquipmentMagicMirror;
        Integer lvlHeroEquipmentActionFigure;
        
        // Hero Equipment - Minion Prince (6 items)
        Integer lvlHeroEquipmentHenchmenPuppet, lvlHeroEquipmentDarkOrb, lvlHeroEquipmentMetalPants;
        Integer lvlHeroEquipmentNobleIron, lvlHeroEquipmentDarkCrown, lvlHeroEquipmentMeteorStaff;
        
        // Hero Equipment - Grand Warden (7 items)
        Integer lvlHeroEquipmentEternalTome, lvlHeroEquipmentLifeGem, lvlHeroEquipmentRageGem;
        Integer lvlHeroEquipmentHealingTome, lvlHeroEquipmentFireball, lvlHeroEquipmentLavaloonPuppet;
        Integer lvlHeroEquipmentHeroicTorch;
        
        // Hero Equipment - Royal Champion (7 items)
        Integer lvlHeroEquipmentRoyalGem, lvlHeroEquipmentSeekingShield, lvlHeroEquipmentHogRiderPuppet;
        Integer lvlHeroEquipmentHasteVial, lvlHeroEquipmentRocketSpear, lvlHeroEquipmentElectroBoots;
        Integer lvlHeroEquipmentFrostFlake;
        
        PlayerRecord(ResultSet rs) throws SQLException {
            this.playerTag = rs.getString("playerTag");
            this.name = rs.getString("name");
            this.lastUpdated = rs.getString("lastUpdated");
            this.dateJoin = rs.getString("dateJoin");
            this.dateLeft = rs.getString("dateLeft");
            this.lastActive = rs.getString("lastActive");
            this.thLevel = (Integer) rs.getObject("thLevel");
            this.bhLevel = (Integer) rs.getObject("bhLevel");
            this.xpLevel = (Integer) rs.getObject("xpLevel");
            this.trophies = (Integer) rs.getObject("trophies");
            this.bestTrophies = (Integer) rs.getObject("bestTrophies");
            this.legendTrophies = (Integer) rs.getObject("legendTrophies");
            this.bbTrophies = (Integer) rs.getObject("bbTrophies");
            this.bestBbTrophies = (Integer) rs.getObject("bestBbTrophies");
            this.warStars = (Integer) rs.getObject("warStars");
            this.attackWins = (Integer) rs.getObject("attackWins");
            this.defenseWins = (Integer) rs.getObject("defenseWins");
            this.clanRole = rs.getString("clanRole");
            this.warPreference = (Boolean) rs.getObject("warPreference");
            this.donations = (Integer) rs.getObject("donations");
            this.donationsReceived = (Integer) rs.getObject("donationsReceived");
            this.clanCapitalContributions = (Integer) rs.getObject("clanCapitalContributions");
            this.legacyLeagueName = rs.getString("legacyLeagueName");
            this.leagueInt = (Integer) rs.getObject("leagueInt");
            this.bbLeagueName = rs.getString("bbLeagueName");
            
            // Achievements
            this.achievementBiggerCoffers = (Integer) rs.getObject("achievementBiggerCoffers");
            this.achievementGetThoseGoblins = (Integer) rs.getObject("achievementGetThoseGoblins");
            this.achievementBiggerBetter = (Integer) rs.getObject("achievementBiggerBetter");
            this.achievementNiceAndTidy = (Integer) rs.getObject("achievementNiceAndTidy");
            this.achievementDiscoverNewTroops = (Integer) rs.getObject("achievementDiscoverNewTroops");
            this.achievementGoldGrab = (Integer) rs.getObject("achievementGoldGrab");
            this.achievementElixirEscapade = (Integer) rs.getObject("achievementElixirEscapade");
            this.achievementSweetVictory = (Integer) rs.getObject("achievementSweetVictory");
            this.achievementEmpireBuilder = (Integer) rs.getObject("achievementEmpireBuilder");
            this.achievementWallBuster = (Integer) rs.getObject("achievementWallBuster");
            this.achievementHumiliator = (Integer) rs.getObject("achievementHumiliator");
            this.achievementUnionBuster = (Integer) rs.getObject("achievementUnionBuster");
            this.achievementConqueror = (Integer) rs.getObject("achievementConqueror");
            this.achievementUnbreakable = (Integer) rs.getObject("achievementUnbreakable");
            this.achievementFriendInNeed = (Integer) rs.getObject("achievementFriendInNeed");
            this.achievementMortarMauler = (Integer) rs.getObject("achievementMortarMauler");
            this.achievementHeroicHeist = (Integer) rs.getObject("achievementHeroicHeist");
            this.achievementLeagueAllStar = (Integer) rs.getObject("achievementLeagueAllStar");
            this.achievementXBowExterminator = (Integer) rs.getObject("achievementXBowExterminator");
            this.achievementFirefighter = (Integer) rs.getObject("achievementFirefighter");
            this.achievementWarHero = (Integer) rs.getObject("achievementWarHero");
            this.achievementClanWarWealth = (Integer) rs.getObject("achievementClanWarWealth");
            this.achievementAntiArtillery = (Integer) rs.getObject("achievementAntiArtillery");
            this.achievementSharingIsCaring = (Integer) rs.getObject("achievementSharingIsCaring");
            this.achievementKeepYourAccountSafe = (Integer) rs.getObject("achievementKeepYourAccountSafe");
            this.achievementMasterEngineering = (Integer) rs.getObject("achievementMasterEngineering");
            this.achievementNextGenerationModel = (Integer) rs.getObject("achievementNextGenerationModel");
            this.achievementUnBuildIt = (Integer) rs.getObject("achievementUnBuildIt");
            this.achievementChampionBuilder = (Integer) rs.getObject("achievementChampionBuilder");
            this.achievementHighGear = (Integer) rs.getObject("achievementHighGear");
            this.achievementHiddenTreasures = (Integer) rs.getObject("achievementHiddenTreasures");
            this.achievementGamesChampion = (Integer) rs.getObject("achievementGamesChampion");
            this.achievementDragonSlayer = (Integer) rs.getObject("achievementDragonSlayer");
            this.achievementWarLeagueLegend = (Integer) rs.getObject("achievementWarLeagueLegend");
            this.achievementWellSeasoned = (Integer) rs.getObject("achievementWellSeasoned");
            this.achievementShatteredAndScattered = (Integer) rs.getObject("achievementShatteredAndScattered");
            this.achievementNotSoEasyThisTime = (Integer) rs.getObject("achievementNotSoEasyThisTime");
            this.achievementBustThis = (Integer) rs.getObject("achievementBustThis");
            this.achievementSuperbWork = (Integer) rs.getObject("achievementSuperbWork");
            this.achievementSiegeSharer = (Integer) rs.getObject("achievementSiegeSharer");
            this.achievementAggressiveCapitalism = (Integer) rs.getObject("achievementAggressiveCapitalism");
            this.achievementMostValuableClanmate = (Integer) rs.getObject("achievementMostValuableClanmate");
            this.achievementCounterspell = (Integer) rs.getObject("achievementCounterspell");
            this.achievementMonolithMasher = (Integer) rs.getObject("achievementMonolithMasher");
            this.achievementUngratefulChild = (Integer) rs.getObject("achievementUngratefulChild");
            this.achievementSupercharger = (Integer) rs.getObject("achievementSupercharger");
            this.achievementMultiArcherTowerTerminator = (Integer) rs.getObject("achievementMultiArcherTowerTerminator");
            this.achievementRicochetCannonCrusher = (Integer) rs.getObject("achievementRicochetCannonCrusher");
            this.achievementFirespitterFinisher = (Integer) rs.getObject("achievementFirespitterFinisher");
            this.achievementMultiGearTowerTrampler = (Integer) rs.getObject("achievementMultiGearTowerTrampler");
            this.achievementCraftingConnoisseur = (Integer) rs.getObject("achievementCraftingConnoisseur");
            this.achievementCraftersNightmare = (Integer) rs.getObject("achievementCraftersNightmare");
            this.achievementLeagueFollower = (Integer) rs.getObject("achievementLeagueFollower");
            
            // Elixir Troops
            this.lvlTroopElixirBarbarian = (Integer) rs.getObject("lvlTroopElixirBarbarian");
            this.lvlTroopElixirArcher = (Integer) rs.getObject("lvlTroopElixirArcher");
            this.lvlTroopElixirGiant = (Integer) rs.getObject("lvlTroopElixirGiant");
            this.lvlTroopElixirGoblin = (Integer) rs.getObject("lvlTroopElixirGoblin");
            this.lvlTroopElixirWallBreaker = (Integer) rs.getObject("lvlTroopElixirWallBreaker");
            this.lvlTroopElixirBalloon = (Integer) rs.getObject("lvlTroopElixirBalloon");
            this.lvlTroopElixirWizard = (Integer) rs.getObject("lvlTroopElixirWizard");
            this.lvlTroopElixirHealer = (Integer) rs.getObject("lvlTroopElixirHealer");
            this.lvlTroopElixirDragon = (Integer) rs.getObject("lvlTroopElixirDragon");
            this.lvlTroopElixirPEKKA = (Integer) rs.getObject("lvlTroopElixirPEKKA");
            this.lvlTroopElixirBabyDragon = (Integer) rs.getObject("lvlTroopElixirBabyDragon");
            this.lvlTroopElixirMiner = (Integer) rs.getObject("lvlTroopElixirMiner");
            this.lvlTroopElixirElectroDragon = (Integer) rs.getObject("lvlTroopElixirElectroDragon");
            this.lvlTroopElixirYeti = (Integer) rs.getObject("lvlTroopElixirYeti");
            this.lvlTroopElixirDragonRider = (Integer) rs.getObject("lvlTroopElixirDragonRider");
            this.lvlTroopElixirElectroTitan = (Integer) rs.getObject("lvlTroopElixirElectroTitan");
            this.lvlTroopElixirRootRider = (Integer) rs.getObject("lvlTroopElixirRootRider");
            this.lvlTroopElixirThrower = (Integer) rs.getObject("lvlTroopElixirThrower");
            
            // Dark Elixir Troops
            this.lvlTroopDarkElixirMinion = (Integer) rs.getObject("lvlTroopDarkElixirMinion");
            this.lvlTroopDarkElixirHogRider = (Integer) rs.getObject("lvlTroopDarkElixirHogRider");
            this.lvlTroopDarkElixirValkyrie = (Integer) rs.getObject("lvlTroopDarkElixirValkyrie");
            this.lvlTroopDarkElixirGolem = (Integer) rs.getObject("lvlTroopDarkElixirGolem");
            this.lvlTroopDarkElixirWitch = (Integer) rs.getObject("lvlTroopDarkElixirWitch");
            this.lvlTroopDarkElixirLavaHound = (Integer) rs.getObject("lvlTroopDarkElixirLavaHound");
            this.lvlTroopDarkElixirBowler = (Integer) rs.getObject("lvlTroopDarkElixirBowler");
            this.lvlTroopDarkElixirIceGolem = (Integer) rs.getObject("lvlTroopDarkElixirIceGolem");
            this.lvlTroopDarkElixirHeadhunter = (Integer) rs.getObject("lvlTroopDarkElixirHeadhunter");
            this.lvlTroopDarkElixirApprenticeWarden = (Integer) rs.getObject("lvlTroopDarkElixirApprenticeWarden");
            this.lvlTroopDarkElixirDruid = (Integer) rs.getObject("lvlTroopDarkElixirDruid");
            this.lvlTroopDarkElixirFurnace = (Integer) rs.getObject("lvlTroopDarkElixirFurnace");
            
            // Spells
            this.lvlSpellLightning = (Integer) rs.getObject("lvlSpellLightning");
            this.lvlSpellHealing = (Integer) rs.getObject("lvlSpellHealing");
            this.lvlSpellRage = (Integer) rs.getObject("lvlSpellRage");
            this.lvlSpellJump = (Integer) rs.getObject("lvlSpellJump");
            this.lvlSpellFreeze = (Integer) rs.getObject("lvlSpellFreeze");
            this.lvlSpellClone = (Integer) rs.getObject("lvlSpellClone");
            this.lvlSpellInvisibility = (Integer) rs.getObject("lvlSpellInvisibility");
            this.lvlSpellRecall = (Integer) rs.getObject("lvlSpellRecall");
            this.lvlSpellRevive = (Integer) rs.getObject("lvlSpellRevive");
            
            // Dark Spells
            this.lvlDarkSpellPoison = (Integer) rs.getObject("lvlDarkSpellPoison");
            this.lvlDarkSpellEarthquake = (Integer) rs.getObject("lvlDarkSpellEarthquake");
            this.lvlDarkSpellHaste = (Integer) rs.getObject("lvlDarkSpellHaste");
            this.lvlDarkSpellSkeleton = (Integer) rs.getObject("lvlDarkSpellSkeleton");
            this.lvlDarkSpellBat = (Integer) rs.getObject("lvlDarkSpellBat");
            this.lvlDarkSpellOvergrowth = (Integer) rs.getObject("lvlDarkSpellOvergrowth");
            this.lvlDarkSpellIceBlock = (Integer) rs.getObject("lvlDarkSpellIceBlock");
            
            // Builder Base Troops
            this.lvlTroopBuilderBaseRagedBarbarian = (Integer) rs.getObject("lvlTroopBuilderBaseRagedBarbarian");
            this.lvlTroopBuilderBaseSneakyArcher = (Integer) rs.getObject("lvlTroopBuilderBaseSneakyArcher");
            this.lvlTroopBuilderBaseBoxerGiant = (Integer) rs.getObject("lvlTroopBuilderBaseBoxerGiant");
            this.lvlTroopBuilderBaseBetaMinion = (Integer) rs.getObject("lvlTroopBuilderBaseBetaMinion");
            this.lvlTroopBuilderBaseBomber = (Integer) rs.getObject("lvlTroopBuilderBaseBomber");
            this.lvlTroopBuilderBaseBabyDragon = (Integer) rs.getObject("lvlTroopBuilderBaseBabyDragon");
            this.lvlTroopBuilderBaseCannonCart = (Integer) rs.getObject("lvlTroopBuilderBaseCannonCart");
            this.lvlTroopBuilderBaseNightWitch = (Integer) rs.getObject("lvlTroopBuilderBaseNightWitch");
            this.lvlTroopBuilderBaseDropShip = (Integer) rs.getObject("lvlTroopBuilderBaseDropShip");
            this.lvlTroopBuilderBasePowerPekka = (Integer) rs.getObject("lvlTroopBuilderBasePowerPekka");
            this.lvlTroopBuilderBaseHogGlider = (Integer) rs.getObject("lvlTroopBuilderBaseHogGlider");
            this.lvlTroopBuilderBaseElectrofireWizard = (Integer) rs.getObject("lvlTroopBuilderBaseElectrofireWizard");
            
            // Siege Machines
            this.lvlSiegeWallWrecker = (Integer) rs.getObject("lvlSiegeWallWrecker");
            this.lvlSiegeBattleBlimp = (Integer) rs.getObject("lvlSiegeBattleBlimp");
            this.lvlSiegeStoneSlammer = (Integer) rs.getObject("lvlSiegeStoneSlammer");
            this.lvlSiegeSiegeBarracks = (Integer) rs.getObject("lvlSiegeSiegeBarracks");
            this.lvlSiegeLogLauncher = (Integer) rs.getObject("lvlSiegeLogLauncher");
            this.lvlSiegeFlameFlinger = (Integer) rs.getObject("lvlSiegeFlameFlinger");
            this.lvlSiegeBattleDrill = (Integer) rs.getObject("lvlSiegeBattleDrill");
            this.lvlSiegeTroopLauncher = (Integer) rs.getObject("lvlSiegeTroopLauncher");
            
            // Pets
            this.lvlPetLASSI = (Integer) rs.getObject("lvlPetLASSI");
            this.lvlPetMightyYak = (Integer) rs.getObject("lvlPetMightyYak");
            this.lvlPetElectroOwl = (Integer) rs.getObject("lvlPetElectroOwl");
            this.lvlPetUnicorn = (Integer) rs.getObject("lvlPetUnicorn");
            this.lvlPetPhoenix = (Integer) rs.getObject("lvlPetPhoenix");
            this.lvlPetPoisonLizard = (Integer) rs.getObject("lvlPetPoisonLizard");
            this.lvlPetDiggy = (Integer) rs.getObject("lvlPetDiggy");
            this.lvlPetFrosty = (Integer) rs.getObject("lvlPetFrosty");
            this.lvlPetSpiritFox = (Integer) rs.getObject("lvlPetSpiritFox");
            this.lvlPetAngryJelly = (Integer) rs.getObject("lvlPetAngryJelly");
            this.lvlPetSneezy = (Integer) rs.getObject("lvlPetSneezy");
            
            // Heroes
            this.lvlHeroBarbarianKing = (Integer) rs.getObject("lvlHeroBarbarianKing");
            this.lvlHeroArcherQueen = (Integer) rs.getObject("lvlHeroArcherQueen");
            this.lvlHeroMinionPrince = (Integer) rs.getObject("lvlHeroMinionPrince");
            this.lvlHeroGrandWarden = (Integer) rs.getObject("lvlHeroGrandWarden");
            this.lvlHeroRoyalChampion = (Integer) rs.getObject("lvlHeroRoyalChampion");
            this.lvlHeroBattleMachine = (Integer) rs.getObject("lvlHeroBattleMachine");
            this.lvlHeroBattleCopter = (Integer) rs.getObject("lvlHeroBattleCopter");
            
            // Hero Equipment - Barbarian King
            this.lvlHeroEquipmentBarbarianPuppet = (Integer) rs.getObject("lvlHeroEquipmentBarbarianPuppet");
            this.lvlHeroEquipmentRageVial = (Integer) rs.getObject("lvlHeroEquipmentRageVial");
            this.lvlHeroEquipmentEarthquakeBoots = (Integer) rs.getObject("lvlHeroEquipmentEarthquakeBoots");
            this.lvlHeroEquipmentVampstache = (Integer) rs.getObject("lvlHeroEquipmentVampstache");
            this.lvlHeroEquipmentGiantGauntlet = (Integer) rs.getObject("lvlHeroEquipmentGiantGauntlet");
            this.lvlHeroEquipmentSpikyBall = (Integer) rs.getObject("lvlHeroEquipmentSpikyBall");
            this.lvlHeroEquipmentSnakeBracelet = (Integer) rs.getObject("lvlHeroEquipmentSnakeBracelet");
            this.lvlHeroEquipmentStickHorse = (Integer) rs.getObject("lvlHeroEquipmentStickHorse");
            
            // Hero Equipment - Archer Queen
            this.lvlHeroEquipmentArcherPuppet = (Integer) rs.getObject("lvlHeroEquipmentArcherPuppet");
            this.lvlHeroEquipmentInvisibilityVial = (Integer) rs.getObject("lvlHeroEquipmentInvisibilityVial");
            this.lvlHeroEquipmentGiantArrow = (Integer) rs.getObject("lvlHeroEquipmentGiantArrow");
            this.lvlHeroEquipmentHealerPuppet = (Integer) rs.getObject("lvlHeroEquipmentHealerPuppet");
            this.lvlHeroEquipmentFrozenArrow = (Integer) rs.getObject("lvlHeroEquipmentFrozenArrow");
            this.lvlHeroEquipmentMagicMirror = (Integer) rs.getObject("lvlHeroEquipmentMagicMirror");
            this.lvlHeroEquipmentActionFigure = (Integer) rs.getObject("lvlHeroEquipmentActionFigure");
            
            // Hero Equipment - Minion Prince
            this.lvlHeroEquipmentHenchmenPuppet = (Integer) rs.getObject("lvlHeroEquipmentHenchmenPuppet");
            this.lvlHeroEquipmentDarkOrb = (Integer) rs.getObject("lvlHeroEquipmentDarkOrb");
            this.lvlHeroEquipmentMetalPants = (Integer) rs.getObject("lvlHeroEquipmentMetalPants");
            this.lvlHeroEquipmentNobleIron = (Integer) rs.getObject("lvlHeroEquipmentNobleIron");
            this.lvlHeroEquipmentDarkCrown = (Integer) rs.getObject("lvlHeroEquipmentDarkCrown");
            this.lvlHeroEquipmentMeteorStaff = (Integer) rs.getObject("lvlHeroEquipmentMeteorStaff");
            
            // Hero Equipment - Grand Warden
            this.lvlHeroEquipmentEternalTome = (Integer) rs.getObject("lvlHeroEquipmentEternalTome");
            this.lvlHeroEquipmentLifeGem = (Integer) rs.getObject("lvlHeroEquipmentLifeGem");
            this.lvlHeroEquipmentRageGem = (Integer) rs.getObject("lvlHeroEquipmentRageGem");
            this.lvlHeroEquipmentHealingTome = (Integer) rs.getObject("lvlHeroEquipmentHealingTome");
            this.lvlHeroEquipmentFireball = (Integer) rs.getObject("lvlHeroEquipmentFireball");
            this.lvlHeroEquipmentLavaloonPuppet = (Integer) rs.getObject("lvlHeroEquipmentLavaloonPuppet");
            this.lvlHeroEquipmentHeroicTorch = (Integer) rs.getObject("lvlHeroEquipmentHeroicTorch");
            
            // Hero Equipment - Royal Champion
            this.lvlHeroEquipmentRoyalGem = (Integer) rs.getObject("lvlHeroEquipmentRoyalGem");
            this.lvlHeroEquipmentSeekingShield = (Integer) rs.getObject("lvlHeroEquipmentSeekingShield");
            this.lvlHeroEquipmentHogRiderPuppet = (Integer) rs.getObject("lvlHeroEquipmentHogRiderPuppet");
            this.lvlHeroEquipmentHasteVial = (Integer) rs.getObject("lvlHeroEquipmentHasteVial");
            this.lvlHeroEquipmentRocketSpear = (Integer) rs.getObject("lvlHeroEquipmentRocketSpear");
            this.lvlHeroEquipmentElectroBoots = (Integer) rs.getObject("lvlHeroEquipmentElectroBoots");
            this.lvlHeroEquipmentFrostFlake = (Integer) rs.getObject("lvlHeroEquipmentFrostFlake");
        }
    }
    
    /**
     * Main method for testing or standalone execution
     */
    public static void main(String[] args) {
        String dbName;

        if (args.length < 1) {
            dbName = "20CG8UURL.db";
            System.err.println("[DEBUG] No dbName argument provided. Using temporary dbName: " + dbName);
        } else {
            dbName = args[0];
        }
        
        A02_ClanMembers updater = null;
        try {
            updater = new A02_ClanMembers(dbName);
            updater.updateDatabase(null); // null = fetch from API
            
            Thread.sleep(5000);
            
        } catch (Exception e) {
            String errorMsg = "Error updating database: " + e.getMessage();
            System.err.println(errorMsg);
            e.printStackTrace();
            
            if (updater != null && updater.discordLogger != null) {
                updater.discordLogger.logError(errorMsg);
                updater.discordLogger.logError("Stack trace: " + UtilsConfig.getStackTraceAsString(e));
            }
            
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            
            System.exit(1);
        }
    }
}
