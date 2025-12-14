// Usage: node dbCreate.js <dbFileName>
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

const DiscordLog = require('../discordbot/logs/discordLog');
const discordLog = new DiscordLog();

// Intercept console.error to also send errors to Discord
const originalConsoleError = console.error;

console.error = function (...args) {
    try {
        // Send error to Discord
        discordLog.logError(args.map(a => (typeof a === 'object' ? JSON.stringify(a, null, 2) : String(a))).join(' '));
    } catch (e) {
        // Fallback to original error if Discord logging fails
        originalConsoleError('Failed to send error to Discord:', e);
    }
    // Always call the original console.error
    originalConsoleError.apply(console, args);
};

function ensureColumn(db, tableName, columnName, columnType) {
    db.get(`PRAGMA table_info(${tableName})`, (err, row) => {
        if (err) {
            console.error(`Error checking columns for ${tableName}:`, err.message);
            return;
        }
        db.all(`PRAGMA table_info(${tableName})`, (err, columns) => {
            if (err) {
                console.error(`Error getting columns for ${tableName}:`, err.message);
                return;
            }
            const exists = columns.some(col => col.name === columnName);
            if (!exists) {
                db.run(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnType}`, (err) => {
                    if (err) console.error(`Error adding column ${columnName} to ${tableName}:`, err.message);
                    else console.log(`Column '${columnName}' added to table '${tableName}'.`);
                });
            }
        });
    });
}

function createOrUpdateTable(db, tableName, columns, indexColumns) {
    if (!columns || columns.length === 0) {
        console.error(`No columns specified for table ${tableName}.`);
        return;
    }
    const columnsDef = columns.map(col => `${col.name} ${col.type}`).join(',\n    ');
    const createSQL = `(${columnsDef})`;
    db.run(`CREATE TABLE IF NOT EXISTS ${tableName} ${createSQL}`, (err) => {

        if (err) console.error(`Error creating table ${tableName}:`, err.message);

        else console.log(`Table '${tableName}' ensured.`);

        // Ensure columns exist (for updates)
        columns.forEach(col => {
            ensureColumn(db, tableName, col.name, col.type);
        });
        // Create indexes on specified columns
        if (indexColumns && indexColumns.length > 0) {
            indexColumns.forEach(col => {
                const indexName = `idx_${tableName}_${col}`;
                db.run(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName}(${col})`, (err) => {
                    if (err) {
                        console.error(`Error creating index on ${tableName}.${col}:`, err.message);
                    }
                        
                    else console.log(`Index '${indexName}' created on '${tableName}(${col})'.`);
                });
            });
        }
    });
}

function createDatabase(dbName) {
    const dbDir = path.resolve(__dirname, '../../../../database/core');
    const dbPath = path.join(dbDir, dbName);
    const dbExists = fs.existsSync(dbPath);

    // Log INFO: database is being created/running
    const infoMsgStart = `Database creation started for: ${dbName}`;
    console.log(infoMsgStart);
    discordLog.logInfo(infoMsgStart);

    // Create a blank database file if it doesn't exist
    if (!dbExists) {
        try {
            fs.mkdirSync(dbDir, { recursive: true });
            fs.closeSync(fs.openSync(dbPath, 'w'));
            const infoMsgBlank = `Blank database file '${dbName}' created at ${dbPath}`;
            console.log(infoMsgBlank);
            discordLog.logInfo(infoMsgBlank);
        } catch (err) {
            const errMsg = `Failed to create database file: ${dbPath}. Error: ${err.message}. Check the console log for details.`;
            console.error(errMsg);
            discordLog.logError(errMsg);
            throw err;
        }
    }

    const db = new sqlite3.Database(dbPath, (err) => {
        if (err) {
            const errMsg = `Error creating database: ${err.message}`;
            console.error(errMsg);
            discordLog.logError(errMsg);
        } else {
            const infoMsgOpen = `Database '${dbName}' opened`;
            console.log(infoMsgOpen);
            discordLog.logSuccess(infoMsgOpen);
        }
    });

    try {
        tableInfo(db);
        const successMsg = `Database created successfully: ${dbPath}`;
        console.log(successMsg);
        discordLog.logSuccess(successMsg);
    } catch (err) {
        const errMsg = `Database creation failed for ${dbName}: ${err.message}`;
        console.error(errMsg);
        discordLog.logError(errMsg);
        throw err;
    } finally {
        db.close();
    }
}

function tableInfo(db) {
    // Table 1: A01_ClanInfo
    // Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}
    // DB format: New rows daily
    // DB Ordering: Date logged Ascending

    createOrUpdateTable(
        db,
        'A01_ClanInfo',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'dateLogged', type: 'TEXT' },
            { name: 'season', type: 'TEXT' },

            // Clan Info
            { name: 'name', type: 'TEXT' },
            { name: 'type', type: 'TEXT' },
            { name: 'description', type: 'TEXT' },
            { name: 'memberCount', type: 'INTEGER' },
            { name: 'clanLevel', type: 'INTEGER' },
            { name: 'clanPoints', type: 'INTEGER' },
            { name: 'clanBbPoints', type: 'INTEGER' },
            { name: 'locationName', type: 'TEXT' },
            { name: 'isFamilyFriendly', type: 'BOOLEAN' },
            { name: 'chatLanguage', type: 'TEXT' },

            // Entrance Requirements
            { name: 'requiredTrophies', type: 'INTEGER' },
            { name: 'requiredBbTrophies', type: 'INTEGER' },
            { name: 'requiredThLevel', type: 'INTEGER' },

            // Clan War Info
            { name: 'warFrequency', type: 'TEXT' },
            { name: 'isWarLogPublic', type: 'BOOLEAN' },
            { name: 'warWinStreak', type: 'INTEGER' },
            { name: 'warWins', type: 'INTEGER' },
            { name: 'warTies', type: 'INTEGER' },
            { name: 'warLosses', type: 'INTEGER' },
            { name: 'CWLLeagueName', type: 'TEXT' },

            // Clan Capital Info
            { name: 'capitalHallLevel', type: 'INTEGER' },
            { name: 'capitalPoints', type: 'INTEGER' },

            // Clan Capital Districts
            { name: 'lvlCapitalPeak', type: 'INTEGER' },
            { name: 'lvlBarbarianCamp', type: 'INTEGER' },
            { name: 'lvlWizardValley', type: 'INTEGER' },
            { name: 'lvlBalloonLagoon', type: 'INTEGER' },
            { name: 'lvlBuildersWorkshop', type: 'INTEGER' },
            { name: 'lvlDragonCliffs', type: 'INTEGER' },
            { name: 'lvlGolemQuarry', type: 'INTEGER' },
            { name: 'lvlSkeletonPark', type: 'INTEGER' },
            { name: 'lvlGoblinMines', type: 'INTEGER' }
        ],
        ['season']
    );


    // Table 2: A02_ClanMembers
    // Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/members || https://api.clashofclans.com/v1/players/%23{playerTag}
    // DB format: New rows only for new members; old members updated. Updated when number of members change, or every day, whichever is sooner.
    // DB Ordering: Date of first join

    createOrUpdateTable(
        db,
        'A02_ClanMembers',
        [
            // Main player info
            { name: 'playerTag', type: 'TEXT PRIMARY KEY' },
            { name: 'name', type: 'TEXT' },

            { name: 'lastUpdated', type: 'TEXT' },
            { name: 'dateJoin', type: 'TEXT' },
            { name: 'dateLeft', type: 'TEXT' },
            { name: 'lastActive', type: 'TEXT' },

            { name: 'thLevel', type: 'INTEGER' },
            { name: 'bhLevel', type: 'INTEGER' },
            { name: 'xpLevel', type: 'INTEGER' },

            { name: 'trophies', type: 'INTEGER' },
            { name: 'bestTrophies', type: 'INTEGER' },
            { name: 'legendTrophies', type: 'INTEGER' },
            { name: 'bbTrophies', type: 'INTEGER' },
            { name: 'bestBbTrophies', type: 'INTEGER' },
            
            { name: 'warStars', type: 'INTEGER' },
            { name: 'attackWins', type: 'INTEGER' },
            { name: 'defenseWins', type: 'INTEGER' },

            { name: 'clanRole', type: 'TEXT' },
            { name: 'warPreference', type: 'BOOLEAN' },
            { name: 'donations', type: 'INTEGER' },
            { name: 'donationsReceived', type: 'INTEGER' },
            { name: 'clanCapitalContributions', type: 'INTEGER' },

            { name: 'legacyLeagueName', type: 'TEXT' },
            { name: 'leagueInt', type: 'INTEGER' },
            { name: 'bbLeagueName', type: 'TEXT' },

            // Achievements
            { name: 'achievementBiggerCoffers', type: 'INTEGER' },
            { name: 'achievementGetThoseGoblins', type: 'INTEGER' },
            { name: 'achievementBiggerBetter', type: 'INTEGER' },
            { name: 'achievementNiceAndTidy', type: 'INTEGER' },
            { name: 'achievementDiscoverNewTroops', type: 'INTEGER' },
            { name: 'achievementGoldGrab', type: 'INTEGER' },
            { name: 'achievementElixirEscapade', type: 'INTEGER' },
            { name: 'achievementSweetVictory', type: 'INTEGER' },
            { name: 'achievementEmpireBuilder', type: 'INTEGER' },
            { name: 'achievementWallBuster', type: 'INTEGER' },
            { name: 'achievementHumiliator', type: 'INTEGER' },
            { name: 'achievementUnionBuster', type: 'INTEGER' },
            { name: 'achievementConqueror', type: 'INTEGER' },
            { name: 'achievementUnbreakable', type: 'INTEGER' },
            { name: 'achievementFriendInNeed', type: 'INTEGER' },
            { name: 'achievementMortarMauler', type: 'INTEGER' },
            { name: 'achievementHeroicHeist', type: 'INTEGER' },
            { name: 'achievementLeagueAllStar', type: 'INTEGER' },
            { name: 'achievementXBowExterminator', type: 'INTEGER' },
            { name: 'achievementFirefighter', type: 'INTEGER' },
            { name: 'achievementWarHero', type: 'INTEGER' },
            { name: 'achievementClanWarWealth', type: 'INTEGER' },
            { name: 'achievementAntiArtillery', type: 'INTEGER' },
            { name: 'achievementSharingIsCaring', type: 'INTEGER' },
            { name: 'achievementKeepYourAccountSafe', type: 'INTEGER' },
            { name: 'achievementMasterEngineering', type: 'INTEGER' },
            { name: 'achievementNextGenerationModel', type: 'INTEGER' },
            { name: 'achievementUnBuildIt', type: 'INTEGER' },
            { name: 'achievementChampionBuilder', type: 'INTEGER' },
            { name: 'achievementHighGear', type: 'INTEGER' },
            { name: 'achievementHiddenTreasures', type: 'INTEGER' },
            { name: 'achievementGamesChampion', type: 'INTEGER' },
            { name: 'achievementDragonSlayer', type: 'INTEGER' },
            { name: 'achievementWarLeagueLegend', type: 'INTEGER' },
            { name: 'achievementWellSeasoned', type: 'INTEGER' },
            { name: 'achievementShatteredAndScattered', type: 'INTEGER' },
            { name: 'achievementNotSoEasyThisTime', type: 'INTEGER' },
            { name: 'achievementBustThis', type: 'INTEGER' },
            { name: 'achievementSuperbWork', type: 'INTEGER' },
            { name: 'achievementSiegeSharer', type: 'INTEGER' },
            { name: 'achievementAggressiveCapitalism', type: 'INTEGER' },
            { name: 'achievementMostValuableClanmate', type: 'INTEGER' },
            { name: 'achievementCounterspell', type: 'INTEGER' },
            { name: 'achievementMonolithMasher', type: 'INTEGER' },
            { name: 'achievementUngratefulChild', type: 'INTEGER' },
            { name: 'achievementSupercharger', type: 'INTEGER' },
            { name: 'achievementMultiArcherTowerTerminator', type: 'INTEGER' },
            { name: 'achievementRicochetCannonCrusher', type: 'INTEGER' },
            { name: 'achievementFirespitterFinisher', type: 'INTEGER' },
            { name: 'achievementMultiGearTowerTrampler', type: 'INTEGER' },
            { name: 'achievementCraftingConnoisseur', type: 'INTEGER' },
            { name: 'achievementCraftersNightmare', type: 'INTEGER' },
            { name: 'achievementLeagueFollower', type: 'INTEGER' },

            // Elixir Troops
            { name: 'lvlTroopElixirBarbarian', type: 'INTEGER' },
            { name: 'lvlTroopElixirArcher', type: 'INTEGER' },
            { name: 'lvlTroopElixirGiant', type: 'INTEGER' },
            { name: 'lvlTroopElixirGoblin', type: 'INTEGER' },
            { name: 'lvlTroopElixirWallBreaker', type: 'INTEGER' },
            { name: 'lvlTroopElixirBalloon', type: 'INTEGER' },
            { name: 'lvlTroopElixirWizard', type: 'INTEGER' },
            { name: 'lvlTroopElixirHealer', type: 'INTEGER' },
            { name: 'lvlTroopElixirDragon', type: 'INTEGER' },
            { name: 'lvlTroopElixirPEKKA', type: 'INTEGER' },
            { name: 'lvlTroopElixirBabyDragon', type: 'INTEGER' },
            { name: 'lvlTroopElixirMiner', type: 'INTEGER' },
            { name: 'lvlTroopElixirElectroDragon', type: 'INTEGER' },
            { name: 'lvlTroopElixirYeti', type: 'INTEGER' },
            { name: 'lvlTroopElixirDragonRider', type: 'INTEGER' },
            { name: 'lvlTroopElixirElectroTitan', type: 'INTEGER' },
            { name: 'lvlTroopElixirRootRider', type: 'INTEGER' },
            { name: 'lvlTroopElixirThrower', type: 'INTEGER' },

            // Dark Elixir Troops
            { name: 'lvlTroopDarkElixirMinion', type: 'INTEGER' },
            { name: 'lvlTroopDarkElixirHogRider', type: 'INTEGER' },
            { name: 'lvlTroopDarkElixirValkyrie', type: 'INTEGER' },
            { name: 'lvlTroopDarkElixirGolem', type: 'INTEGER' },
            { name: 'lvlTroopDarkElixirWitch', type: 'INTEGER' },
            { name: 'lvlTroopDarkElixirLavaHound', type: 'INTEGER' },
            { name: 'lvlTroopDarkElixirBowler', type: 'INTEGER' },
            { name: 'lvlTroopDarkElixirIceGolem', type: 'INTEGER' },
            { name: 'lvlTroopDarkElixirHeadhunter', type: 'INTEGER' },
            { name: 'lvlTroopDarkElixirApprenticeWarden', type: 'INTEGER' },
            { name: 'lvlTroopDarkElixirDruid', type: 'INTEGER' },
            { name: 'lvlTroopDarkElixirFurnace', type: 'INTEGER' },

            // Spells
            { name: 'lvlSpellLightning', type: 'INTEGER' },
            { name: 'lvlSpellHealing', type: 'INTEGER' },
            { name: 'lvlSpellRage', type: 'INTEGER' },
            { name: 'lvlSpellJump', type: 'INTEGER' },
            { name: 'lvlSpellFreeze', type: 'INTEGER' },
            { name: 'lvlSpellClone', type: 'INTEGER' },
            { name: 'lvlSpellInvisibility', type: 'INTEGER' },
            { name: 'lvlSpellRecall', type: 'INTEGER' },
            { name: 'lvlSpellRevive', type: 'INTEGER' },

            // Dark Spells
            { name: 'lvlDarkSpellPoison', type: 'INTEGER' },
            { name: 'lvlDarkSpellEarthquake', type: 'INTEGER' },
            { name: 'lvlDarkSpellHaste', type: 'INTEGER' },
            { name: 'lvlDarkSpellSkeleton', type: 'INTEGER' },
            { name: 'lvlDarkSpellBat', type: 'INTEGER' },
            { name: 'lvlDarkSpellOvergrowth', type: 'INTEGER' },
            { name: 'lvlDarkSpellIceBlock', type: 'INTEGER' },

            // Builder Base Troops
            { name: 'lvlTroopBuilderBaseRagedBarbarian', type: 'INTEGER' },
            { name: 'lvlTroopBuilderBaseSneakyArcher', type: 'INTEGER' },
            { name: 'lvlTroopBuilderBaseBoxerGiant', type: 'INTEGER' },
            { name: 'lvlTroopBuilderBaseBetaMinion', type: 'INTEGER' },
            { name: 'lvlTroopBuilderBaseBomber', type: 'INTEGER' },
            { name: 'lvlTroopBuilderBaseBabyDragon', type: 'INTEGER' },
            { name: 'lvlTroopBuilderBaseCannonCart', type: 'INTEGER' },
            { name: 'lvlTroopBuilderBaseNightWitch', type: 'INTEGER' },
            { name: 'lvlTroopBuilderBaseDropShip', type: 'INTEGER' },
            { name: 'lvlTroopBuilderBasePowerPekka', type: 'INTEGER' },
            { name: 'lvlTroopBuilderBaseHogGlider', type: 'INTEGER' },
            { name: 'lvlTroopBuilderBaseElectrofireWizard', type: 'INTEGER' },

            // Siege Machines
            { name: 'lvlSiegeWallWrecker', type: 'INTEGER' },
            { name: 'lvlSiegeBattleBlimp', type: 'INTEGER' },
            { name: 'lvlSiegeStoneSlammer', type: 'INTEGER' },
            { name: 'lvlSiegeSiegeBarracks', type: 'INTEGER' },
            { name: 'lvlSiegeLogLauncher', type: 'INTEGER' },
            { name: 'lvlSiegeFlameFlinger', type: 'INTEGER' },
            { name: 'lvlSiegeBattleDrill', type: 'INTEGER' },
            { name: 'lvlSiegeTroopLauncher', type: 'INTEGER' },

            // Pets
            { name: 'lvlPetLASSI', type: 'INTEGER' },
            { name: 'lvlPetMightyYak', type: 'INTEGER' },
            { name: 'lvlPetElectroOwl', type: 'INTEGER' },
            { name: 'lvlPetUnicorn', type: 'INTEGER' },
            { name: 'lvlPetPhoenix', type: 'INTEGER' },
            { name: 'lvlPetPoisonLizard', type: 'INTEGER' },
            { name: 'lvlPetDiggy', type: 'INTEGER' },
            { name: 'lvlPetFrosty', type: 'INTEGER' },
            { name: 'lvlPetSpiritFox', type: 'INTEGER' },
            { name: 'lvlPetAngryJelly', type: 'INTEGER' },
            { name: 'lvlPetSneezy', type: 'INTEGER' },

            // Heroes
            { name: 'lvlHeroBarbarianKing', type: 'INTEGER' },
            { name: 'lvlHeroArcherQueen', type: 'INTEGER' },
            { name: 'lvlHeroMinionPrince', type: 'INTEGER' },
            { name: 'lvlHeroGrandWarden', type: 'INTEGER' },
            { name: 'lvlHeroRoyalChampion', type: 'INTEGER' },

            { name: 'lvlHeroBattleMachine', type: 'INTEGER' },
            { name: 'lvlHeroBattleCopter', type: 'INTEGER' },

            // Hero Equipment
            { name: 'lvlHeroEquipmentBarbarianPuppet', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentRageVial', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentEarthquakeBoots', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentVampstache', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentGiantGauntlet', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentSpikyBall', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentSnakeBracelet', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentStickHorse', type: 'INTEGER' },

            { name: 'lvlHeroEquipmentArcherPuppet', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentInvisibilityVial', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentGiantArrow', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentHealerPuppet', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentFrozenArrow', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentMagicMirror', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentActionFigure', type: 'INTEGER' },

            { name: 'lvlHeroEquipmentHenchmenPuppet', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentDarkOrb', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentMetalPants', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentNobleIron', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentDarkCrown', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentMeteorStaff', type: 'INTEGER' },

            { name: 'lvlHeroEquipmentEternalTome', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentLifeGem', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentRageGem', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentHealingTome', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentFireball', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentLavaloonPuppet', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentHeroicTorch', type: 'INTEGER' },

            { name: 'lvlHeroEquipmentRoyalGem', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentSeekingShield', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentHogRiderPuppet', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentHasteVial', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentRocketSpear', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentElectroBoots', type: 'INTEGER' },
            { name: 'lvlHeroEquipmentFrostFlake', type: 'INTEGER' }
        ],
        ['playerTag']
    );

    // Table 3: A03_CWLWarDetails
    // Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/currentwar/leaguegroup || https://api.clashofclans.com/v1/clanwarleagues/wars/%23{warTag [NOT CLAN TAG!!!]}.
    // DB format: Because of API limitations, we will first store the war details. Each row is a war, meaning every day should have 4 wars, and every CWL should have 28 wars.
    // DB Ordering: Season name > war number > war tag

    createOrUpdateTable(
        db,
        'A03_CWLWarDetails',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'dateLogged', type: 'TEXT' },

            // CWL Season Info
            { name: 'season', type: 'TEXT' },
            { name: 'seasonWarState', type: 'TEXT' },
            { name: 'teamSize', type: 'INTEGER' },

            // War Info
            { name: 'warNum', type: 'INTEGER' },
            { name: 'warTag', type: 'TEXT' },
            { name: 'warState', type: 'TEXT' },
            { name: 'prepStartTime', type: 'TEXT' },
            { name: 'startTime', type: 'TEXT' },
            { name: 'endTime', type: 'TEXT' },

            // Clan Info
            { name: 'clanTag', type: 'TEXT' },
            { name: 'clanName', type: 'TEXT' },
            { name: 'clanLevel', type: 'INTEGER' },
            { name: 'clanAttacks', type: 'INTEGER' },
            { name: 'clanStars', type: 'INTEGER' },
            { name: 'clanDestructionPercent', type: 'REAL' },

            { name: 'opponentTag', type: 'TEXT' },
            { name: 'opponentName', type: 'TEXT' },
            { name: 'opponentLevel', type: 'INTEGER' },
            { name: 'opponentAttacks', type: 'INTEGER' },
            { name: 'opponentStars', type: 'INTEGER' },
            { name: 'opponentDestructionPercent', type: 'REAL' },

            { name: 'winningClanTag', type: 'TEXT' }
        ],
        ['season', 'seasonWarState']
    );

    // Table 4: A04_CWLAttackDetails
    // Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/currentwar/leaguegroup || https://api.clashofclans.com/v1/clanwarleagues/wars/%23{warTag [NOT CLAN TAG!!!]}
    // DB format: Because of API limitations, all attacks are stored. Basically, only data for each attack for each war is stored. Data is first obtained from A3_CWLWarDetails.
    // DB Ordering: war tag (based on A3_CWLWarDetails) > Team1 attacker map position then team 2.

    createOrUpdateTable(
        db,
        'A04_CWLAttackDetails',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'dateLogged', type: 'TEXT' },
            { name: 'season', type: 'TEXT' },
            { name: 'warTag', type: 'TEXT' },
            { name: 'clanTag', type: 'TEXT' },
            { name: 'opponentTag', type: 'TEXT' },

            // Player Details
            { name: 'attackerTag', type: 'TEXT' },
            { name: 'attackerName', type: 'TEXT' },
            { name: 'attackerThLevel', type: 'INTEGER' },
            { name: 'attackerMapPosition', type: 'INTEGER' },

            // Opponent Details
            { name: 'defenderTag', type: 'TEXT' },
            { name: 'defenderName', type: 'TEXT' },
            { name: 'defenderThLevel', type: 'INTEGER' },
            { name: 'defenderMapPosition', type: 'INTEGER' },

            // Attack Details
            { name: 'stars', type: 'INTEGER' },
            { name: 'destructionPercentage', type: 'INTEGER' },
            { name: 'attackOrder', type: 'INTEGER' },
            { name: 'duration', type: 'INTEGER' }
        ],
        ['season', 'warTag', 'clanTag', 'opponentTag', 'attackerTag']
    );

    // Table 5: A05_ClanWarLog
    // Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/warlog
    // DB format: Stores high-level overview of war details.
    // DB Ordering: first to last war

    createOrUpdateTable(
        db,
        'A05_ClanWarLog',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'dateLogged', type: 'TEXT' },
            { name: 'cwSeason', type: 'TEXT' },

            // Clan War Info
            { name: 'result', type: 'TEXT' },
            { name: 'teamsize', type: 'INTEGER' },
            { name: 'attacksPerMember', type: 'INTEGER' },
            { name: 'battleModifier', type: 'TEXT' },
            { name: 'endTime', type: 'TEXT' },

            // Attacker War Stats
            { name: 'clanTag', type: 'TEXT' },
            { name: 'clanName', type: 'TEXT' },
            { name: 'clanLevel', type: 'INTEGER' },
            { name: 'clanAttacks', type: 'INTEGER' },
            { name: 'clanStars', type: 'INTEGER' },
            { name: 'clanDestructionPercent', type: 'REAL' },
            { name: 'clanXPGained', type: 'INTEGER' },

            // Defender War Stats
            { name: 'opponentTag', type: 'TEXT' },
            { name: 'opponentName', type: 'TEXT' },
            { name: 'opponentLevel', type: 'INTEGER' },
            { name: 'opponentStars', type: 'INTEGER' },
            { name: 'opponentDestructionPercent', type: 'REAL' }
        ],
        ['cwSeason', 'result']
    );

    // Table 6: A06_ClanWarAttackDetails
    // Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/currentwar
    // DB format: Stores detailed information about each player's attacks in clan wars. Does not account for CWL.
    // DB Ordering: first to last war > attacker map position then defender map position. Indexed via endTime

    createOrUpdateTable(
        db,
        'A06_ClanWarAttackDetails',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'dateLogged', type: 'TEXT' },
            { name: 'cwSeason', type: 'TEXT' },

            // Player Details
            { name: 'attackerTag', type: 'TEXT' },
            { name: 'attackerName', type: 'TEXT' },
            { name: 'attackerThLevel', type: 'INTEGER' },
            { name: 'attackerMapPosition', type: 'INTEGER' },

            // Attack 1
            { name: 'defender1Tag', type: 'TEXT' },
            { name: 'defender1Name', type: 'TEXT' },
            { name: 'defender1ThLevel', type: 'INTEGER' },
            { name: 'defender1MapPosition', type: 'INTEGER' },

            { name: 'attack1Stars', type: 'INTEGER' },
            { name: 'attack1DestructionPercentage', type: 'INTEGER' },
            { name: 'attack1Order', type: 'INTEGER' },
            { name: 'attack1Duration', type: 'INTEGER' },

            { name: 'attack1Score', type: 'REAL' },
            { name: 'attack1ThModifier', type: 'REAL' },

            // Attack 2
            { name: 'defender2Tag', type: 'TEXT' },
            { name: 'defender2Name', type: 'TEXT' },
            { name: 'defender2ThLevel', type: 'INTEGER' },
            { name: 'defender2MapPosition', type: 'INTEGER' },

            { name: 'attack2Stars', type: 'INTEGER' },
            { name: 'attack2DestructionPercentage', type: 'INTEGER' },
            { name: 'attack2Order', type: 'INTEGER' },
            { name: 'attack2Duration', type: 'INTEGER' },

            { name: 'attack2Score', type: 'REAL' },
            { name: 'attack2ThModifier', type: 'REAL' },

            // Best Defense
            { name: 'defenseAttackerTag', type: 'TEXT' },
            { name: 'defenseStars', type: 'INTEGER' },
            { name: 'defenseDestructionPercentage', type: 'INTEGER' },
            { name: 'defenseOrder', type: 'INTEGER' },
            { name: 'defenseDuration', type: 'INTEGER' },

            // Scores
            { name: 'attacksUsed', type: 'REAL' },
            { name: 'totalWarScore', type: 'REAL' }
        ],
        ['dateLogged', 'cwSeason', 'attackerTag']
    );

    // Table 7: A07_ClanCapitalLog
    // Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/capitalraidseasons
    // DB format: Stores high-level details of clan capital raids.
    // DB Ordering: first to last raid
    
    createOrUpdateTable(
        db,
        'A07_ClanCapitalLog',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'dateLogged', type: 'TEXT' },

            // Raid Identifiers
            { name: 'ccSeason', type: 'TEXT' },
            { name: 'state', type: 'TEXT' },
            { name: 'startTime', type: 'TEXT' },
            { name: 'endTime', type: 'TEXT' },

            // Raid Info
            { name: 'raidsCompleted', type: 'INTEGER' },
            { name: 'totalAttacks', type: 'INTEGER' },
            { name: 'enemyDistrictsDestroyed', type: 'INTEGER' },

            // Raid Rewards
            { name: 'capitalGoldEarned', type: 'INTEGER' },
            { name: 'raidMedalsOffensive', type: 'INTEGER' },
            { name: 'raidMedalsDefensive', type: 'INTEGER' },

            // Trophy Info
            { name: 'trophyCount', type: 'INTEGER' },
            { name: 'clanXp', type: 'INTEGER' }
        ],
        ['ccSeason', 'state']
    );

    // Table 8: A08_ClanCapitalAttacks
    // Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/capitalraidseasons
    // DB format: Stores stats of every member's attacks in weekend raids.
    // DB Ordering: earliest to latest raid > highest resources earned
    
    createOrUpdateTable(
        db,
        'A08_ClanCapitalAttacks',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'ccSeason', type: 'TEXT' },
            { name: 'dateLogged', type: 'TEXT' },

            // Player Info
            { name: 'playerTag', type: 'TEXT' },
            { name: 'playerName', type: 'TEXT' },

            // Attacks Used
            { name: 'attacksUsed', type: 'INTEGER' },
            { name: 'attackLimit', type: 'INTEGER' },
            { name: 'bonusAttacksGained', type: 'INTEGER' },
            { name: 'attacksUsedScore', type: 'REAL' },

            // Capital Gold Obtained
            { name: 'capitalGoldLooted', type: 'INTEGER' },
            { name: 'goldObtainedScore', type: 'REAL' },

            // Capital Gold Donated
            { name: 'mostValuableClanmatePoints', type: 'INTEGER' },
            { name: 'increaseFromPrevious', type: 'INTEGER' },
            { name: 'goldDonationScore', type: 'REAL' },

            { name: 'clanCapitalScore', type: 'REAL' }
        ],
        ['ccSeason', 'playerTag', 'attacksUsed']
    );

    // Table 9: A09_ClanCapitalClanAttacks
    // Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/capitalraidseasons
    // DB format: Stores stats of clans attacked.
    // DB Ordering: earliest to latest raid > Earliest to finish
    
    createOrUpdateTable(
        db,
        'A09_ClanCapitalClanAttacks',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'ccSeason', type: 'TEXT' },
            { name: 'dateLogged', type: 'TEXT' },

            // Player Info
            { name: 'playerTag', type: 'TEXT' },
            { name: 'playerName', type: 'TEXT' },

            // Raid Stats
            { name: 'attacksUsed', type: 'INTEGER' },
            { name: 'attackLimit', type: 'INTEGER' },
            { name: 'bonusAttacksGained', type: 'INTEGER' },
            { name: 'capitalGoldLooted', type: 'INTEGER' }
        ],
        ['ccSeason']
    );

    // Table 10: A10_ClanCapitalClanDefenses
    // Primary endpoint: https://api.clashofclans.com/v1/clans/%23{clanTag}/capitalraidseasons
    // DB format: Stores stats of clans defended.
    // DB Ordering: earliest to latest raid > highest resources earned
    
    createOrUpdateTable(
        db,
        'A10_ClanCapitalClanDefenses',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'ccSeason', type: 'TEXT' },
            { name: 'dateLogged', type: 'TEXT' },

            // Player Info
            { name: 'playerTag', type: 'TEXT' },
            { name: 'playerName', type: 'TEXT' },

            // Raid Stats
            { name: 'attacksUsed', type: 'INTEGER' },
            { name: 'attackLimit', type: 'INTEGER' },
            { name: 'bonusAttacksGained', type: 'INTEGER' },
            { name: 'capitalGoldLooted', type: 'INTEGER' }
        ],
        ['ccSeason']
    );

    // Table 11: B01_PlayerLog
    // Primary database: A2_ClanMembers
    // DB format: Stores history of active players and relevant metrics to calculate the player quality score. runs every week before the ranked league ends (to obtain trophy numbers)
    // DB Ordering: season name > player rank in clan
    
    createOrUpdateTable(
        db,
        'B01_PlayerLog',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'rankedSeason', type: 'TEXT' },
            { name: 'playerTag', type: 'TEXT' },

            // TH League + Position
            { name: 'thLevel', type: 'INTEGER' },
            { name: 'leagueInt', type: 'INTEGER' },
            { name: 'thLeagueScore', type: 'REAL' },
            { name: 'trophies', type: 'INTEGER' },
            { name: 'trophiesLeagueScore', type: 'REAL' },

            // War Stars
            { name: 'warStars', type: 'INTEGER' },
            { name: 'warStarsScore', type: 'REAL' },

            // Hero Levels
            { name: 'lvlHeroBarbarianKing', type: 'INTEGER' },
            { name: 'lvlHeroArcherQueen', type: 'INTEGER' },
            { name: 'lvlHeroMinionPrince', type: 'INTEGER' },
            { name: 'lvlHeroGrandWarden', type: 'INTEGER' },
            { name: 'lvlHeroRoyalChampion', type: 'INTEGER' },
            { name: 'heroLevelsScore', type: 'REAL' },

            // Lab Levels
            { name: 'labUpgrades', type: 'INTEGER' },
            { name: 'labUpgradesThMax', type: 'INTEGER' },
            { name: 'labLevelsScore', type: 'REAL' },

            // Pet Levels
            { name: 'petUpgrades', type: 'INTEGER' },
            { name: 'petUpgradesThMax', type: 'INTEGER' },
            { name: 'petLevelsScore', type: 'REAL' },

            // Equipment Levels
            { name: 'equipmentUpgrades', type: 'INTEGER' },
            { name: 'equipmentUpgradesThMax', type: 'INTEGER' },
            { name: 'equipmentLevelsScore', type: 'REAL' },

            { name: 'playerQualityScore', type: 'REAL' }
        ],
        ['rankedSeason', 'playerTag']
    );

    // Table 12: B02_ClanGamesLog
    // Primary database: A2_ClanMembers
    // DB format: Stores history of the total amount of clan games points earned, meant to help identify how much each member contributed in each clan games.
    // DB Ordering: season name > player tag
    
    createOrUpdateTable(
        db,
        'B02_ClanGamesLog',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'season', type: 'TEXT' },
            { name: 'playerTag', type: 'TEXT' },

            // Clan Games Stats
            { name: 'gamesChampionPoints', type: 'INTEGER' },
            { name: 'increaseFromPrevious', type: 'INTEGER' },

            { name: 'clanGamesScore', type: 'REAL' }
        ],
        ['season', 'playerTag']
    );

    // Table 13: B03_CWLPlayerLog
    // Primary database: A4_CWLAttackDetails
    // DB format: Stores history of the player's performance each CWL.
    // DB Ordering: season name > highest performing player
    
    createOrUpdateTable(
        db,
        'B03_CWLPlayerLog',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            { name: 'season', type: 'TEXT' },
            { name: 'playerTag', type: 'TEXT' },
            { name: 'thLevel', type: 'INTEGER' },
            { name: 'mirrorRule', type: 'BOOLEAN' },

            // Stars
            { name: 'war1Stars', type: 'INTEGER' },
            { name: 'war2Stars', type: 'INTEGER' },
            { name: 'war3Stars', type: 'INTEGER' },
            { name: 'war4Stars', type: 'INTEGER' },
            { name: 'war5Stars', type: 'INTEGER' },
            { name: 'war6Stars', type: 'INTEGER' },
            { name: 'war7Stars', type: 'INTEGER' },

            // Percentages
            { name: 'war1Percentage', type: 'INTEGER' },
            { name: 'war2Percentage', type: 'INTEGER' },
            { name: 'war3Percentage', type: 'INTEGER' },
            { name: 'war4Percentage', type: 'INTEGER' },
            { name: 'war5Percentage', type: 'INTEGER' },
            { name: 'war6Percentage', type: 'INTEGER' },
            { name: 'war7Percentage', type: 'INTEGER' },

            // Stars Percentage Score
            { name: 'war1AttackScore', type: 'REAL' },
            { name: 'war2AttackScore', type: 'REAL' },
            { name: 'war3AttackScore', type: 'REAL' },
            { name: 'war4AttackScore', type: 'REAL' },
            { name: 'war5AttackScore', type: 'REAL' },
            { name: 'war6AttackScore', type: 'REAL' },
            { name: 'war7AttackScore', type: 'REAL' },

            // Opponent TH Level
            { name: 'war1OppThLevel', type: 'INTEGER' },
            { name: 'war2OppThLevel', type: 'INTEGER' },
            { name: 'war3OppThLevel', type: 'INTEGER' },
            { name: 'war4OppThLevel', type: 'INTEGER' },
            { name: 'war5OppThLevel', type: 'INTEGER' },
            { name: 'war6OppThLevel', type: 'INTEGER' },
            { name: 'war7OppThLevel', type: 'INTEGER' },

            // Opponent TH Modifier
            { name: 'war1OppThLevelModifier', type: 'REAL' },
            { name: 'war2OppThLevelModifier', type: 'REAL' },
            { name: 'war3OppThLevelModifier', type: 'REAL' },
            { name: 'war4OppThLevelModifier', type: 'REAL' },
            { name: 'war5OppThLevelModifier', type: 'REAL' },
            { name: 'war6OppThLevelModifier', type: 'REAL' },
            { name: 'war7OppThLevelModifier', type: 'REAL' },

            // Player Map Position
            { name: 'war1MapPosition', type: 'INTEGER' },
            { name: 'war2MapPosition', type: 'INTEGER' },
            { name: 'war3MapPosition', type: 'INTEGER' },
            { name: 'war4MapPosition', type: 'INTEGER' },
            { name: 'war5MapPosition', type: 'INTEGER' },
            { name: 'war6MapPosition', type: 'INTEGER' },
            { name: 'war7MapPosition', type: 'INTEGER' },

            // Opponent Map Position
            { name: 'war1OppMapPosition', type: 'INTEGER' },
            { name: 'war2OppMapPosition', type: 'INTEGER' },
            { name: 'war3OppMapPosition', type: 'INTEGER' },
            { name: 'war4OppMapPosition', type: 'INTEGER' },
            { name: 'war5OppMapPosition', type: 'INTEGER' },
            { name: 'war6OppMapPosition', type: 'INTEGER' },
            { name: 'war7OppMapPosition', type: 'INTEGER' },

            // Mirror Check Modifier
            { name: 'war1MirrorModifier', type: 'REAL' },
            { name: 'war2MirrorModifier', type: 'REAL' },
            { name: 'war3MirrorModifier', type: 'REAL' },
            { name: 'war4MirrorModifier', type: 'REAL' },
            { name: 'war5MirrorModifier', type: 'REAL' },
            { name: 'war6MirrorModifier', type: 'REAL' },
            { name: 'war7MirrorModifier', type: 'REAL' },

            // Individual War Scores
            { name: 'war1Score', type: 'REAL' },
            { name: 'war2Score', type: 'REAL' },
            { name: 'war3Score', type: 'REAL' },
            { name: 'war4Score', type: 'REAL' },
            { name: 'war5Score', type: 'REAL' },
            { name: 'war6Score', type: 'REAL' },
            { name: 'war7Score', type: 'REAL' },

            // Attacks Used
            { name: 'attacksUsed', type: 'INTEGER' },
            { name: 'maxAttacks', type: 'INTEGER' },
            { name: 'attackModifier', type: 'REAL' },

            { name: 'cwlPerformanceScore', type: 'REAL' }
        ],
        ['season', 'playerTag']
    );

    // Table 14: C01_ClanSeasonStats
    // Primary database: A1_ClanInfo || A2_ClanMembers
    // DB format: Stores summarized clan history (monthly, right after CWL signup ends)
    // DB Ordering: season number
    
    createOrUpdateTable(
        db,
        'C01_ClanSeasonStats',
        [
            { name: 'season', type: 'TEXT PRIMARY KEY' },
            { name: 'teamSeason', type: 'INTEGER' },

            //  Clan High Level Overview
            { name: 'clanLevel', type: 'INTEGER' },
            { name: 'estClanXpGain', type: 'INTEGER' },
            { name: 'thClanPoints', type: 'INTEGER' },
            { name: 'bhClanPoints', type: 'INTEGER' },
            { name: 'playerCount', type: 'INTEGER' },

            // Clan Entrance Requirements
            { name: 'thLeagueRequirement', type: 'INTEGER' },
            { name: 'bhTrophyRequirement', type: 'INTEGER' },
            { name: 'minThRequirement', type: 'INTEGER' },

            // Player Stats
            { name: 'avgPlayerLvl', type: 'INTEGER' },
            { name: 'avgThLeague', type: 'REAL' },
            { name: 'avgBhTrophies', type: 'REAL' },
            { name: 'avgThLevel', type: 'REAL' },
            { name: 'avgBhLevel', type: 'REAL' },
            { name: 'avgWarStars', type: 'REAL' },

            // Clan War Stats
            { name: 'warsWon', type: 'INTEGER' },
            { name: 'warsLost', type: 'INTEGER' },
            { name: 'warsTied', type: 'INTEGER' },

            // Clan Games Stats
            { name: 'CgTotalPoints', type: 'INTEGER' },
            { name: 'CgActivePlayers', type: 'INTEGER' },
            { name: 'CgMaxPlayers', type: 'INTEGER' },
            { name: 'CgAvgPoints', type: 'REAL' },

            // Clan Capital Stats
            { name: 'ccCapitalHallLvl', type: 'INTEGER' },
            { name: 'ccUpgrades', type: 'INTEGER' },
            { name: 'ccTrophies', type: 'INTEGER' },
            { name: 'ccAvgGoldDonated', type: 'REAL' }
        ],
        ['season', 'teamSeason']
    );

    // Table 15: C02_CwlSeasonStats
    // Primary database: A3_CWLWarDetails || A4_CWLAttackDetails
    // DB format: Stores summarized CWL history (monthly, right after CWL ends)
    // DB Ordering: season number
    
    createOrUpdateTable(
        db,
        'C02_CwlSeasonStats',
        [
            { name: 'season', type: 'TEXT PRIMARY KEY' },
            { name: 'teamSeason', type: 'INTEGER' },
            { name: 'clanLevel', type: 'INTEGER' },

            // CWL High Level Overview
            { name: 'cwlLeague', type: 'TEXT' },
            { name: 'cwlPosition', type: 'INTEGER' },
            { name: 'cwlPromotion', type: 'BOOLEAN' },
            { name: 'cwlWarSize', type: 'INTEGER' },
            { name: 'cwlWarsWon', type: 'INTEGER' },
            { name: 'cwlTotalWars', type: 'INTEGER' },

            // CWL Stats
            { name: 'cwlStarsEarned', type: 'INTEGER' },
            { name: 'cwlStarsMax', type: 'INTEGER' },
            { name: 'cwlStarsPercentage', type: 'REAL' },

            { name: 'cwlDamageEarned', type: 'INTEGER' },
            { name: 'cwlDamageMax', type: 'INTEGER' },
            { name: 'cwlDamagePercentage', type: 'REAL' },

            { name: 'cwlXpEarned', type: 'INTEGER' },

            { name: 'cwlAttacksUsed', type: 'INTEGER' },
            { name: 'cwlAttacksMax', type: 'INTEGER' },
            { name: 'cwlAttacksPercentage', type: 'REAL' },

            { name: 'cwlAvgStarsPerAttack', type: 'REAL' },
            { name: 'cwlAvgTeamThLevel', type: 'REAL' },

            //CWL Opponent Stats
            { name: 'cwlTotalAvgAttacks', type: 'REAL' },

            { name: 'cwlOpponentAttacks', type: 'INTEGER' },
            { name: 'cwlOpponentAttacksMax', type: 'INTEGER' },
            { name: 'cwlOpponentAttacksPercentage', type: 'REAL' },

            { name: 'cwlOpponentAvgStars', type: 'REAL' },
            { name: 'cwlOpponentAvgStarsMax', type: 'INTEGER' },
            { name: 'cwlOpponentAvgStarsPercentage', type: 'REAL' },
            { name: 'cwlTotalAvgStarsPerAttack', type: 'REAL' },

            { name: 'cwlTotalAvgDamage', type: 'REAL' },
            { name: 'cwlTotalAvgDamageMax', type: 'REAL' },
            { name: 'cwlTotalAvgDamagePercentage', type: 'REAL' },

            { name: 'cwlTotalAvgThLevel', type: 'REAL' }
        ],
        ['season', 'teamSeason']
    );

    // Table 16: C03_ClanWarSeasonStats
    // Primary database: A5_ClanWarLog || A6_ClanWarAttackDetails
    // DB format: Stores summarized clan war history (monthly, right before CWL signup ends)
    // DB Ordering: season number
    
    createOrUpdateTable(
        db,
        'C03_ClanWarSeasonStats',
        [
            { name: 'season', type: 'TEXT PRIMARY KEY' },
            { name: 'teamSeason', type: 'INTEGER' },
            { name: 'clanLevel', type: 'INTEGER' },

            // Clan War High Level Overview
            { name: 'totalWars', type: 'INTEGER' },
            { name: 'seasonWarsWon', type: 'INTEGER' },
            { name: 'seasonWarsLost', type: 'INTEGER' },
            { name: 'seasonWarsTied', type: 'INTEGER' },

            // Clan War Stats
            { name: 'totalStarsEarned', type: 'INTEGER' },
            { name: 'totalStarsMax', type: 'INTEGER' },
            { name: 'starsPercentage', type: 'REAL' },

            { name: 'totalAttacksUsed', type: 'INTEGER' },
            { name: 'totalAttacksMax', type: 'INTEGER' },
            { name: 'attacksPercentage', type: 'REAL' },

            { name: 'avgStarsPerAttack', type: 'REAL' },
            { name: 'avgNewStarsPerAttack', type: 'REAL' },
            { name: 'avgDamagePercentage', type: 'REAL' },

            { name: 'estXpEarned', type: 'INTEGER' },
            { name: 'avgXpPerWar', type: 'REAL' },

            // Opponent Stats
            { name: 'opponentStarsEarned', type: 'INTEGER' },
            { name: 'opponentStarsMax', type: 'INTEGER' },
            { name: 'opponentStarsPercentage', type: 'REAL' },

            { name: 'opponentAttacksUsed', type: 'INTEGER' },
            { name: 'opponentAttacksMax', type: 'INTEGER' },
            { name: 'opponentAttacksPercentage', type: 'REAL' },

            { name: 'avgOpponentStarsPerAttack', type: 'REAL' },
            { name: 'avgOpponrntNewStarsPerAttack', type: 'REAL' },
            { name: 'avgOpponentDamagePercentage', type: 'REAL' }
        ],
        ['season', 'teamSeason']
    );

    // Table 17: C04_WeekendRaidSeasonStats
    // Primary database: A7_ClanCapitalLog || A8_ClanCapitalAttacks || A9_ClanCapitalClanAttacks || A10_ClanCapitalClanDefenses
    // DB format: Stores summarized clan capital history (monthly, right before CWL signup ends)
    // DB Ordering: season number
    
    createOrUpdateTable(
        db,
        'C04_WeekendRaidSeasonStats',
        [
            { name: 'season', type: 'TEXT PRIMARY KEY' },
            { name: 'teamSeason', type: 'INTEGER' },
            { name: 'clanLevel', type: 'INTEGER' },

            //  Clan High Level Overview
            { name: 'clanCapitalHallLvl', type: 'INTEGER' },
            { name: 'clanCapitalUpgrades', type: 'INTEGER' },
            { name: 'clanCapitalTrophies', type: 'INTEGER' },
            { name: 'seasonRaidsCompleted', type: 'INTEGER' },

            // Weekend Raid Player Stats
            { name: 'seasonClanMembers', type: 'INTEGER' },
            { name: 'totalPlayers', type: 'INTEGER' },
            { name: 'avgPlayersPerRaid', type: 'REAL' },
            { name: 'totalMaxPlayers', type: 'INTEGER' },
            { name: 'percentagePlayersPerRaid', type: 'REAL' },

            // Weekend Raid Stats
            { name: 'trophiesGained', type: 'INTEGER' },
            { name: 'avgTrophiesGained', type: 'REAL' },

            { name: 'goldLootedTotal', type: 'INTEGER' },
            { name: 'goldLootedAvg', type: 'INTEGER' },
            { name: 'goldLootedAvgPerPlayer', type: 'REAL' },
            { name: 'goldLootedAvgPerAttack', type: 'REAL' },

            { name: 'attacksTotal', type: 'INTEGER' },
            { name: 'attacksTotalPerRaid', type: 'REAL' },
            { name: 'attacksAvgPerPlayer', type: 'REAL' },
            { name: 'attacksAvgPerPlayerPercentage', type: 'REAL' },

            { name: 'raidMedalsAttackTotal', type: 'INTEGER' },
            { name: 'raidMedalsDefenseTotal', type: 'INTEGER' },
            { name: 'raidMedalsTotal', type: 'INTEGER' },
            { name: 'raidMedalsAvg', type: 'REAL' },

            // Clans Stats
            { name: 'clansAttackedTotal', type: 'INTEGER' },
            { name: 'clansAttackedAvg', type: 'REAL' },
            { name: 'avgAttacksPerClan', type: 'REAL' },

            { name: 'clansDefendedTotal', type: 'INTEGER' },
            { name: 'clansDefendedAttacks', type: 'INTEGER' },
            { name: 'clansDefendedAttacksAvg', type: 'REAL' },

            // Clan Performance
            { name: 'attackPerformanceRatio', type: 'REAL' },
            { name: 'clansAttackDefenseRatio', type: 'REAL' }
        ],
        ['season', 'teamSeason']
    );

    // Table 18: C05_ThLeagueSeasonStats
    // Primary database: A2_ClanMembers
    // DB format: Stores count of number of players per league in the clan (weekly, right before ranked signup ends)
    // DB Ordering: season number
    
    createOrUpdateTable(
        db,
        'C05_ThLeagueSeasonStats',
        [
            { name: 'season', type: 'TEXT PRIMARY KEY' },
            { name: 'teamSeason', type: 'INTEGER' },
            { name: 'clanLevel', type: 'INTEGER' },

            { name: 'avgThLeague', type: 'REAL' },

            // Unranked
            { name: 'leagueCount0', type: 'INTEGER' },

            // Skeleton League
            { name: 'leagueCount1', type: 'INTEGER' },
            { name: 'leagueCount2', type: 'INTEGER' },
            { name: 'leagueCount3', type: 'INTEGER' },

            // Barbarian League
            { name: 'leagueCount4', type: 'INTEGER' },
            { name: 'leagueCount5', type: 'INTEGER' },
            { name: 'leagueCount6', type: 'INTEGER' },

            // Archer League
            { name: 'leagueCount7', type: 'INTEGER' },
            { name: 'leagueCount8', type: 'INTEGER' },
            { name: 'leagueCount9', type: 'INTEGER' },

            // Wizard League
            { name: 'leagueCount10', type: 'INTEGER' },
            { name: 'leagueCount11', type: 'INTEGER' },
            { name: 'leagueCount12', type: 'INTEGER' },

            // Valkyrie League
            { name: 'leagueCount13', type: 'INTEGER' },
            { name: 'leagueCount14', type: 'INTEGER' },
            { name: 'leagueCount15', type: 'INTEGER' },

            // Witch League
            { name: 'leagueCount16', type: 'INTEGER' },
            { name: 'leagueCount17', type: 'INTEGER' },
            { name: 'leagueCount18', type: 'INTEGER' },

            // Golem League
            { name: 'leagueCount19', type: 'INTEGER' },
            { name: 'leagueCount20', type: 'INTEGER' },
            { name: 'leagueCount21', type: 'INTEGER' },

            // P.E.K.K.A League
            { name: 'leagueCount22', type: 'INTEGER' },
            { name: 'leagueCount23', type: 'INTEGER' },
            { name: 'leagueCount24', type: 'INTEGER' },

            // Electro Titan League
            { name: 'leagueCount25', type: 'INTEGER' },
            { name: 'leagueCount26', type: 'INTEGER' },
            { name: 'leagueCount27', type: 'INTEGER' },

            // Dragon League
            { name: 'leagueCount28', type: 'INTEGER' },
            { name: 'leagueCount29', type: 'INTEGER' },
            { name: 'leagueCount30', type: 'INTEGER' },

            // Electro Dragon League
            { name: 'leagueCount31', type: 'INTEGER' },
            { name: 'leagueCount32', type: 'INTEGER' },
            { name: 'leagueCount33', type: 'INTEGER' },

            // Legend League
            { name: 'leagueCount34', type: 'INTEGER' }
        ],
        ['season', 'teamSeason', 'avgThLeague']
    );

    // Table 19: C06_BhLeagueSeasonStats
    // Primary database: A2_ClanMembers
    // DB format: Stores count of number of players per builder base league in the clan (monthly, right before CWL signup ends)
    // DB Ordering: season number
    
    createOrUpdateTable(
        db,
        'C06_BhLeagueSeasonStats',
        [
            { name: 'season', type: 'TEXT PRIMARY KEY' },
            { name: 'teamSeason', type: 'INTEGER' },
            { name: 'clanLevel', type: 'INTEGER' },

            { name: 'avgBhLeague', type: 'REAL' },

            // Unranked
            { name: 'leagueCount0', type: 'INTEGER' },

            // Sub-2000 Leagues
            { name: 'leagueCountWood5', type: 'INTEGER' },
            { name: 'leagueCountWood4', type: 'INTEGER' },
            { name: 'leagueCountWood3', type: 'INTEGER' },
            { name: 'leagueCountWood2', type: 'INTEGER' },
            { name: 'leagueCountWood1', type: 'INTEGER' },

            { name: 'leagueCountClay5', type: 'INTEGER' },
            { name: 'leagueCountClay4', type: 'INTEGER' },
            { name: 'leagueCountClay3', type: 'INTEGER' },
            { name: 'leagueCountClay2', type: 'INTEGER' },
            { name: 'leagueCountClay1', type: 'INTEGER' },

            { name: 'leagueCountStone5', type: 'INTEGER' },
            { name: 'leagueCountStone4', type: 'INTEGER' },
            { name: 'leagueCountStone3', type: 'INTEGER' },
            { name: 'leagueCountStone2', type: 'INTEGER' },
            { name: 'leagueCountStone1', type: 'INTEGER' },

            { name: 'leagueCountCopper5', type: 'INTEGER' },
            { name: 'leagueCountCopper4', type: 'INTEGER' },
            { name: 'leagueCountCopper3', type: 'INTEGER' },
            { name: 'leagueCountCopper2', type: 'INTEGER' },
            { name: 'leagueCountCopper1', type: 'INTEGER' },

            // Crystal to Titan Leagues
            { name: 'leagueCountBrass3', type: 'INTEGER' },
            { name: 'leagueCountBrass2', type: 'INTEGER' },
            { name: 'leagueCountBrass1', type: 'INTEGER' },

            { name: 'leagueCountIron3', type: 'INTEGER' },
            { name: 'leagueCountIron2', type: 'INTEGER' },
            { name: 'leagueCountIron1', type: 'INTEGER' },

            { name: 'leagueCountSteel3', type: 'INTEGER' },
            { name: 'leagueCountSteel2', type: 'INTEGER' },
            { name: 'leagueCountSteel1', type: 'INTEGER' },

            { name: 'leagueCountTitanium3', type: 'INTEGER' },
            { name: 'leagueCountTitanium2', type: 'INTEGER' },
            { name: 'leagueCountTitanium1', type: 'INTEGER' },

            { name: 'leagueCountPlatinum3', type: 'INTEGER' },
            { name: 'leagueCountPlatinum2', type: 'INTEGER' },
            { name: 'leagueCountPlatinum1', type: 'INTEGER' },

            // Legend Leagues
            { name: 'leagueCountEmerald3', type: 'INTEGER' },
            { name: 'leagueCountEmerald2', type: 'INTEGER' },
            { name: 'leagueCountEmerald1', type: 'INTEGER' },

            { name: 'leagueCountRuby3', type: 'INTEGER' },
            { name: 'leagueCountRuby2', type: 'INTEGER' },
            { name: 'leagueCountRuby1', type: 'INTEGER' },

            { name: 'leagueCountDiamond', type: 'INTEGER' }
        ],
        ['season', 'teamSeason', 'avgBhLeague']
    );

    // Table 20: C07_ThCountSeasonStats
    // Primary database: A2_ClanMembers
    // DB format: Stores count of number of players THs in the clan (monthly, right before CWL signup ends)
    // DB Ordering: season number
    
    createOrUpdateTable(
        db,
        'C07_ThCountSeasonStats',
        [
            { name: 'season', type: 'TEXT PRIMARY KEY' },
            { name: 'teamSeason', type: 'INTEGER' },
            { name: 'clanLevel', type: 'INTEGER' },

            { name: 'avgThLevel', type: 'REAL' },

            // Town Hall Levels
            { name: 'countTh1', type: 'INTEGER' },
            { name: 'countTh2', type: 'INTEGER' },
            { name: 'countTh3', type: 'INTEGER' },
            { name: 'countTh4', type: 'INTEGER' },
            { name: 'countTh5', type: 'INTEGER' },
            { name: 'countTh6', type: 'INTEGER' },
            { name: 'countTh7', type: 'INTEGER' },
            { name: 'countTh8', type: 'INTEGER' },
            { name: 'countTh9', type: 'INTEGER' },
            { name: 'countTh10', type: 'INTEGER' },
            { name: 'countTh11', type: 'INTEGER' },
            { name: 'countTh12', type: 'INTEGER' },
            { name: 'countTh13', type: 'INTEGER' },
            { name: 'countTh14', type: 'INTEGER' },
            { name: 'countTh15', type: 'INTEGER' },
            { name: 'countTh16', type: 'INTEGER' },
            { name: 'countTh17', type: 'INTEGER' },
            { name: 'countTh18', type: 'INTEGER' }            
        ],
        ['season', 'teamSeason', 'avgThLevel']
    );

    // Table 21: C08_BhCountSeasonStats
    // Primary database: A2_ClanMembers
    // DB format: Stores count of number of players BHs in the clan (monthly, right before CWL signup ends)
    // DB Ordering: season number
    
    createOrUpdateTable(
        db,
        'C08_BhCountSeasonStats',
        [
            { name: 'season', type: 'TEXT PRIMARY KEY' },
            { name: 'teamSeason', type: 'INTEGER' },
            { name: 'clanLevel', type: 'INTEGER' },

            { name: 'avgBhLevel', type: 'REAL' },

            // Builder Hall Levels
            { name: 'countBh1', type: 'INTEGER' },
            { name: 'countBh2', type: 'INTEGER' },
            { name: 'countBh3', type: 'INTEGER' },
            { name: 'countBh4', type: 'INTEGER' },
            { name: 'countBh5', type: 'INTEGER' },
            { name: 'countBh6', type: 'INTEGER' },
            { name: 'countBh7', type: 'INTEGER' },
            { name: 'countBh8', type: 'INTEGER' },
            { name: 'countBh9', type: 'INTEGER' },
            { name: 'countBh10', type: 'INTEGER' }
        ],
        ['season', 'teamSeason', 'avgBhLevel']
    );

    // Table 22: D01_Achievements
    // Primary database: A1_ClanInfo
    // DB format: Stores achievements of the clan (updated whenever the relevant achievement is obtained; formatted per achievement)
    // DB Ordering: order of Achievement
    
    createOrUpdateTable(
        db,
        'D01_Achievements',
        [
            { name: 'id', type: 'INTEGER PRIMARY KEY AUTOINCREMENT' },
            
            { name: 'dateLogged', type: 'TEXT' },
            { name: 'achievementName', type: 'TEXT' },
            { name: 'achievementInt', type: 'INTEGER' }   
        ],
        ['dateLogged', 'achievementName']
    );
}

// Get db name from command line argument
const dbFileName = "TEMPORARYNAME.db";
if (!dbFileName) {
    console.error('Please specify a database file name.');
    process.exit(1);
}

createDatabase(dbFileName);