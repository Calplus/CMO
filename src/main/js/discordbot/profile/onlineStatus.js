const { Client, GatewayIntentBits } = require('discord.js');
const fs = require('fs');
const path = require('path');
const DiscordLog = require('../logs/discordLog');

/**
 * Loads configuration from .env file
 * @returns {Object} Configuration object with properties
 */
function loadConfig() {
    // JavaScript runs externally, so load from .env file
    let configPath = path.resolve(__dirname, '../../../../../.env');
    
    // Fallback to .env.example if .env doesn't exist
    if (!fs.existsSync(configPath)) {
        configPath = path.resolve(__dirname, '../../../../../.env.example');
        
        if (!fs.existsSync(configPath)) {
            console.error('ERROR: .env file not found.');
            console.error('Please copy .env.example to .env and fill in your values.');
            return null;
        }
    }

    try {
        const configContent = fs.readFileSync(configPath, 'utf8');
        const configLines = configContent.split('\n');
        
        const config = {};
        configLines.forEach(line => {
            // Skip comments and empty lines
            if (line.trim().startsWith('#') || !line.trim()) {
                return;
            }
            
            const [key, ...valueParts] = line.split('=');
            if (key && valueParts.length > 0) {
                config[key.trim()] = valueParts.join('=').trim();
            }
        });

        return config;
    } catch (error) {
        console.error(`ERROR: Failed to read .env file: ${error.message}`);
        return null;
    }
}

// Load configuration
const config = loadConfig();
if (!config) {
    process.exit(1);
}

// Create Discord logger instance
const discordLogger = new DiscordLog();

// Create a new Discord client
const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
    ]
});

// Event: Bot is ready and online
client.once('ready', () => {
    discordLogger.logSuccess('Discord bot is now online!');
    discordLogger.logInfo(`Logged in as: ${client.user.tag}`);
    discordLogger.logInfo(`Bot ID: ${client.user.id}`);
    discordLogger.logInfo(`Serving ${client.guilds.cache.size} server(s)`);
    
    // Set custom status (optional)
    client.user.setPresence({
        status: 'online',
        activities: [{
            name: 'Playing with BMO :D',
            type: 0 // 0 = Playing, 1 = Streaming, 2 = Listening, 3 = Watching
        }]
    });
});

// Event: Handle errors
client.on('error', (error) => {
    discordLogger.logError(`Discord client error: ${error.message}`);
});

// Event: Handle warnings
client.on('warn', (warning) => {
    discordLogger.logWarning(`Discord client warning: ${warning}`);
});

// Event: Bot disconnected
client.on('disconnect', () => {
    discordLogger.logWarning('Bot disconnected from Discord');
});

// Event: Bot reconnecting
client.on('reconnecting', () => {
    discordLogger.logInfo('Bot reconnecting to Discord...');
});

// Login to Discord
const token = config.DISCORD_BOT_TOKEN;

if (!token) {
    discordLogger.logError('DISCORD_BOT_TOKEN not found in .env file');
    process.exit(1);
}

discordLogger.logInfo('Starting Discord bot...');
client.login(token).catch((error) => {
    discordLogger.logError(`Failed to login to Discord: ${error.message}`);
    process.exit(1);
});

// Handle process termination
process.on('SIGINT', () => {
    discordLogger.logInfo('Shutting down Discord bot...');
    client.destroy();
    process.exit(0);
});

process.on('SIGTERM', () => {
    discordLogger.logInfo('Shutting down Discord bot...');
    client.destroy();
    process.exit(0);
});
