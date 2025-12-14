const { Client, GatewayIntentBits } = require('discord.js');
const dotenv = require('dotenv');
const path = require('path');
const DiscordLog = require('../logs/discordLog');

// Load environment variables from .env file in project root
dotenv.config({ path: path.resolve(__dirname, '../../../../../.env') });

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
const token = process.env.DISCORD_BOT_TOKEN;

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
