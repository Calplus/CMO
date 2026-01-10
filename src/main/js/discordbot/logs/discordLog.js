const https = require('https');
const fs = require('fs');
const path = require('path');

/**
 * Sends log messages to a Discord channel via the Discord bot.
 * Messages are queued and processed sequentially to maintain order.
 * Reacts to Discord rate limits (429) with retry_after delays.
 */
class DiscordLog {
    constructor() {
        this.discordEnabled = this.loadConfig();
        this.messageQueue = [];
        this.isProcessing = false;
        
        // Batch message handling
        this.batchBuffer = [];
        this.batchStartTime = 0;
        this.batchTimeoutMs = 5000; // 5 seconds
        
        // INFO message batching - accumulate INFO logs until a terminal log type (SUCCESS/ERROR/WARNING)
        this.infoBatchBuffer = [];
        
        // Add process exit handler to ensure all messages are sent
        if (this.discordEnabled) {
            process.on('exit', () => {
                this.flushSync();
            });
            
            // Handle graceful shutdown signals
            process.on('SIGINT', () => {
                this.flush().then(() => process.exit(0));
            });
            process.on('SIGTERM', () => {
                this.flush().then(() => process.exit(0));
            });
        }
    }

    /**
     * Loads the Discord bot token and channel ID from .env file
     * @returns {boolean} - True if Discord logging is enabled, false otherwise
     */
    loadConfig() {
        // JavaScript runs externally, so load from .env file
        let configPath = path.resolve(__dirname, '../../../../../.env');
        
        // Fallback to .env.example if .env doesn't exist
        if (!fs.existsSync(configPath)) {
            configPath = path.resolve(__dirname, '../../../../../.env.example');
            
            if (!fs.existsSync(configPath)) {
                console.error('WARNING: .env file not found. Discord logging disabled.');
                console.error('Please copy .env.example to .env and fill in your values.');
                return false;
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

            // Load using .env key format
            this.botToken = config['DISCORD_BOT_TOKEN'];
            this.channelId = config['DISCORD_LOG_CHANNELID'];
            this.adminUserId = config['DISCORD_ADMIN_USERID'];

            if (!this.botToken) {
                console.error('WARNING: DISCORD_BOT_TOKEN not found in .env file. Discord logging disabled.');
                return false;
            }
            if (!this.channelId) {
                console.error('WARNING: DISCORD_LOG_CHANNELID not found in .env file. Discord logging disabled.');
                return false;
            }
            
            if (!this.adminUserId) {
                console.error('INFO: DISCORD_ADMIN_USERID not configured. Admin pings will be skipped.');
            }

            this.discordApiUrl = `/api/v10/channels/${this.channelId}/messages`;
            return true;
        } catch (error) {
            console.error(`WARNING: Failed to read configuration file. Discord logging disabled. Error: ${error.message}`);
            return false;
        }
    }

    /**
     * Gets the caller's filename from the stack trace
     * @returns {string} - The filename of the caller
     */
    getCallerFilename() {
        const originalPrepareStackTrace = Error.prepareStackTrace;
        Error.prepareStackTrace = (_, stack) => stack;
        const stack = new Error().stack;
        Error.prepareStackTrace = originalPrepareStackTrace;

        // Look through the stack to find the first external caller
        for (let i = 0; i < stack.length; i++) {
            const caller = stack[i];
            const fileName = caller.getFileName();
            
            // Skip internal Node.js modules, this file, and async wrappers
            if (fileName && 
                !fileName.includes('node:') && 
                !fileName.includes('discordLog.js') &&
                !fileName.includes('internal/')) {
                return path.basename(fileName);
            }
        }
        
        return 'CLI';
    }

    /**
     * Formats timestamp with milliseconds
     * @returns {string} - Formatted timestamp
     */
    getTimestamp() {
        const now = new Date();
        const year = now.getFullYear();
        const month = String(now.getMonth() + 1).padStart(2, '0');
        const day = String(now.getDate()).padStart(2, '0');
        const hours = String(now.getHours()).padStart(2, '0');
        const minutes = String(now.getMinutes()).padStart(2, '0');
        const seconds = String(now.getSeconds()).padStart(2, '0');
        const milliseconds = String(now.getMilliseconds()).padStart(3, '0');
        
        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
    }

    /**
     * Formats a log message with emote, timestamp, filename, type, and message
     * @param {string} emote - The emote to use
     * @param {string} type - The log type (INFO, SUCCESS, ERROR, etc.)
     * @param {string} message - The message to log
     * @param {string} filename - The calling file
     * @returns {string} - Formatted message
     */
    formatMessage(emote, type, message, filename) {
        const timestamp = this.getTimestamp();
        return `${emote} [${timestamp}] [${filename}] ${type}: ${message}`;
    }

    /**
     * Sends a message to the Discord channel
     * @param {string} message - The message to send
     * @returns {Promise<number>} - 0 if successful, -1 if failed, >0 for retry delay in ms
     */
    sendMessage(message) {
        return new Promise((resolve) => {
            const payload = JSON.stringify({ content: message });

            const options = {
                hostname: 'discord.com',
                port: 443,
                path: this.discordApiUrl,
                method: 'POST',
                headers: {
                    'Authorization': `Bot ${this.botToken}`,
                    'Content-Type': 'application/json',
                    'Content-Length': Buffer.byteLength(payload)
                }
            };

            const req = https.request(options, (res) => {
                let data = '';

                res.on('data', (chunk) => {
                    data += chunk;
                });

                res.on('end', () => {
                    if (res.statusCode === 200 || res.statusCode === 201) {
                        resolve(0); // Success
                    } else if (res.statusCode === 429) {
                        // Rate limited - parse retry_after
                        console.error(`Failed to send message to Discord. Status code: ${res.statusCode}`);
                        console.error(`Response: ${data}`);
                        
                        try {
                            const response = JSON.parse(data);
                            if (response.retry_after) {
                                const retryAfterMs = Math.ceil(response.retry_after * 1000);
                                resolve(retryAfterMs);
                                return;
                            }
                        } catch (e) {
                            console.error(`Error parsing retry_after: ${e.message}`);
                        }
                        
                        // Default to 1 second if parsing fails
                        resolve(1000);
                    } else {
                        console.error(`Failed to send message to Discord. Status code: ${res.statusCode}`);
                        console.error(`Response: ${data}`);
                        resolve(-1); // Failed
                    }
                });
            });

            req.on('error', (error) => {
                console.error(`Error sending message to Discord: ${error.message}`);
                resolve(-1); // Failed
            });

            req.write(payload);
            req.end();
        });
    }

    /**
     * Processes the message queue sequentially with reactive rate limiting
     */
    async processQueue() {
        if (this.isProcessing || this.messageQueue.length === 0) {
            return;
        }

        this.isProcessing = true;

        try {
            while (this.messageQueue.length > 0) {
                const queueItem = this.messageQueue.shift();
                if (!queueItem) continue;
                
                const { message, resolve } = queueItem;
                
                try {
                    // Try to send message, handle rate limiting
                    let result = await this.sendMessage(message);
                    
                    while (result > 0) {
                        // Rate limited - wait for retry_after duration
                        await new Promise(resolveWait => setTimeout(resolveWait, result));
                        // Retry sending the message
                        result = await this.sendMessage(message);
                    }
                    
                    // Complete promise: result == 0 means success, result == -1 means failure
                    resolve(result === 0);
                } catch (error) {
                    // Complete the promise with failure and continue processing
                    console.error(`Error processing message: ${error.message}`);
                    resolve(false);
                }
            }
        } catch (error) {
            console.error(`Fatal error in queue processing: ${error.message}`);
        } finally {
            this.isProcessing = false;
            
            // Check if new messages arrived while we were finishing
            if (this.messageQueue.length > 0) {
                // Use setImmediate to avoid recursion depth issues
                setImmediate(() => this.processQueue());
            }
        }
    }
    
    /**
     * Waits for all queued messages to be sent (async)
     * @returns {Promise<void>}
     */
    async flush() {
        while (this.messageQueue.length > 0 || this.isProcessing) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }
    
    /**
     * Synchronous flush for process.on('exit') handler
     * Uses busy-wait since async operations don't work in exit handler
     */
    flushSync() {
        const start = Date.now();
        const timeout = 5000; // 5 second timeout
        
        while ((this.messageQueue.length > 0 || this.isProcessing) && (Date.now() - start < timeout)) {
            // Busy wait - not ideal but necessary for synchronous exit handler
            const end = Date.now() + 10;
            while (Date.now() < end) {}
        }
    }

    /**
     * Adds a message to the queue and processes it
     * @param {string} message - The message to queue
     * @returns {Promise<boolean>} - True if successful, false otherwise
     */
    queueMessage(message) {
        return new Promise((resolve) => {
            // If Discord is disabled, resolve immediately with false
            if (!this.discordEnabled) {
                resolve(false);
                return;
            }
            
            this.messageQueue.push({ message, resolve });
            this.processQueue();
        });
    }

    /**
     * Sends a log message with timestamp to the Discord channel
     * @param {string} message - The log message to send
     * @returns {Promise<boolean>} - True if successful, false otherwise
     */
    async log(message) {
        const filename = this.getCallerFilename();
        const formattedMessage = this.formatMessage('📝', 'LOG', message, filename);
        console.log(formattedMessage);
        return await this.queueMessage(formattedMessage);
    }

    /**
     * Sends an error log message to the Discord channel with admin user ping
     * @param {string} message - The error message to send
     * @returns {Promise<boolean>} - True if successful, false otherwise
     */
    async logError(message) {
        const filename = this.getCallerFilename();
        let formattedMessage = this.formatMessage('🔴', 'ERROR', message, filename);
        
        // Add admin user ping if configured
        if (this.adminUserId) {
            formattedMessage = `<@${this.adminUserId}> ${formattedMessage}`;
        }
        
        console.error(formattedMessage);
        
        // Flush any accumulated INFO messages before sending error
        this.flushInfoBatch();
        
        return await this.queueMessage(formattedMessage);
    }

    /**
     * Sends a success log message to the Discord channel
     * @param {string} message - The success message to send
     * @returns {Promise<boolean>} - True if successful, false otherwise
     */
    async logSuccess(message) {
        const filename = this.getCallerFilename();
        const formattedMessage = this.formatMessage('🟢', 'SUCCESS', message, filename);
        console.log(formattedMessage);
        
        // Combine accumulated INFO messages with success message
        const combinedMessage = this.combineInfoBatchWithMessage(formattedMessage);
        
        return await this.queueMessage(combinedMessage);
    }

    /**
     * Sends a warning log message to the Discord channel
     * @param {string} message - The warning message to send
     * @returns {Promise<boolean>} - True if successful, false otherwise
     */
    async logWarning(message) {
        const filename = this.getCallerFilename();
        const formattedMessage = this.formatMessage('🟡', 'WARNING', message, filename);
        console.warn(formattedMessage);
        
        // Combine accumulated INFO messages with warning message
        const combinedMessage = this.combineInfoBatchWithMessage(formattedMessage);
        
        return await this.queueMessage(combinedMessage);
    }

    /**
     * Adds an info log message to the INFO batch buffer
     * INFO messages are accumulated and sent together with the next SUCCESS/ERROR/WARNING message
     * @param {string} message - The info message to add
     * @returns {Promise<boolean>} - True (immediately)
     */
    async logInfo(message) {
        const filename = this.getCallerFilename();
        const formattedMessage = this.formatMessage('🔵', 'INFO', message, filename);
        console.log(formattedMessage);
        
        this.infoBatchBuffer.push(formattedMessage);
        
        return true;
    }

    /**
     * Flushes any accumulated INFO messages without sending them
     * (Used internally when error occurs to clear the buffer)
     */
    flushInfoBatch() {
        this.infoBatchBuffer = [];
    }
    
    /**
     * Combines accumulated INFO messages with a terminal message (SUCCESS/ERROR/WARNING)
     * and clears the INFO buffer
     * @param {string} terminalMessage - The terminal message to append
     * @returns {string} - Combined message with all INFO logs followed by the terminal message
     */
    combineInfoBatchWithMessage(terminalMessage) {
        if (this.infoBatchBuffer.length === 0) {
            return terminalMessage;
        }
        const combined = this.infoBatchBuffer.join('\n') + '\n' + terminalMessage;
        this.infoBatchBuffer = [];
        return combined;
    }
    
    /**
     * Adds a log message to the batch buffer
     * @param {string} message - The log message to add to batch
     */
    batchLog(message) {
        const filename = this.getCallerFilename();
        const formattedMessage = this.formatMessage('📝', 'LOG', message, filename);
        console.log(formattedMessage);
        this.addToBatch(formattedMessage);
    }

    /**
     * Adds an info log message to the batch buffer
     * @param {string} message - The info message to add to batch
     */
    batchInfo(message) {
        const filename = this.getCallerFilename();
        const formattedMessage = this.formatMessage('🔵', 'INFO', message, filename);
        console.log(formattedMessage);
        this.addToBatch(formattedMessage);
    }

    /**
     * Adds a success log message to the batch buffer
     * @param {string} message - The success message to add to batch
     */
    batchSuccess(message) {
        const filename = this.getCallerFilename();
        const formattedMessage = this.formatMessage('🟢', 'SUCCESS', message, filename);
        console.log(formattedMessage);
        this.addToBatch(formattedMessage);
    }

    /**
     * Adds a warning log message to the batch buffer
     * @param {string} message - The warning message to add to batch
     */
    batchWarning(message) {
        const filename = this.getCallerFilename();
        const formattedMessage = this.formatMessage('🟡', 'WARNING', message, filename);
        console.log(formattedMessage);
        this.addToBatch(formattedMessage);
    }

    /**
     * Adds a message to the batch buffer with timeout check
     * @param {string} formattedMessage - The formatted message to add
     */
    addToBatch(formattedMessage) {
        const isFirstMessage = this.batchBuffer.length === 0;
        
        if (isFirstMessage) {
            this.batchStartTime = Date.now();
        } else {
            // Check if batch timeout exceeded
            const elapsed = Date.now() - this.batchStartTime;
            if (elapsed >= this.batchTimeoutMs) {
                // Flush existing batch first
                this.flushBatchInternal();
                // Start new batch
                this.batchStartTime = Date.now();
            }
        }
        
        this.batchBuffer.push(formattedMessage);
    }

    /**
     * Sends all batched messages immediately
     * @returns {Promise<boolean>} - True if successful, false otherwise
     */
    async flushBatch() {
        return await this.flushBatchInternal();
    }

    /**
     * Internal method to flush batch
     * @returns {Promise<boolean>} - True if successful, false otherwise
     */
    async flushBatchInternal() {
        if (this.batchBuffer.length === 0) {
            return true;
        }
        
        const batchContent = this.batchBuffer.join('\n');
        this.batchBuffer = [];
        this.batchStartTime = 0;
        
        const DISCORD_CHARACTER_LIMIT = 2000;
        
        // Split message if it exceeds Discord's character limit
        if (batchContent.length <= DISCORD_CHARACTER_LIMIT) {
            return await this.queueMessage(batchContent);
        } else {
            // Split into chunks
            let result = true;
            let start = 0;
            while (start < batchContent.length) {
                let end = Math.min(start + DISCORD_CHARACTER_LIMIT, batchContent.length);
                
                // Try to split at newline if possible
                if (end < batchContent.length) {
                    const lastNewline = batchContent.lastIndexOf('\n', end);
                    if (lastNewline > start) {
                        end = lastNewline;
                    }
                }
                
                const chunk = batchContent.substring(start, end);
                result = result && await this.queueMessage(chunk);
                start = end + (end < batchContent.length && batchContent.charAt(end) === '\n' ? 1 : 0);
            }
            return result;
        }
    }
}

// Main function for CLI usage
async function main() {
    const args = process.argv.slice(2);
    
    if (args.length === 0) {
        // Test mode
        const discordLog = new DiscordLog();
        await discordLog.logInfo('Testing Discord logging from JavaScript');
        await discordLog.logSuccess('Database update completed successfully');
        await discordLog.logWarning('API rate limit approaching');
        await discordLog.logError('Failed to connect to database');
        await discordLog.log('Custom log message without prefix');
        return;
    }
    
    // CLI mode: node discordLog.js <logType> <message>
    const logType = args[0].toLowerCase();
    const message = args.slice(1).join(' ');
    
    if (!message) {
        console.error('Error: Message is required');
        process.exit(1);
    }
    
    const discordLog = new DiscordLog();
    
    try {
        switch (logType) {
            case 'info':
                await discordLog.logInfo(message);
                break;
            case 'success':
                await discordLog.logSuccess(message);
                break;
            case 'warning':
                await discordLog.logWarning(message);
                break;
            case 'error':
                await discordLog.logError(message);
                break;
            case 'log':
                await discordLog.log(message);
                break;
            default:
                console.error(`Error: Unknown log type '${logType}'. Use: info, success, warning, error, or log`);
                process.exit(1);
        }
    } catch (error) {
        console.error(`Error logging to Discord: ${error.message}`);
        process.exit(1);
    }
}

// Run main if executed directly
if (require.main === module) {
    main();
}

module.exports = DiscordLog;
