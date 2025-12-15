const fs = require('fs');
const path = require('path');

/**
 * Creates or updates a .env file in the project root directory.
 * @param {string} content - The content to write to the .env file.
 */

function createOrUpdateEnvFile(content) {
    // Get the current working directory (assumed to be project root)
    const projectRoot = process.cwd();
    const envFile = path.join(projectRoot, '.env');
    fs.writeFileSync(envFile, content, { encoding: 'utf8', flag: 'w' });
}

// Interactive prompt for cocApiKey with error handling and retry
const readline = require('readline');


/**
 * Prompts the user for a value for a given key, with validation and error handling.
 * @param {string} key - The environment variable key.
 * @param {string} promptText - The prompt to display to the user.
 * @param {(input: string) => boolean} validate - Validation function for the input.
 * @returns {Promise<string>} - The validated input value.
 */

function promptForEnvKey(key, promptText, validate) {
    return new Promise((resolve) => {
        const rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });
        rl.question(promptText, (input) => {
            rl.close();
            const value = input.trim();
            if (!validate(value)) {
                console.error(`Error: Value for ${key} is invalid. Please try again.`);
                resolve(null);
            } else {
                resolve(value);
            }
        });
    });
}

/**
 * Handles prompting and error handling for multiple environment keys.
 * @param {Array<{key: string, promptText: string, validate: (input: string) => boolean}>} keys
 * @returns {Promise<Object>} - Object with key-value pairs.
 */

async function promptForEnvKeys(keys) {
    const results = {};

    for (const { key, promptText, validate } of keys) {
        let value = null;

        while (value === null) {
            value = await promptForEnvKey(key, promptText, validate);
        }

        results[key] = value;
    }

    return results;
}

async function main() {
    const keysToPrompt = [
        {
            key: 'API_COC_KEY',
            promptText: 'Enter your Clash of Clans API Key: ',
            validate: (input) => !!input
        },
        
        {
            key: 'DISCORD_BOT_TOKEN',
            promptText: 'Enter your Discord Bot Token: ',
            validate: (input) => !!input
        },
        
        {
            key: 'DISCORD_LOG_CHANNELID',
            promptText: 'Enter your Discord Log Channel ID: ',
            validate: (input) => !!input && /^\d+$/.test(input)
        },
        
        {
            key: 'DISCORD_ADMIN_USERID',
            promptText: 'Enter your Discord Admin User ID: ',
            validate: (input) => !!input && /^\d+$/.test(input)
        }
    ];

    const envValues = await promptForEnvKeys(keysToPrompt);
    let envContent = '';

    for (const [key, value] of Object.entries(envValues)) {
        envContent += `${key}=${value}\n`;
    }

    createOrUpdateEnvFile(envContent);
    console.log('.env file created/updated in project root.');
}

main();

module.exports = { createOrUpdateEnvFile };