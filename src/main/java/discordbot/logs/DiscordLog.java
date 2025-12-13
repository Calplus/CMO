package discordbot.logs;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sends log messages to a Discord channel via the Discord bot.
 * Messages are queued and processed sequentially to maintain order.
 */
public class DiscordLog {
    private String botToken;
    private String channelId;
    private String discordApiUrl;
    private final BlockingQueue<QueuedMessage> messageQueue;
    private final AtomicBoolean isProcessing;
    private final HttpClient httpClient;

    private static class QueuedMessage {
        String message;
        CompletableFuture<Boolean> future;

        QueuedMessage(String message, CompletableFuture<Boolean> future) {
            this.message = message;
            this.future = future;
        }
    }

    public DiscordLog() {
        this.messageQueue = new LinkedBlockingQueue<>();
        this.isProcessing = new AtomicBoolean(false);
        this.httpClient = HttpClient.newHttpClient();
        loadConfig();
    }

    /**
     * Loads the Discord bot token and channel ID from the .env file
     */
    private void loadConfig() {
        Path envPath = Paths.get(System.getProperty("user.dir"), ".env");

        if (!Files.exists(envPath)) {
            throw new RuntimeException(".env file not found at: " + envPath);
        }

        try {
            Map<String, String> config = new HashMap<>();
            Files.lines(envPath).forEach(line -> {
                String[] parts = line.split("=", 2);
                if (parts.length == 2) {
                    config.put(parts[0].trim(), parts[1].trim());
                }
            });

            this.botToken = config.get("DISCORD_BOT_TOKEN");
            this.channelId = config.get("DISCORD_LOG_CHANNELID");

            if (this.botToken == null || this.botToken.isEmpty()) {
                throw new RuntimeException("DISCORD_BOT_TOKEN not found in .env file");
            }
            if (this.channelId == null || this.channelId.isEmpty()) {
                throw new RuntimeException("DISCORD_LOG_CHANNELID not found in .env file");
            }

            this.discordApiUrl = "https://discord.com/api/v10/channels/" + this.channelId + "/messages";

        } catch (IOException e) {
            throw new RuntimeException("Failed to read .env file", e);
        }
    }

    /**
     * Gets the caller's filename from the stack trace
     * @return The filename of the caller
     */
    private String getCallerFilename() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

        // Skip internal calls and find the first external caller
        for (StackTraceElement element : stackTrace) {
            String className = element.getClassName();
            String fileName = element.getFileName();

            // Skip Thread, DiscordLog, and internal classes
            if (fileName != null && 
                !className.equals("java.lang.Thread") &&
                !className.equals("discordbot.logs.DiscordLog") &&
                !className.startsWith("java.") &&
                !className.startsWith("sun.")) {
                return fileName;
            }
        }

        return "CLI";
    }

    /**
     * Formats timestamp with milliseconds
     * @return Formatted timestamp
     */
    private String getTimestamp() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        return now.format(formatter);
    }

    /**
     * Formats a log message with emote, timestamp, filename, type, and message
     * @param emote The emote to use
     * @param type The log type (INFO, SUCCESS, ERROR, etc.)
     * @param message The message to log
     * @param filename The calling file
     * @return Formatted message
     */
    private String formatMessage(String emote, String type, String message, String filename) {
        String timestamp = getTimestamp();
        return String.format("%s [%s] [%s] %s: %s", emote, timestamp, filename, type, message);
    }

    /**
     * Sends a message to the Discord channel
     * @param message The message to send
     * @return True if successful, false otherwise
     */
    private boolean sendMessage(String message) {
        try {
            String payload = String.format("{\"content\":\"%s\"}", 
                message.replace("\\", "\\\\")
                       .replace("\"", "\\\"")
                       .replace("\n", "\\n")
                       .replace("\r", "\\r")
                       .replace("\t", "\\t"));

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(this.discordApiUrl))
                    .header("Authorization", "Bot " + this.botToken)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200 || response.statusCode() == 201) {
                return true;
            } else {
                System.err.println("Failed to send message to Discord. Status code: " + response.statusCode());
                System.err.println("Response: " + response.body());
                return false;
            }

        } catch (Exception e) {
            System.err.println("Error sending message to Discord: " + e.getMessage());
            return false;
        }
    }

    /**
     * Processes the message queue sequentially
     */
    private void processQueue() {
        if (!isProcessing.compareAndSet(false, true)) {
            return;
        }

        CompletableFuture.runAsync(() -> {
            try {
                while (!messageQueue.isEmpty()) {
                    QueuedMessage queuedMsg = messageQueue.poll();
                    if (queuedMsg != null) {
                        boolean success = sendMessage(queuedMsg.message);
                        queuedMsg.future.complete(success);
                    }
                }
            } finally {
                isProcessing.set(false);
                // Check if new messages arrived while we were finishing
                if (!messageQueue.isEmpty()) {
                    processQueue();
                }
            }
        });
    }

    /**
     * Adds a message to the queue and processes it
     * @param message The message to queue
     * @return CompletableFuture that resolves to true if successful, false otherwise
     */
    private CompletableFuture<Boolean> queueMessage(String message) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        messageQueue.add(new QueuedMessage(message, future));
        processQueue();
        return future;
    }

    /**
     * Sends a log message with timestamp to the Discord channel
     * @param message The log message to send
     * @return CompletableFuture that resolves to true if successful, false otherwise
     */
    public CompletableFuture<Boolean> log(String message) {
        String filename = getCallerFilename();
        String formattedMessage = formatMessage("📝", "LOG", message, filename);
        System.out.println(formattedMessage);
        return queueMessage(formattedMessage);
    }

    /**
     * Sends an error log message to the Discord channel
     * @param message The error message to send
     * @return CompletableFuture that resolves to true if successful, false otherwise
     */
    public CompletableFuture<Boolean> logError(String message) {
        String filename = getCallerFilename();
        String formattedMessage = formatMessage("🔴", "ERROR", message, filename);
        System.err.println(formattedMessage);
        return queueMessage(formattedMessage);
    }

    /**
     * Sends a success log message to the Discord channel
     * @param message The success message to send
     * @return CompletableFuture that resolves to true if successful, false otherwise
     */
    public CompletableFuture<Boolean> logSuccess(String message) {
        String filename = getCallerFilename();
        String formattedMessage = formatMessage("🟢", "SUCCESS", message, filename);
        System.out.println(formattedMessage);
        return queueMessage(formattedMessage);
    }

    /**
     * Sends a warning log message to the Discord channel
     * @param message The warning message to send
     * @return CompletableFuture that resolves to true if successful, false otherwise
     */
    public CompletableFuture<Boolean> logWarning(String message) {
        String filename = getCallerFilename();
        String formattedMessage = formatMessage("🟡", "WARNING", message, filename);
        System.out.println(formattedMessage);
        return queueMessage(formattedMessage);
    }

    /**
     * Sends an info log message to the Discord channel
     * @param message The info message to send
     * @return CompletableFuture that resolves to true if successful, false otherwise
     */
    public CompletableFuture<Boolean> logInfo(String message) {
        String filename = getCallerFilename();
        String formattedMessage = formatMessage("🔵", "INFO", message, filename);
        System.out.println(formattedMessage);
        return queueMessage(formattedMessage);
    }

    /**
     * Main method for testing
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            // Test mode
            DiscordLog discordLog = new DiscordLog();
            discordLog.logInfo("Testing Discord logging from Java");
            discordLog.logSuccess("Database update completed successfully");
            discordLog.logWarning("API rate limit approaching");
            discordLog.logError("Failed to connect to database");
            discordLog.log("Custom log message without prefix");
            
            // Wait a bit for messages to be sent
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return;
        }

        // CLI mode: java DiscordLog <logType> <message>
        String logType = args[0].toLowerCase();
        StringBuilder messageBuilder = new StringBuilder();
        for (int i = 1; i < args.length; i++) {
            if (i > 1) messageBuilder.append(" ");
            messageBuilder.append(args[i]);
        }
        String message = messageBuilder.toString();

        if (message.isEmpty()) {
            System.err.println("Error: Message is required");
            System.exit(1);
        }

        DiscordLog discordLog = new DiscordLog();

        try {
            switch (logType) {
                case "info":
                    discordLog.logInfo(message);
                    break;
                case "success":
                    discordLog.logSuccess(message);
                    break;
                case "warning":
                    discordLog.logWarning(message);
                    break;
                case "error":
                    discordLog.logError(message);
                    break;
                case "log":
                    discordLog.log(message);
                    break;
                default:
                    System.err.println("Error: Unknown log type '" + logType + "'. Use: info, success, warning, error, or log");
                    System.exit(1);
            }

            // Wait a bit for the message to be sent
            Thread.sleep(1000);

        } catch (Exception e) {
            System.err.println("Error logging to Discord: " + e.getMessage());
            System.exit(1);
        }
    }
}
