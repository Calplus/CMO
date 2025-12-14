package utils;

import java.io.OutputStream;
import java.io.PrintStream;

import discordbot.logs.DiscordLog;

/**
 * Custom PrintStream that intercepts System.err and logs to Discord
 */
public class ErrorInterceptor extends PrintStream {
    private final DiscordLog logger;
    private final PrintStream original;
    private final StringBuilder lineBuffer = new StringBuilder();
    private static final ThreadLocal<Boolean> isIntercepting = ThreadLocal.withInitial(() -> false);
    private static PrintStream originalErr;

    public ErrorInterceptor(OutputStream out, DiscordLog logger, PrintStream original) {
        super(out, true);
        this.logger = logger;
        this.original = original;
    }

    @Override
    public void write(byte[] buf, int off, int len) {
        String text = new String(buf, off, len);
        original.write(buf, off, len);
        
        // Prevent infinite recursion - don't intercept if already intercepting
        if (isIntercepting.get()) {
            return;
        }
        
        try {
            isIntercepting.set(true);
            
            // Accumulate the text in buffer
            lineBuffer.append(text);
            
            // Check if we have a complete line (ends with newline)
            if (text.contains("\n")) {
                String fullText = lineBuffer.toString().trim();
                if (!fullText.isEmpty() && !fullText.startsWith("🔴")) {
                    // Don't log if it's already a Discord error message (starts with emoji)
                    logger.logError(fullText);
                }
                lineBuffer.setLength(0);
            }
        } finally {
            isIntercepting.set(false);
        }
    }

    @Override
    public void println(String x) {
        original.println(x);
        
        // Prevent infinite recursion
        if (isIntercepting.get()) {
            return;
        }
        
        try {
            isIntercepting.set(true);
            if (x != null && !x.trim().isEmpty() && !x.startsWith("🔴")) {
                logger.logError(x);
            }
        } finally {
            isIntercepting.set(false);
        }
    }

    @Override
    public void println(Object x) {
        original.println(x);
        
        // Prevent infinite recursion
        if (isIntercepting.get()) {
            return;
        }
        
        try {
            isIntercepting.set(true);
            if (x != null) {
                String msg = x.toString();
                if (!msg.startsWith("🔴")) {
                    logger.logError(msg);
                }
            }
        } finally {
            isIntercepting.set(false);
        }
    }
    
    /**
     * Sets up error interception to log all System.err to Discord
     * @param logger The DiscordLog instance to use for logging
     */
    public static void setupErrorInterception(DiscordLog logger) {
        if (originalErr == null) {
            originalErr = System.err;
            ErrorInterceptor interceptor = new ErrorInterceptor(originalErr, logger, originalErr);
            System.setErr(interceptor);
        }
    }
}
