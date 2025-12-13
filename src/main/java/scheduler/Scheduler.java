package scheduler;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;

import databaseupdater.A01_ClanInfo;

/**
 * A scheduler that manages running Java tasks at specified intervals.
 * Supports resetting timers if tasks are run early for any reason.
 */
public class Scheduler {
    
    private final ScheduledExecutorService executor;
    private final Map<String, ScheduledTask> scheduledTasks;
    
    public Scheduler() {
        this.executor = Executors.newScheduledThreadPool(4);
        this.scheduledTasks = new HashMap<>();
    }
    
    /**
     * Represents a scheduled task with its future and interval
     */
    private static class ScheduledTask {
        private ScheduledFuture<?> future;
        private final long intervalHours;
        private final Runnable task;
        
        public ScheduledTask(ScheduledFuture<?> future, long intervalHours, Runnable task) {
            this.future = future;
            this.intervalHours = intervalHours;
            this.task = task;
        }
        
        public void cancel() {
            if (future != null) {
                future.cancel(false);
            }
        }
        
        public long getIntervalHours() {
            return intervalHours;
        }
        
        public Runnable getTask() {
            return task;
        }
        
        public void setFuture(ScheduledFuture<?> future) {
            this.future = future;
        }
    }
    
    /**
     * Schedules A01_ClanInfo to run every 24 hours
     */
    public void scheduleA01_ClanInfo(String dbName) {
        Runnable task = () -> {
            try {
                log("Starting scheduled A01_ClanInfo update for database: " + dbName);
                A01_ClanInfo updater = new A01_ClanInfo(dbName);
                updater.updateDatabase();
                log("Completed scheduled A01_ClanInfo update for database: " + dbName);
            } catch (Exception e) {
                log("Error in scheduled A01_ClanInfo update: " + e.getMessage());
                e.printStackTrace();
            }
        };
        
        scheduleTask("A01_ClanInfo_" + dbName, task, 24);
    }
    
    /**
     * Schedules a task to run at specified interval
     * 
     * @param taskName Unique identifier for the task
     * @param task The task to run
     * @param intervalHours How often to run the task (in hours)
     */
    public void scheduleTask(String taskName, Runnable task, long intervalHours) {
        // Cancel existing task if it exists
        if (scheduledTasks.containsKey(taskName)) {
            scheduledTasks.get(taskName).cancel();
        }
        
        // Schedule new task
        ScheduledFuture<?> future = executor.scheduleAtFixedRate(
            task, 
            0, // Initial delay (0 = run immediately)
            intervalHours, 
            TimeUnit.HOURS
        );
        
        scheduledTasks.put(taskName, new ScheduledTask(future, intervalHours, task));
        log("Scheduled task '" + taskName + "' to run every " + intervalHours + " hours");
    }
    
    /**
     * Runs a task immediately and resets its timer
     * 
     * @param taskName The name of the task to run early
     */
    public void runTaskEarly(String taskName) {
        ScheduledTask scheduledTask = scheduledTasks.get(taskName);
        if (scheduledTask == null) {
            log("Warning: Task '" + taskName + "' not found");
            return;
        }
        
        log("Running task '" + taskName + "' early and resetting timer");
        
        // Cancel the existing scheduled task
        scheduledTask.cancel();
        
        // Run the task immediately
        try {
            scheduledTask.getTask().run();
        } catch (Exception e) {
            log("Error running task '" + taskName + "' early: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Reschedule with full interval
        ScheduledFuture<?> newFuture = executor.scheduleAtFixedRate(
            scheduledTask.getTask(),
            scheduledTask.getIntervalHours(), // Wait full interval before next run
            scheduledTask.getIntervalHours(),
            TimeUnit.HOURS
        );
        
        scheduledTask.setFuture(newFuture);
        log("Reset timer for task '" + taskName + "' - next run in " + scheduledTask.getIntervalHours() + " hours");
    }
    
    /**
     * Stops a scheduled task
     */
    public void stopTask(String taskName) {
        ScheduledTask task = scheduledTasks.get(taskName);
        if (task != null) {
            task.cancel();
            scheduledTasks.remove(taskName);
            log("Stopped task: " + taskName);
        } else {
            log("Warning: Task '" + taskName + "' not found");
        }
    }
    
    /**
     * Lists all currently scheduled tasks
     */
    public void listTasks() {
        log("Currently scheduled tasks:");
        if (scheduledTasks.isEmpty()) {
            log("  No tasks scheduled");
        } else {
            for (Map.Entry<String, ScheduledTask> entry : scheduledTasks.entrySet()) {
                String taskName = entry.getKey();
                long interval = entry.getValue().getIntervalHours();
                log("  - " + taskName + " (every " + interval + " hours)");
            }
        }
    }
    
    /**
     * Shuts down the scheduler
     */
    public void shutdown() {
        log("Shutting down scheduler...");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    log("Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log("Scheduler shutdown complete");
    }
    
    /**
     * Logs a message with timestamp
     */
    private void log(String message) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.println("[" + timestamp + "] " + message);
    }
    
    /**
     * Main method for testing or standalone execution
     */
    public static void main(String[] args) {
        Scheduler scheduler = new Scheduler();
        
        // Schedule A01_ClanInfo to run every 24 hours for the default database
        scheduler.scheduleA01_ClanInfo("20CG8UURL.db");
        
        // Add shutdown hook to clean up when the program exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
        }));
        
        // Keep the program running (in a real application, this would be handled differently)
        try {
            // Example: Run the task early after 10 seconds for testing
            Thread.sleep(10000);
            scheduler.runTaskEarly("A01_ClanInfo_20CG8UURL.db");
            
            // List current tasks
            scheduler.listTasks();
            
            // Keep running indefinitely
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            scheduler.shutdown();
            Thread.currentThread().interrupt();
        }
    }
}