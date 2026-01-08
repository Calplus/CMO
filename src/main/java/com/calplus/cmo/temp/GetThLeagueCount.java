package com.calplus.cmo.temp;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import com.calplus.cmo.calculations.CalcPlayerQuality;
import com.calplus.cmo.discordbot.logs.DiscordLog;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class GetThLeagueCount {
    
    private static final String URL = "https://clashspot.net/en/stats/players/home-village";
    private static final String OUTPUT_DIR = "config/cocdata";
    private static final String OUTPUT_FILE = "thLeagueCount.json";
    private static final int CHART_LOAD_WAIT_MS = 3000;
    
    private final WebDriver driver;
    private final WebDriverWait wait;
    private final JavascriptExecutor js;
    private final DiscordLog discordLog;
    
    // Suppress Selenium warnings
    static {
        Logger.getLogger("org.openqa.selenium").setLevel(Level.SEVERE);
    }
    
    public GetThLeagueCount() {
        this(true); // Default: headless
    }
    
    public GetThLeagueCount(boolean headless) {
        this.discordLog = new DiscordLog();
        
        ChromeOptions options = new ChromeOptions();
        options.addArguments("--log-level=3"); // Suppress console logs
        
        if (headless) {
            options.addArguments("--headless=new");
            options.addArguments("--disable-gpu");
        }
        
        options.addArguments(
            "--disable-blink-features=AutomationControlled",
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--window-size=1920,1080"
        );
        
        this.driver = new ChromeDriver(options);
        this.wait = new WebDriverWait(driver, Duration.ofSeconds(30));
        this.js = (JavascriptExecutor) driver;
    }
    
    public Map<String, Object> scrapeData() {
        Map<String, Object> result = new LinkedHashMap<>();
        
        try {
            driver.get(URL);
            
            // Wait for charts to be rendered
            wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.tagName("canvas")));
            Thread.sleep(CHART_LOAD_WAIT_MS);

            discordLog.logSuccess("Successfully fetched data from ClashSpot");

            List<WebElement> canvases = driver.findElements(By.tagName("canvas"));
            
            // Find and extract the target chart
            for (int i = 0; i < canvases.size(); i++) {
                String chartTitle = getChartTitle(i);
                
                if (chartTitle.contains("Number of THs by league")) {
                    String chartDataJson = extractChartData(i);
                    
                    if (chartDataJson != null) {
                        JsonObject chartJson = JsonParser.parseString(chartDataJson).getAsJsonObject();
                        result.put("timestamp", java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC)
                            .format(java.time.format.DateTimeFormatter.ISO_INSTANT));
                        
                        // Process data into matrix format
                        JsonArray labels = chartJson.getAsJsonArray("labels");
                        JsonArray datasets = chartJson.getAsJsonArray("datasets");
                        
                        result.put("labels", labels);
                        result.put("matrix", createMatrix(datasets));

                        int[][] cumulativeMatrix = CalcPlayerQuality.createCumulativeMatrix(datasets);
                        
                        result.put("cumulativeMatrix", cumulativeMatrix);
                        result.put("baseScoreMatrix", CalcPlayerQuality.createBaseScoreMatrix(cumulativeMatrix));
                        
                        discordLog.logSuccess("Successfully updated JSON file: " + 
                            datasets.size() + " TH levels, " + labels.size() + " leagues");
                        break;
                    }
                }
            }
            
            if (result.isEmpty()) {
                String errorMsg = "Could not find or extract 'Number of THs by league' chart";
                discordLog.logError(errorMsg);
                throw new RuntimeException(errorMsg);
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            discordLog.logError("Scraping interrupted: " + e.getMessage());
            throw new RuntimeException("Scraping interrupted", e);
        } catch (Exception e) {
            discordLog.logError("Error scraping data: " + e.getMessage());
            throw new RuntimeException("Scraping failed", e);
        }
        
        return result;
    }
    
    /**
     * Creates a 2D matrix from datasets
     */
    private int[][] createMatrix(JsonArray datasets) {
        int[][] matrix = new int[datasets.size()][];
        
        for (int i = 0; i < datasets.size(); i++) {
            JsonObject dataset = datasets.get(i).getAsJsonObject();
            JsonArray dataArray = dataset.getAsJsonArray("data");
            matrix[i] = new int[dataArray.size()];
            
            for (int j = 0; j < dataArray.size(); j++) {
                matrix[i][j] = dataArray.get(j).getAsInt();
            }
        }
        
        return matrix;
    }

    
    
    /**
     * Attempts to extract chart data using multiple methods
     */
    private String extractChartData(int index) {
        String result = extractChartDataMethod1(index);
        if (result == null) result = extractChartDataMethod2(index);
        if (result == null) result = extractChartDataMethod3(index);
        return result;
    }
    
    private String extractChartDataMethod1(int index) {
        try {
            Object result = js.executeScript(
                "var canvas = document.querySelectorAll('canvas')[" + index + "];" +
                "if (canvas && canvas.chart) {" +
                "  var chart = canvas.chart;" +
                "  return JSON.stringify({" +
                "    labels: chart.data.labels," +
                "    datasets: chart.data.datasets.map(function(ds) {" +
                "      return { label: ds.label, data: ds.data };" +
                "    })" +
                "  });" +
                "}" +
                "return null;"
            );
            return result != null ? result.toString() : null;
        } catch (Exception e) {
            return null;
        }
    }
    
    private String extractChartDataMethod2(int index) {
        try {
            Object result = js.executeScript(
                "var canvas = document.querySelectorAll('canvas')[" + index + "];" +
                "if (window.Chart && window.Chart.instances) {" +
                "  var instances = window.Chart.instances;" +
                "  for (var key in instances) {" +
                "    if (instances[key].canvas === canvas) {" +
                "      var chart = instances[key];" +
                "      return JSON.stringify({" +
                "        labels: chart.data.labels," +
                "        datasets: chart.data.datasets.map(function(ds) {" +
                "          return { label: ds.label, data: ds.data };" +
                "        })" +
                "      });" +
                "    }" +
                "  }" +
                "}" +
                "return null;"
            );
            return result != null ? result.toString() : null;
        } catch (Exception e) {
            return null;
        }
    }
    
    private String extractChartDataMethod3(int index) {
        try {
            Object result = js.executeScript(
                "var canvas = document.querySelectorAll('canvas')[" + index + "];" +
                "if (window.Chart) {" +
                "  var chart = null;" +
                "  if (typeof Chart.getChart === 'function') {" +
                "    chart = Chart.getChart(canvas);" +
                "  } else if (canvas.id && Chart.getChart) {" +
                "    chart = Chart.getChart(canvas.id);" +
                "  }" +
                "  if (chart && chart.data) {" +
                "    return JSON.stringify({" +
                "      labels: chart.data.labels," +
                "      datasets: chart.data.datasets.map(function(ds) {" +
                "        return { label: ds.label, data: ds.data };" +
                "      })" +
                "    });" +
                "  }" +
                "}" +
                "return null;"
            );
            return result != null ? result.toString() : null;
        } catch (Exception e) {
            return null;
        }
    }
    
    private String getChartTitle(int chartIndex) {
        try {
            Object result = js.executeScript(
                "var canvas = document.querySelectorAll('canvas')[" + chartIndex + "];" +
                "var parent = canvas.closest('div');" +
                "var h2 = parent ? parent.previousElementSibling : null;" +
                "while (h2 && h2.tagName !== 'H2') {" +
                "  h2 = h2.previousElementSibling;" +
                "}" +
                "return h2 ? h2.textContent.trim() : 'Chart " + chartIndex + "';"
            );
            return result != null ? result.toString() : "Chart " + chartIndex;
        } catch (Exception e) {
            return "Chart " + chartIndex;
        }
    }
    
    public void saveToJson(Map<String, Object> data, String filename) {
        try {
            // Ensure output directory exists
            Path outputPath = Paths.get(OUTPUT_DIR);
            Files.createDirectories(outputPath);
            
            Path filePath = outputPath.resolve(filename);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            
            try (FileWriter writer = new FileWriter(filePath.toFile())) {
                gson.toJson(data, writer);
                discordLog.logSuccess("Data saved to " + filePath);
            }
        } catch (IOException e) {
            discordLog.logError("Error writing JSON file: " + e.getMessage());
            throw new RuntimeException("Failed to save JSON", e);
        }
    }
    
    public void close() {
        if (driver != null) {
            driver.quit();
        }
    }
    
    public static void main(String[] args) {
        boolean headless = true;
        for (String arg : args) {
            if (arg.equals("--no-headless") || arg.equals("-v")) {
                headless = false;
            }
        }
        
        GetThLeagueCount scraper = new GetThLeagueCount(headless);
        
        try {
            scraper.discordLog.logInfo("Obtaining TH League data from ClashSpot");
            
            Map<String, Object> data = scraper.scrapeData();
            scraper.saveToJson(data, OUTPUT_FILE);
            
            scraper.discordLog.logSuccess("TH League data saved to: " + Paths.get(OUTPUT_DIR, OUTPUT_FILE).toAbsolutePath());
            
        } catch (Exception e) {
            scraper.discordLog.logError("Fatal error during scraping: " + e.getMessage());
            throw e;
        } finally {
            scraper.close();
        }
    }
}