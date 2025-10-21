import java.io.*;
import java.net.http.*;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import javax.net.ssl.*;
import java.security.cert.X509Certificate;

/**
 * Databricks Dashboard Upload & Schedule Setup
 * Java equivalent of the Python dashboardscheduler
 */
public class DatabricksDashboardUploader {
    
    // Configuration constants - Replace with your actual values
    private static final String DATABRICKS_INSTANCE = "your-databricks-instance";
    private static final String TOKEN = "your-databricks-api-token";
    private static final String LVDASH_FILE_PATH = "path/to/your/dashboard.json";
    
    // Databricks API endpoints
    private static final String BASE_URL = "https://" + DATABRICKS_INSTANCE + ".cloud.databricks.com/api/2.0";
    private static final String LAKEVIEW_API = "https://" + DATABRICKS_INSTANCE + ".cloud.databricks.com/api/2.0/lakeview";
    
    private final String instance;
    private final String token;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    
    public DatabricksDashboardUploader(String instance, String token) {
        this.instance = instance;
        this.token = token;
        this.objectMapper = new ObjectMapper();
        
        // Create HTTP client with SSL verification disabled (equivalent to verify=False)
        this.httpClient = HttpClient.newBuilder()
            .sslContext(createTrustAllSSLContext())
            .build();
    }
    
    /**
     * Create SSL context that trusts all certificates (equivalent to urllib3.disable_warnings)
     */
    private SSLContext createTrustAllSSLContext() {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[]{new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() { return null; }
                public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            }}, new java.security.SecureRandom());
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SSL context", e);
        }
    }
    
    /**
     * Read and parse the LVDash JSON file
     * @param filePath Path to the LVDash file
     * @return Map containing dashboard data
     * @throws Exception if file reading or JSON parsing fails
     */
    public Map<String, Object> readLvdashFile(String filePath) throws Exception {
        try {
            if (!Files.exists(Paths.get(filePath))) {
                throw new FileNotFoundException("File not found: " + filePath);
            }
            
            String content = Files.readString(Paths.get(filePath));
            Map<String, Object> dashboardData = objectMapper.readValue(content, Map.class);
            
            System.out.println("✓ Successfully read LVDash file: " + filePath);
            return dashboardData;
        } catch (IOException e) {
            if (e.getMessage().contains("JSON")) {
                throw new Exception("Invalid JSON format in LVDash file: " + e.getMessage());
            }
            throw new Exception("Error reading LVDash file: " + e.getMessage());
        }
    }
    
    /**
     * Create/Upload dashboard to Databricks
     * @param dashboardData Dashboard configuration data
     * @return Dashboard ID
     * @throws Exception if dashboard creation fails
     */
    public String createDashboard(Map<String, Object> dashboardData) throws Exception {
        try {
            // Extract dashboard name from data or use default
            String dashboardName = (String) dashboardData.getOrDefault("displayName", "HPX_Executive_Summary");
            
            // Prepare the payload for dashboard creation
            Map<String, Object> payload = new HashMap<>();
            payload.put("display_name", dashboardName);
            payload.put("serialized_dashboard", objectMapper.writeValueAsString(dashboardData));
            payload.put("parent_path", "/Workspace/Users/your-username/ThoughtspotLvdash/Output");
            
            String url = LAKEVIEW_API + "/dashboards";
            
            System.out.println("Uploading dashboard '" + dashboardName + "' to Databricks...");
            
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == 200 || response.statusCode() == 201) {
                Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);
                String dashboardId = (String) result.get("dashboard_id");
                System.out.println("✓ Dashboard uploaded successfully!");
                System.out.println("  Dashboard ID: " + dashboardId);
                System.out.println("  Dashboard Name: " + dashboardName);
                return dashboardId;
            } else {
                String errorMsg = parseErrorResponse(response);
                throw new Exception("Failed to upload dashboard: " + errorMsg);
            }
            
        } catch (IOException | InterruptedException e) {
            throw new Exception("Network error during dashboard upload: " + e.getMessage());
        } catch (Exception e) {
            throw new Exception("Error creating dashboard: " + e.getMessage());
        }
    }
    
    /**
     * Publish the dashboard
     * @param dashboardId Dashboard ID to publish
     * @return true if successful, false otherwise
     */
    public boolean publishDashboard(String dashboardId) {
        try {
            String url = LAKEVIEW_API + "/dashboards/" + dashboardId + "/published";
            
            System.out.println("Publishing dashboard " + dashboardId + "...");
            
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == 200 || response.statusCode() == 204) {
                System.out.println("✓ Dashboard published successfully!");
                return true;
            } else {
                String errorMsg = parseErrorResponse(response);
                System.out.println("⚠ Warning: Could not publish dashboard: " + errorMsg);
                return false;
            }
            
        } catch (Exception e) {
            System.out.println("⚠ Warning: Error publishing dashboard: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Create a schedule for dashboard refresh
     * @param dashboardId Dashboard ID to schedule
     * @param cronSchedule Cron expression for scheduling (default: "0 8 * * *")
     * @return Schedule ID
     * @throws Exception if schedule creation fails
     */
    public String createSchedule(String dashboardId, String cronSchedule) throws Exception {
        if (cronSchedule == null) {
            cronSchedule = "0 8 * * *";
        }
        
        try {
            Map<String, Object> scheduleConfig = new HashMap<>();
            scheduleConfig.put("quartz_cron_expression", cronSchedule);
            scheduleConfig.put("timezone_id", "UTC");
            
            Map<String, Object> payload = new HashMap<>();
            payload.put("dashboard_id", dashboardId);
            payload.put("display_name", "Daily Refresh - " + dashboardId);
            payload.put("pause_status", "UNPAUSED");
            payload.put("schedule", scheduleConfig);
            
            String url = LAKEVIEW_API + "/dashboards/" + dashboardId + "/schedules";
            
            System.out.println("Creating schedule for dashboard (cron: " + cronSchedule + ")...");
            
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() == 200 || response.statusCode() == 201) {
                Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);
                String scheduleId = (String) result.get("schedule_id");
                System.out.println("✓ Schedule created successfully!");
                System.out.println("  Schedule ID: " + scheduleId);
                System.out.println("  Cron Expression: " + cronSchedule);
                return scheduleId;
            } else {
                String errorMsg = parseErrorResponse(response);
                throw new Exception("Failed to create schedule: " + errorMsg);
            }
            
        } catch (IOException | InterruptedException e) {
            throw new Exception("Network error during schedule creation: " + e.getMessage());
        } catch (Exception e) {
            throw new Exception("Error creating schedule: " + e.getMessage());
        }
    }
    
    /**
     * Parse error response from Databricks API
     * @param response HTTP response object
     * @return Formatted error message
     */
    private String parseErrorResponse(HttpResponse<String> response) {
        try {
            Map<String, Object> errorData = objectMapper.readValue(response.body(), Map.class);
            String errorCode = (String) errorData.getOrDefault("error_code", "UNKNOWN_ERROR");
            String errorMessage = (String) errorData.getOrDefault("message", response.body());
            return "[" + response.statusCode() + "] " + errorCode + ": " + errorMessage;
        } catch (Exception e) {
            return "[" + response.statusCode() + "] " + response.body();
        }
    }
    
    /**
     * Generate dashboard URL
     * @param dashboardId Dashboard ID
     * @return Dashboard URL
     */
    public String getDashboardUrl(String dashboardId) {
        return "https://" + instance + ".cloud.databricks.com/sql/dashboards/" + dashboardId;
    }
    
    /**
     * Main execution function
     */
    public static void main(String[] args) {
        System.out.println("=".repeat(70));
        System.out.println("Databricks Dashboard Upload & Schedule Setup");
        System.out.println("=".repeat(70));
        System.out.println();
        
        try {
            // Initialize uploader
            DatabricksDashboardUploader uploader = new DatabricksDashboardUploader(DATABRICKS_INSTANCE, TOKEN);
            
            // Step 1: Read LVDash file
            System.out.println("Step 1: Reading LVDash file...");
            Map<String, Object> dashboardData = uploader.readLvdashFile(LVDASH_FILE_PATH);
            System.out.println();
            
            // Step 2: Upload dashboard
            System.out.println("Step 2: Uploading dashboard to Databricks...");
            String dashboardId = uploader.createDashboard(dashboardData);
            System.out.println();
            
            // Step 3: Publish dashboard
            System.out.println("Step 3: Publishing dashboard...");
            uploader.publishDashboard(dashboardId);
            System.out.println();
            
            // Step 4: Create schedule (runs daily at 8 AM UTC)
            System.out.println("Step 4: Creating dashboard schedule...");
            String cronSchedule = "0 8 * * *";  // Daily at 8 AM UTC
            // Change to "0 */4 * * *" for every 4 hours
            // Change to "0 0 * * 1" for every Monday at midnight
            String scheduleId = uploader.createSchedule(dashboardId, cronSchedule);
            System.out.println();
            
            // Success summary
            System.out.println("=".repeat(70));
            System.out.println("✓ ALL STEPS COMPLETED SUCCESSFULLY!");
            System.out.println("=".repeat(70));
            System.out.println("Dashboard ID: " + dashboardId);
            System.out.println("Schedule ID: " + scheduleId);
            System.out.println("Dashboard URL: " + uploader.getDashboardUrl(dashboardId));
            System.out.println();
            System.out.println("Next steps:");
            System.out.println("1. Visit the dashboard URL above to view your dashboard");
            System.out.println("2. The dashboard will refresh automatically based on the schedule");
            System.out.println("3. You can modify the schedule in Databricks UI if needed");
            System.out.println("=".repeat(70));
            
        } catch (FileNotFoundException e) {
            System.out.println("\n❌ FILE ERROR: " + e.getMessage());
            System.out.println("\nPlease check:");
            System.out.println("1. The file path is correct");
            System.out.println("2. The file exists at the specified location");
            System.out.println("3. You have read permissions for the file");
            
        } catch (Exception e) {
            if (e.getMessage().contains("JSON")) {
                System.out.println("\n❌ JSON PARSING ERROR: " + e.getMessage());
                System.out.println("\nPlease check:");
                System.out.println("1. The LVDash file contains valid JSON");
                System.out.println("2. The file is not corrupted");
                System.out.println("3. The file is a proper Databricks LVDash export");
            } else if (e.getMessage().contains("Connection") || e.getMessage().contains("Network")) {
                System.out.println("\n❌ CONNECTION ERROR: " + e.getMessage());
                System.out.println("\nPlease check:");
                System.out.println("1. Your internet connection is active");
                System.out.println("2. The Databricks instance URL is correct");
                System.out.println("3. You can access Databricks from your network");
            } else {
                System.out.println("\n❌ ERROR: " + e.getMessage());
                System.out.println("\nPlease check:");
                System.out.println("1. Your Databricks token is valid and not expired");
                System.out.println("2. You have permissions to create dashboards and schedules");
                System.out.println("3. The Databricks instance name is correct");
                System.out.println("4. The LVDash file format is compatible with your Databricks version");
                System.out.println("\nFull error details:");
                e.printStackTrace();
            }
        }
    }
}