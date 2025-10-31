/**
 * Java equivalent of DBT_Job_Runner.py
 * Converts Python dbt Cloud job runner to Java implementation
 * Maintains same functionality for triggering and monitoring dbt Cloud jobs
 */

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class DBTCloudJobRunner {
    private String accountId;
    private String jobId;
    private String apiToken;
    private String baseUrl;
    private boolean verifySsl;
    private HttpClient httpClient;
    private ObjectMapper objectMapper;
    
    public DBTCloudJobRunner(String accountId, String jobId, String apiToken, boolean verifySsl) {
        this.accountId = accountId;
        this.jobId = jobId;
        this.apiToken = apiToken;
        this.baseUrl = "https://vs509.us1.dbt.com/api/v2";
        this.verifySsl = verifySsl;
        this.objectMapper = new ObjectMapper();
        
        // Create HTTP client with SSL verification settings
        HttpClient.Builder clientBuilder = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30));
        
        this.httpClient = clientBuilder.build();
    }
    
    public DBTCloudJobRunner(String accountId, String jobId, String apiToken) {
        this(accountId, jobId, apiToken, false);
    }
    
    /**
     * Trigger the dbt Cloud job
     * @param cause Reason for triggering the job
     * @return Run ID if successful, null if failed
     */
    public String triggerJob(String cause) {
        String url = String.format("%s/accounts/%s/jobs/%s/run/", baseUrl, accountId, jobId);
        
        try {
            // Create payload
            Map<String, String> payload = new HashMap<>();
            payload.put("cause", cause);
            String jsonPayload = objectMapper.writeValueAsString(payload);
            
            // Build request
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Token " + apiToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                .build();
            
            // Send request
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                JsonNode responseJson = objectMapper.readTree(response.body());
                String runId = responseJson.get("data").get("id").asText();
                System.out.println("✓ Job triggered successfully! Run ID: " + runId);
                return runId;
            } else {
                System.out.println("✗ Failed to trigger job. Status code: " + response.statusCode());
                System.out.println("Response: " + response.body());
                return null;
            }
            
        } catch (IOException | InterruptedException | JsonProcessingException e) {
            System.out.println("✗ Failed to trigger job: " + e.getMessage());
            return null;
        }
    }
    
    public String triggerJob() {
        return triggerJob("Triggered via API");
    }
    
    /**
     * Get the status of a specific run
     * @param runId The run ID to check
     * @return JsonNode containing run data, null if failed
     */
    public JsonNode getRunStatus(String runId) {
        String url = String.format("%s/accounts/%s/runs/%s/", baseUrl, accountId, runId);
        
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Token " + apiToken)
                .header("Content-Type", "application/json")
                .GET()
                .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                JsonNode responseJson = objectMapper.readTree(response.body());
                return responseJson.get("data");
            } else {
                System.out.println("✗ Failed to get run status. Status code: " + response.statusCode());
                return null;
            }
            
        } catch (IOException | InterruptedException e) {
            System.out.println("✗ Failed to get run status: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Wait for the job to complete and return detailed status
     * @param runId The run ID to monitor
     * @param pollInterval Polling interval in seconds
     * @param timeout Timeout in seconds
     * @return Map containing success status and message
     */
    public Map<String, Object> waitForCompletion(String runId, int pollInterval, int timeout) {
        System.out.println("\nMonitoring run " + runId + "...");
        long startTime = System.currentTimeMillis();
        long timeoutMillis = timeout * 1000L;
        
        while (true) {
            if (System.currentTimeMillis() - startTime > timeoutMillis) {
                Map<String, Object> result = new HashMap<>();
                result.put("success", false);
                result.put("message", "Job execution timed out after " + timeout + " seconds");
                return result;
            }
            
            JsonNode runData = getRunStatus(runId);
            
            if (runData == null) {
                Map<String, Object> result = new HashMap<>();
                result.put("success", false);
                result.put("message", "Failed to retrieve run status");
                return result;
            }
            
            String status = runData.has("status_humanized") ? 
                runData.get("status_humanized").asText() : "Unknown";
            System.out.print("Status: " + status + "\r");
            
            // Check if job is finished
            if (runData.has("is_complete") && runData.get("is_complete").asBoolean()) {
                return processCompletion(runData);
            }
            
            try {
                Thread.sleep(pollInterval * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Map<String, Object> result = new HashMap<>();
                result.put("success", false);
                result.put("message", "Monitoring interrupted");
                return result;
            }
        }
    }
    
    public Map<String, Object> waitForCompletion(String runId) {
        return waitForCompletion(runId, 10, 3600);
    }
    
    /**
     * Process the completed run and return detailed results
     * @param runData JsonNode containing run data
     * @return Map containing success status and detailed message
     */
    private Map<String, Object> processCompletion(JsonNode runData) {
        String status = runData.has("status_humanized") ? 
            runData.get("status_humanized").asText() : "Unknown";
        String runId = runData.has("id") ? runData.get("id").asText() : "Unknown";
        
        Map<String, Object> result = new HashMap<>();
        
        // Success case
        if (runData.has("is_success") && runData.get("is_success").asBoolean()) {
            String duration = runData.has("duration_humanized") ? 
                runData.get("duration_humanized").asText() : "Unknown";
            
            String message = String.format(
                "✓ Job completed successfully!\n" +
                "  Run ID: %s\n" +
                "  Status: %s\n" +
                "  Duration: %s",
                runId, status, duration
            );
            
            result.put("success", true);
            result.put("message", message);
            return result;
        }
        
        // Error case
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append(String.format("✗ Job failed with status: %s\n", status));
        errorMessage.append(String.format("  Run ID: %s\n", runId));
        
        // Add run results details if available
        if (runData.has("run_steps") && runData.get("run_steps").isArray()) {
            JsonNode runSteps = runData.get("run_steps");
            if (runSteps.size() > 0) {
                errorMessage.append("\n  Detailed Error Information:\n");
                
                for (JsonNode step : runSteps) {
                    String stepName = step.has("name") ? step.get("name").asText() : "Unknown Step";
                    String stepStatus = step.has("status_humanized") ? 
                        step.get("status_humanized").asText() : "Unknown";
                    
                    // Check if step failed (status 20 or 30 for error or cancelled)
                    if (step.has("status")) {
                        int statusCode = step.get("status").asInt();
                        if (statusCode == 20 || statusCode == 30) {
                            errorMessage.append(String.format("    - %s: %s\n", stepName, stepStatus));
                            
                            // Add error message if available
                            if (step.has("error_message") && !step.get("error_message").isNull()) {
                                errorMessage.append(String.format("      Error: %s\n", 
                                    step.get("error_message").asText()));
                            }
                            
                            // Add logs URL if available
                            if (step.has("logs_url") && !step.get("logs_url").isNull()) {
                                errorMessage.append(String.format("      Logs: %s\n", 
                                    step.get("logs_url").asText()));
                            }
                        }
                    }
                }
            }
        }
        
        // Add general error if available
        if (runData.has("status_message") && !runData.get("status_message").isNull()) {
            errorMessage.append(String.format("\n  Status Message: %s", 
                runData.get("status_message").asText()));
        }
        
        result.put("success", false);
        result.put("message", errorMessage.toString());
        result.put("run_data", runData);
        return result;
    }
    
    /**
     * Main method to trigger and optionally wait for job completion
     * @param cause Reason for triggering the job
     * @param wait Whether to wait for completion
     * @return Map containing success status and message
     */
    public Map<String, Object> runJob(String cause, boolean wait) {
        System.out.println("Triggering dbt Cloud Job " + jobId + "...");
        
        String runId = triggerJob(cause);
        
        if (runId == null) {
            Map<String, Object> result = new HashMap<>();
            result.put("success", false);
            result.put("message", "Failed to trigger job");
            return result;
        }
        
        if (wait) {
            return waitForCompletion(runId);
        } else {
            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("message", "Job triggered successfully. Run ID: " + runId);
            result.put("run_id", runId);
            return result;
        }
    }
    
    public Map<String, Object> runJob(String cause) {
        return runJob(cause, true);
    }
    
    public Map<String, Object> runJob() {
        return runJob("Triggered via API", true);
    }
    
    /**
     * Main method - equivalent to Python's main() function
     */
    public static void main(String[] args) {
        // Configuration
        String ACCOUNT_ID = "265860";
        String JOB_ID = "957217";
        String API_TOKEN = "dbtu_WZIpxNg4jNqPp9T9EIePdk4j18DKxwqldaYQOYhIcQDQhOCDb8";
        
        // Initialize runner
        DBTCloudJobRunner runner = new DBTCloudJobRunner(ACCOUNT_ID, JOB_ID, API_TOKEN);
        
        // Run the job and wait for completion
        Map<String, Object> result = runner.runJob("Triggered from Java application", true);
        
        // Print the final result
        System.out.println("\n" + "=".repeat(60));
        System.out.println(result.get("message"));
        System.out.println("=".repeat(60));
        
        // Exit with appropriate code
        boolean success = (Boolean) result.get("success");
        System.exit(success ? 0 : 1);
    }
}