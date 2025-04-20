package com.cs6650.get_server;

import com.cs6650.consumer.db.DynamoDBManager;
import com.cs6650.consumer.model.SkierDaySummary;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servlet to handle GET requests to retrieve the total vertical for a skier
 * for a specific resort/season/day combination.
 *
 * URL format expected:
 * /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
 *
 * Responds with the total vertical distance as an integer in JSON format.
 */
@WebServlet(name = "SkierResortServlet", urlPatterns = "/skiers/*")
public class SkierResortServlet extends HttpServlet {
    private static final Gson gson = new Gson();
    private static final Logger logger = Logger.getLogger(SkierResortServlet.class.getName());
    
    private static DynamoDBManager dbManager;
    
    @Override
    public void init() throws ServletException {
        super.init();
        try {
            // Toggle between local and remote DynamoDB
            boolean useLocalDynamoDB = true; // <-- control here (true = localhost, false = AWS)
            dbManager = new DynamoDBManager(useLocalDynamoDB);
            
            logger.info("Initialized DynamoDBManager successfully.");
        } catch (Exception e) {
            logger.severe("Failed to initialize DynamoDBManager: " + e.getMessage());
            throw new ServletException("Failed to connect to DynamoDB", e);
        }
    }
    
    /**
     * Handles GET requests to retrieve total vertical for a skier on a specific resort/season/day.
     *
     * Expected URL path:
     * /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
     *
     * Returns the total vertical distance as an integer.
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        
        String urlPath = request.getPathInfo();
        logger.info("Received GET request: " + request.getRequestURI());
        
        // Validate URL path
        if (urlPath == null || urlPath.isEmpty() || urlPath.equals("/")) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            JsonObject errorResponse = new JsonObject();
            errorResponse.addProperty("message", "Missing URL parameters");
            response.getWriter().write(gson.toJson(errorResponse));
            return;
        }
        
        String[] urlParts = urlPath.substring(1).split("/");
        
        // Validate URL structure
        // Expected format: /{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
        if (urlParts.length != 7 ||
                !"seasons".equals(urlParts[1]) ||
                !"days".equals(urlParts[3]) ||
                !"skiers".equals(urlParts[5])) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            JsonObject errorResponse = new JsonObject();
            errorResponse.addProperty("message", "Invalid URL format, expected /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}");
            response.getWriter().write(gson.toJson(errorResponse));
            return;
        }
        
        try {
            // Parse path variables
            int resortID = Integer.parseInt(urlParts[0]);
            int seasonID = Integer.parseInt(urlParts[2]);
            int dayID = Integer.parseInt(urlParts[4]);
            int skierID = Integer.parseInt(urlParts[6]);
            
            // Validate dayID is within range 1-366
            if (dayID < 1 || dayID > 366) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                JsonObject errorResponse = new JsonObject();
                errorResponse.addProperty("message", "Invalid dayID: must be a number between 1 and 366");
                response.getWriter().write(gson.toJson(errorResponse));
                return;
            }
            
            // Validate other parsed IDs
            if (resortID <= 0 || skierID <= 0) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                JsonObject errorResponse = new JsonObject();
                errorResponse.addProperty("message", "Invalid parameters: resortID and skierID must be positive integers.");
                response.getWriter().write(gson.toJson(errorResponse));
                return;
            }
            
            // Query DynamoDB for skier summary, including seasonID in the query
            SkierDaySummary summary = dbManager.getSkierDaySummary(skierID, dayID, seasonID, resortID);
            
            // Check if data exists
            if (summary == null) {
                // No data found for this skier at this resort on this day
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                JsonObject errorResponse = new JsonObject();
                errorResponse.addProperty("message", "No data found for skier " + skierID + " at resort " + resortID +
                        " on day " + dayID + " in season " + seasonID);
                response.getWriter().write(gson.toJson(errorResponse));
                return;
            }
            
            // Return the total vertical as a JSON integer value
            int totalVertical = summary.getTotalVertical();
            response.setStatus(HttpServletResponse.SC_OK);
            
            // Create a JSON response with just the integer value
            JsonObject jsonResponse = new JsonObject();
            jsonResponse.addProperty("totalVertical", totalVertical);
            response.getWriter().write(gson.toJson(jsonResponse)); // Just the integer as JSON
            
        } catch (NumberFormatException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            JsonObject errorResponse = new JsonObject();
            errorResponse.addProperty("message", "Invalid parameter format. resortID and skierID must be integers.");
            response.getWriter().write(gson.toJson(errorResponse));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Unexpected error: " + e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            JsonObject errorResponse = new JsonObject();
            errorResponse.addProperty("message", "Internal server error");
            response.getWriter().write(gson.toJson(errorResponse));
        }
    }
}