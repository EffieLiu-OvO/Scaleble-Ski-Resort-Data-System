package com.cs6650.get_server;

import com.cs6650.consumer.db.DynamoDBManager;
import com.cs6650.consumer.model.ResortDaySummary;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Servlet to handle GET requests to retrieve the number of unique skiers
 * for a given resort, season, and day.
 *
 * URL format expected:
 * /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
 *
 * Responds with JSON containing resort, season, day, and number of unique skiers.
 */
@WebServlet(name = "ResortSkiersServlet", urlPatterns = "/resorts/*")
public class ResortSkiersServlet extends HttpServlet {
    private static final Gson gson = new Gson();
    private static final Logger logger = Logger.getLogger(ResortSkiersServlet.class.getName());

    private static DynamoDBManager dbManager;

    @Override
    public void init() throws ServletException {
        super.init();
        try {
            // // Toggle between local and remote DynamoDB
            boolean useLocalDynamoDB = true; // <-- control here (true = localhost, false = AWS)
            dbManager = new DynamoDBManager(useLocalDynamoDB);

            logger.info("Initialized DynamoDBManager successfully.");
        } catch (Exception e) {
            logger.severe("Failed to initialize DynamoDBManager: " + e.getMessage());
            throw new ServletException("Failed to connect to DynamoDB", e);
        }
    }

    /**
     * Handles GET requests to retrieve skier counts for a specific resort/season/day.
     *
     * Expected URL path:
     * /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
     *
     * Returns a JSON object with the resortID, seasonID, dayID, and number of skiers.
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
            response.getWriter().write("{\"error\": \"Missing URL parameters\"}");
            return;
        }

        String[] urlParts = urlPath.substring(1).split("/");

        // Validate URL structure
        // Expected format: /{resortID}/seasons/{seasonID}/day/{dayID}/skiers
        if (urlParts.length != 6 ||
                !"seasons".equals(urlParts[1]) ||
                !"day".equals(urlParts[3]) ||
                !"skiers".equals(urlParts[5])) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\": \"Invalid URL format, expected /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers\"}");
            return;
        }

        try {
            // Parse path variables
            int resortID = Integer.parseInt(urlParts[0]);
            int seasonID = Integer.parseInt(urlParts[2]);
            int dayID = Integer.parseInt(urlParts[4]);

            // Validate parsed IDs
            if (resortID <= 0 || seasonID <= 0 || dayID <= 0) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write("{\"error\": \"resortID, seasonID, and dayID must be positive integers\"}");
                return;
            }

            // Query DynamoDB for resort summary
            String compositeId = resortID + "#" + seasonID + "#" + dayID;
            ResortDaySummary summary = dbManager.getResortDaySummary(resortID, seasonID, dayID);

            if (summary == null) {
                // No data found
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\": \"No data found for specified resort, season, and day\"}");
                return;
            }

            // Build response
            JsonObject jsonResponse = new JsonObject();
            jsonResponse.addProperty("resortID", summary.getResortID());
            jsonResponse.addProperty("seasonID", summary.getSeasonID());
            jsonResponse.addProperty("dayID", summary.getDayID());
            jsonResponse.addProperty("numSkiers", summary.getUniqueSkierCount()); // number of skiers

            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(gson.toJson(jsonResponse));

        } catch (NumberFormatException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\": \"resortID, seasonID, and dayID must be integers\"}");
        } catch (Exception e) {
            logger.severe("Unexpected error: " + e.getMessage());
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\": \"Internal server error\"}");
        }
    }
}
