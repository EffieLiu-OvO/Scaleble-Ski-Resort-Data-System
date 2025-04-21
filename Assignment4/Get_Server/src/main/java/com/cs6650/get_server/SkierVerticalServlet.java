package com.cs6650.get_server;

import com.cs6650.consumer.db.DynamoDBManager;
import com.cs6650.consumer.model.SkierVerticalSummary;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(name = "SkierVerticalServlet", urlPatterns = {"/skiers/*"})
public class SkierVerticalServlet extends HttpServlet {
    private final Gson gson = new Gson();
    private static final Logger logger = Logger.getLogger(SkierVerticalServlet.class.getName());
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

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        String pathInfo = request.getPathInfo();
        logger.info("Received GET request: " + request.getRequestURI());

        if (pathInfo == null || pathInfo.isEmpty() || pathInfo.equals("/")) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("Missing parameters");
            return;
        }

        String[] parts = pathInfo.split("/");


        // URL pattern: /skiers/{skierID}/vertical
        if (parts.length < 3) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("Invalid path. Not enough parts in /skiers/{skierID}/vertical");
            return;
        }

        if (!"vertical".equals(parts[2])) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("Invalid path. Expecting /skiers/{skierID}/vertical");
            return;
        }

        try {
            int skierID = Integer.parseInt(parts[1]);
            Integer seasonID = null;
            String seasonParam = request.getParameter("seasonID");
            if (seasonParam != null && !seasonParam.isEmpty()) {
                seasonID = Integer.parseInt(seasonParam);
            }

            SkierVerticalSummary summary = dbManager.getSkierVerticalSummary(skierID, seasonID);
            if (summary == null) {
                // No data found
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\": \"No vertical data found for this skier");
                return;
            }

            JsonObject jsonResponse = new JsonObject();
            jsonResponse.addProperty("verticals", summary.getTotalVertical());
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(gson.toJson(jsonResponse));

        } catch (NumberFormatException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("Invalid skierID or seasonID");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error handling GET skier vertical", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("Internal server error");
        }
    }

}
