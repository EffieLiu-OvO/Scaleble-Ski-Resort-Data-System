package com.cs6650.server;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

@WebServlet(name = "SkiersServlet", urlPatterns = "/skiers/*")
public class SkiersServlet extends HttpServlet {
    private static final Gson gson = new Gson();
    private static final Logger logger = Logger.getLogger(SkiersServlet.class.getName());

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        logger.info("Received POST request: " + request.getRequestURI());

        StringBuilder jsonString = new StringBuilder();
        try (BufferedReader reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                jsonString.append(line);
            }
        }

        logger.info("Request Body: " + jsonString.toString());

        try {
            JsonObject jsonRequest = gson.fromJson(jsonString.toString(), JsonObject.class);

            if (!validateRequest(jsonRequest)) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write("{\"error\": \"Invalid request parameters\"}");
                return;
            }

            response.setStatus(HttpServletResponse.SC_CREATED);
            JsonObject jsonResponse = new JsonObject();
            jsonResponse.addProperty("message", "Lift ride recorded successfully.");
            jsonResponse.addProperty("message", "Lift ride recorded successfully.");
            jsonResponse.addProperty("skierID", jsonRequest.get("skierID").getAsInt());
            jsonResponse.addProperty("resortID", jsonRequest.get("resortID").getAsInt());
            jsonResponse.addProperty("liftID", jsonRequest.get("liftID").getAsInt());
            jsonResponse.addProperty("seasonID", jsonRequest.get("seasonID").getAsString());
            jsonResponse.addProperty("dayID", jsonRequest.get("dayID").getAsInt());
            jsonResponse.addProperty("time", jsonRequest.get("time").getAsInt());
            jsonResponse.addProperty("verticalGain", jsonRequest.get("liftID").getAsInt() * 10); // Dummy calculation

            response.setStatus(HttpServletResponse.SC_CREATED);
            response.getWriter().write(gson.toJson(jsonResponse));

        } catch (Exception ex) {
            logger.log(Level.SEVERE, "JSON parsing error", ex);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            JsonObject errorResponse = new JsonObject();
            errorResponse.addProperty("error", "Invalid JSON format or missing parameters.");
            response.getWriter().write(gson.toJson(errorResponse));
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        String urlPath = request.getPathInfo();

        logger.info("Received GET request: " + request.getRequestURI());

        if (urlPath == null || urlPath.isEmpty() || urlPath.equals("/")) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\": \"Invalid URL format, please provide skierID\"}");
            return;
        }

        String[] urlParts = urlPath.substring(1).split("/");

        if (urlParts.length != 2 || !urlParts[1].equals("vertical")) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\": \"Invalid URL format, expected /skiers/{skierID}/vertical\"}");
            return;
        }

        try {
            int skierID = Integer.parseInt(urlParts[0]);

            int totalVertical = (int) (Math.random() * 100000);

            JsonObject jsonResponse = new JsonObject();
            jsonResponse.addProperty("skierID", skierID);
            jsonResponse.addProperty("totalVertical", totalVertical);

            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(gson.toJson(jsonResponse));

        } catch (NumberFormatException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\": \"Invalid skierID, must be an integer\"}");
        }
    }

    private boolean validateRequest(JsonObject json) {
        try {
            int skierID = json.get("skierID").getAsInt();
            int resortID = json.get("resortID").getAsInt();
            int liftID = json.get("liftID").getAsInt();
            String seasonID = json.get("seasonID").getAsString();
            int dayID = json.get("dayID").getAsInt();
            int time = json.get("time").getAsInt();

            return skierID >= 1 && skierID <= 100000 &&
                    resortID >= 1 && resortID <= 10 &&
                    liftID >= 1 && liftID <= 40 &&
                    "2025".equals(seasonID) &&
                    dayID == 1 &&
                    time >= 1 && time <= 360;
        } catch (Exception e) {
            return false;
        }
    }
}
