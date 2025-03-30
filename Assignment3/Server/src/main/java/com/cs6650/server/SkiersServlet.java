package com.cs6650.server;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

@WebServlet(name = "SkiersServlet", urlPatterns = "/skiers/*")
public class SkiersServlet extends HttpServlet {
    private static final Gson gson = new Gson();
    private static final Logger logger = Logger.getLogger(SkiersServlet.class.getName());
    private static final String QUEUE_NAME = "skier_lift_rides";

    private Connection connection;
    private BlockingQueue<Channel> channelPool;
    private final int POOL_SIZE = 100;

    @Override
    public void init() throws ServletException {
        super.init();
        try {
            logger.info("Initializing RabbitMQ connection...");

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("34.217.89.157");
            factory.setPort(5672);
            factory.setUsername("guest");
            factory.setPassword("guest");

            factory.setAutomaticRecoveryEnabled(true);

            connection = factory.newConnection();
            logger.info("RabbitMQ connection established successfully");

            channelPool = new LinkedBlockingQueue<>(POOL_SIZE);
            logger.info("Creating channel pool with size: " + POOL_SIZE);

            for (int i = 0; i < POOL_SIZE; i++) {
                Channel channel = connection.createChannel();
                // Keep original queue declaration to maintain compatibility
                channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                channelPool.add(channel);
                if (i % 10 == 0) {
                    logger.info("Created " + (i + 1) + " channels");
                }
            }

            logger.info("RabbitMQ connection and channel pool initialized successfully");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to initialize RabbitMQ connection", e);
            throw new ServletException("Failed to initialize RabbitMQ connection", e);
        }
    }

    @Override
    public void destroy() {
        // Close all channels and connections
        try {
            logger.info("Shutting down RabbitMQ connections...");
            for (Channel channel : channelPool) {
                if (channel.isOpen()) {
                    channel.close();
                }
            }

            if (connection != null && connection.isOpen()) {
                connection.close();
            }

            logger.info("RabbitMQ connection and channels closed successfully");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error closing RabbitMQ resources", e);
        }
        super.destroy();
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        StringBuilder jsonString = new StringBuilder(1024);
        try (BufferedReader reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                jsonString.append(line);
            }
        }

        Channel channel = null;
        try {
            JsonObject jsonRequest = gson.fromJson(jsonString.toString(), JsonObject.class);

            if (!validateRequest(jsonRequest)) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write("{\"error\": \"Invalid request parameters\"}");
                return;
            }

            if (channelPool.size() < POOL_SIZE * 0.2) {
                logger.warning("Channel pool running low: " + channelPool.size() + " available out of " + POOL_SIZE);
            }

            channel = channelPool.take();

            channel.basicPublish("", QUEUE_NAME, null, jsonString.toString().getBytes(StandardCharsets.UTF_8));

            JsonObject jsonResponse = new JsonObject();
            jsonResponse.addProperty("message", "Lift ride recorded successfully.");
            jsonResponse.addProperty("skierID", jsonRequest.get("skierID").getAsInt());

            response.setStatus(HttpServletResponse.SC_CREATED);
            response.getWriter().write(gson.toJson(jsonResponse));

        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error processing request", ex);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            JsonObject errorResponse = new JsonObject();
            errorResponse.addProperty("error", "Invalid JSON format or messaging service unavailable.");
            response.getWriter().write(gson.toJson(errorResponse));
        } finally {
            if (channel != null) {
                try {
                    channelPool.put(channel);
                } catch (InterruptedException e) {
                    logger.log(Level.WARNING, "Interrupted while returning channel to pool", e);
                    Thread.currentThread().interrupt();
                }
            }
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