package com.cs6650.client2;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class ThroughputPlotter {
    public static void main(String[] args) {
        String csvFile = "C:/Users/liume_lhvc42h/Desktop/CS6650-Assignment1/CS6650-Client2/request_records.csv";
        String outputFile = "throughput_plot.png";

        try {
            Map<Long, Integer> requestsBySecond = new HashMap<>();
            long startTime = -1;

            Files.lines(Paths.get(csvFile))
                    .skip(1)
                    .forEach(line -> {
                        String[] parts = line.split(",");
                        long timestamp = Long.parseLong(parts[0]);
                        long second = timestamp / 1000;
                        requestsBySecond.merge(second, 1, Integer::sum);
                    });

            TimeSeries series = new TimeSeries("Throughput");
            startTime = requestsBySecond.keySet().stream().min(Long::compareTo).orElse(0L);

            requestsBySecond.forEach((second, count) -> {
                series.add(new Second(new java.util.Date(second * 1000)),
                        count.doubleValue());
            });

            TimeSeriesCollection dataset = new TimeSeriesCollection();
            dataset.addSeries(series);

            JFreeChart chart = ChartFactory.createTimeSeriesChart(
                    "Throughput Over Time",
                    "Time (seconds)",
                    "Requests per Second",
                    dataset,
                    true,
                    true,
                    false
            );

            XYPlot plot = (XYPlot) chart.getPlot();
            plot.setDomainGridlinesVisible(true);
            plot.setRangeGridlinesVisible(true);
            NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
            rangeAxis.setAutoRangeIncludesZero(true);

            ChartUtils.saveChartAsPNG(
                    new File(outputFile),
                    chart,
                    800,
                    600
            );

            System.out.println("Chart has been generated: " + outputFile);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}