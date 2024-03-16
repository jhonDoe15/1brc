/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class CalculateAverage_jhonDoe15_threading {

    private static final String FILE = "./measurements.txt";

    // private static record Measurement(String station, float value) {
    // private Measurement(String[] parts) {
    // this(parts[0], Double.parseDouble(parts[1]));
    // }
    // }

    private record ResultRow(float min, float mean, float max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private float round(float value) {
            return (float) (Math.round(value * 10.0) / 10.0);
        }
    }

    // private static class StationData {
    // private float min = Double.POSITIVE_INFINITY;
    // private float max = Double.NEGATIVE_INFINITY;
    // private float sum;
    // private long count = 0;
    // }

    private static class StationData {
        float min;
        float max;
        float sum;
        long count;

        public StationData() {
            min = Float.POSITIVE_INFINITY;
            max = Float.NEGATIVE_INFINITY;
        }

        public StationData(float measurement) {
            this.min = measurement;
            this.max = measurement;
            this.sum = measurement;
            this.count = 1L;
        }
    }
    private static String[] splitLine(String line) {
        int sepIdx = line.indexOf(';');
//        String station = line.substring(0, sepIdx);
//        String valueStr = line.substring(sepIdx + 1);
//        float value = Double.parseDouble(valueStr);

        return new String[]{line.substring(0, sepIdx), line.substring(sepIdx + 1)};
    }


    public static void main(String[] args) throws IOException {

        long startTime = System.nanoTime();

        ConcurrentHashMap<String, StationData> stations = new ConcurrentHashMap<>();

        // Get the number of logical processors in the computer
        int numProcessors = Runtime.getRuntime().availableProcessors();

        // Create ExecutorService with a fixed thread pool size equal to the number of processors
        ExecutorService executor = Executors.newFixedThreadPool(numProcessors);

        final int bufferSize = 16384; // or any other appropriate value (e.g., 8KB)
        final int BATCH_SIZE = 150;

        try (BufferedReader br = new BufferedReader(new FileReader(FILE), bufferSize)) {
            String[] batchLines = new String[BATCH_SIZE];
            String line;
            int i = 0;
            while ((line = br.readLine()) != null) {
                batchLines[i++] = line;

                // If the batch size is reached, submit a task to process the batch
                if (i >= BATCH_SIZE) {
                    String[] finalBatchLines = batchLines;
                    executor.submit(() -> handleMeasurementBatch(stations, finalBatchLines));
                    batchLines = new String[BATCH_SIZE];
                    i = 0;
                }
            }

            // Process the remaining lines (if any)
            if (i != 0) {
                String[] finalBatchLines1 = batchLines;
                executor.submit(() -> handleMeasurementBatch(stations, finalBatchLines1));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Shutdown the executor after all tasks are submitted
        executor.shutdown();

        // Wait until all tasks have completed or until the executor is terminated
        try {
            //noinspection ResultOfMethodCallIgnored
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }

        List<ResultRow> resultRows = stations.keySet()
                .stream()
                .parallel()
                .map(name -> calcStationAvg(name, stations))
                .toList();
        System.out.println(resultRows);
        // System.out.println(measurements);
        long timeTaken = System.nanoTime() - startTime;
        System.out.printf("%s ms%n", timeTaken / 1000 / 1000);
    }



    private static ResultRow calcStationAvg(String name, ConcurrentHashMap<String, StationData> stations) {
        StationData stationData = stations.get(name);
        return new ResultRow(stationData.min, round(stationData.sum / stationData.count), stationData.max);
    }

    private static float round(float unRoundedAverage) {
        return (float) (Math.round(unRoundedAverage * 10.0) / 10.0);
    }

    private static void handleMeasurementBatch(ConcurrentHashMap<String, StationData> stations, String[] batchLines) {
        for (int i = 0; i < batchLines.length; i++) {
            // Process each line within the batch
            handleMeasurementClaudePlus(stations, batchLines[i]);
        }
    }
    private static void handleMeasurementClaudePlus(ConcurrentHashMap<String, StationData> stations, String line) {
        String[] lineParts = splitLine(line);
        String station = lineParts[0];
        float measurement = parseFloat(lineParts);

        // Using computeIfPresent and computeIfAbsent
        stations.computeIfPresent(station, (_, value) -> {
            value.min = Math.min(value.min, measurement);
            value.max = Math.max(value.max, measurement);
            value.count += 1;
            value.sum += measurement;
            return value;
        });

        stations.computeIfAbsent(station, _ -> new StationData(measurement));
    }

    private static float parseFloat(String[] lineParts) {
        return Float.parseFloat(lineParts[1]);
    }

    private static float parseDouble(String str) {
        // Assuming str is a valid float representation
        // Parse str manually without using Double.parseDouble
        int dotIndex = str.indexOf('.');
        int integerPart = Integer.parseInt(str.substring(0, dotIndex));
        int decimalPart = Integer.parseInt(str.substring(dotIndex + 1, dotIndex + 2));
        return (float) (integerPart + decimalPart * 0.1);
    }

}
