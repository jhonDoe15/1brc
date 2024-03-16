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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;


public class CalculateAverage_jhonDoe15 {

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

    private static String getStationNameFromLine(String line) {
        int sepIdx = line.indexOf(';');
//        String station = line.substring(0, sepIdx);
//        String valueStr = line.substring(sepIdx + 1);
//        float value = Double.parseDouble(valueStr);

        return line.substring(0, sepIdx);
    }

    private static float getMeasurementFromLine(String line) {
        int sepIdx = line.indexOf(';');
//        String station = line.substring(0, sepIdx);
//        String valueStr = line.substring(sepIdx + 1);
//        float value = Double.parseDouble(valueStr);

        return Float.parseFloat(line.substring(sepIdx + 1));
    }


    public static void main(String[] args) throws IOException {

        long startTime = System.nanoTime();

        ConcurrentHashMap<String, StationData> stations = new ConcurrentHashMap<>();

        Stream<String> lines = Files.lines(Paths.get(FILE));

        lines.parallel()
                .map(CalculateAverage_jhonDoe15::splitLine)
                .forEach(l -> handleMeasurementClaude(stations, l));

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

    private static void handleMeasurement(ConcurrentHashMap<String, StationData> stations, String[] lineParts) {
        String station = lineParts[0];
        float value = parseDouble(lineParts[1]);

        StationData stationData = stations.getOrDefault(station, new StationData());

        // StationData stationData = stations.containsKey(station) ? stations.get(station) : new StationData();

        stationData.min = Math.min(stationData.min, value);
        stationData.max = Math.max(stationData.max, value);
        stationData.count += 1;
        stationData.sum += value;

        stations.put(station, stationData);
    }

    private static void handleMeasurementClaude(ConcurrentHashMap<String, StationData> stations, String[] lineParts) {
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

//    private static void handleMeasurementClaudePlus(ConcurrentHashMap<String, StationData> stations, String lineParts) {
//        String station = lineParts[0];
//        float measurement = parseFloat(lineParts);
//
//        // Using computeIfPresent and computeIfAbsent
//        stations.computeIfPresent(station, (_, value) -> {
//            value.min = Math.min(value.min, measurement);
//            value.max = Math.max(value.max, measurement);
//            value.count += 1;
//            value.sum += measurement;
//            return value;
//        });
//
//        stations.computeIfAbsent(station, _ -> new StationData(measurement));
//    }

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
