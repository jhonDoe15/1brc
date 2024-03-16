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

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;


public class CalculateAverage_jhonDoe15_3 {

    private static final String FILE = "./measurements.txt";

    // private static record Measurement(String station, double value) {
    // private Measurement(String[] parts) {
    // this(parts[0], Double.parseDouble(parts[1]));
    // }
    // }

    private record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
    private static String[] splitLine(String line) {
        int sepIdx = line.indexOf(';');
        return new String[]{line.substring(0, sepIdx), line.substring(sepIdx + 1)};
    }


    public static void main(String[] args) throws IOException {
        long startTime = System.nanoTime();

        Stream<String> lines = Files.lines(Paths.get(FILE));

        // Collect and convert List<Double> to double[]

        Map<String, ResultRow> stationsParsedData = lines.parallel()
                .map(CalculateAverage_jhonDoe15_3::splitLine)
                .collect(
                                Collectors.groupingBy(
                                        line -> line[0],
                                        Collectors.mapping(
                                                splittedLine -> Double.parseDouble(splittedLine[1]),
                                                Collectors.toList()
                                        )
                                )
                )
                .entrySet()
                .stream()
                .parallel()
                .collect(toMap(Map.Entry::getKey,
                        station -> {
                            double[] measurementArray = station.getValue().stream().mapToDouble(Double::doubleValue).toArray();
                            double min = getMin(measurementArray);
                            double max = getMax(measurementArray);
                            double mean = getAvg(measurementArray);
                            return new ResultRow(
                                    min,
                                    mean,
                                    max
                            );
                        }
                ));
        System.out.println(stationsParsedData);

        long timeTaken = System.nanoTime() - startTime;
        System.out.printf("%s ms%n", timeTaken / 1000 / 1000);
    }

    private static ResultRow getStationResultInnerCasting(List<Double> measurementList) {
        double[] measurementArray = measurementList.stream().mapToDouble(Double::doubleValue).toArray();
        double min = getMin(measurementArray);
        double max = getMax(measurementArray);
        double mean = getAvg(measurementArray);
        return new ResultRow(
                min,
                mean,
                max
        );
    }
    static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;

    public static double getAvg(double[] arr) {
        double sum = 0;
        for (int i = 0; i< arr.length; i += SPECIES.length()) {
            var mask = SPECIES.indexInRange(i, arr.length);
            var V = DoubleVector.fromArray(SPECIES, arr, i, mask);
            sum += V.reduceLanes(VectorOperators.ADD, mask);
        }
        return sum / arr.length;
    }

    public static double getMin(double[] arr) {
        double min = Double.POSITIVE_INFINITY;
        for (int i = 0; i < arr.length; i += SPECIES.length()) {
            var mask = SPECIES.indexInRange(i, arr.length);
            var tempMaskedVector = DoubleVector.fromArray(SPECIES, arr, i, mask);
            min = Math.min(min, tempMaskedVector.reduceLanes(VectorOperators.MIN, mask));
        }
        return min;
    }

    public static double getMax(double[] arr) {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < arr.length; i += SPECIES.length()) {
            var mask = SPECIES.indexInRange(i, arr.length);
            var tempMaskedVector = DoubleVector.fromArray(SPECIES, arr, i, mask);
            max = Math.max(max, tempMaskedVector.reduceLanes(VectorOperators.MAX, mask));
        }
        return max;
    }
}
