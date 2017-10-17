/*
 * Copyright (C) 2016 Hylke van der Schaaf.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library. If not, see <http://www.gnu.org/licenses/>.
 */
package de.fraunhofer.iosb.ilt.tests;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import de.fraunhofer.iosb.ilt.sta.service.TokenManager;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.geojson.LineString;
import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.geojson.Polygon;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

/**
 *
 * @author Hylke van der Schaaf
 */
public class CreateEntities {

    /**
     * The number of observations that will be created.
     */
    private static final int OBSERVATION_COUNT = 50;

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateEntities.class.getName());
    private SensorThingsService service;
    private TokenManager tokenManager;
    private final List<Thing> things = new ArrayList<>();
    private final List<Location> locations = new ArrayList<>();
    private final List<Sensor> sensors = new ArrayList<>();
    private final List<ObservedProperty> oProps = new ArrayList<>();
    private final List<Datastream> datastreams = new ArrayList<>();
    private final List<Observation> observations = new ArrayList<>();

    /**
     * @param args the command line arguments
     * @throws ServiceFailureException when there is an error.
     * @throws java.net.URISyntaxException
     * @throws java.net.MalformedURLException
     */
    public static void main(String[] args) throws ServiceFailureException, URISyntaxException, MalformedURLException {
        LOGGER.info("Creating test entities in {}", Constants.BASE_URL);
        CreateEntities tester = new CreateEntities();
        tester.createEntities();
    }

    public CreateEntities() throws MalformedURLException, URISyntaxException {
        service = Constants.createService();
    }

    private void createEntities2() throws ServiceFailureException, URISyntaxException, MalformedURLException {
        Thing thing = new Thing("ILThing", "Das Ding von ILT.");
        service.create(thing);
        things.add(thing);

        Location location = new Location("Location Des Dings von ILT", "First Location of Thing 1.", "application/vnd.geo+json", new Point(8, 49));
        location.getThings().add(things.get(0));
        service.create(location);
        locations.add(location);

        Sensor sensor1 = new Sensor("Sensor 1", "Temperatur Sensor Ding ILT.", "text", "Some metadata.");
        service.create(sensor1);
        sensors.add(sensor1);

        Sensor sensor2 = new Sensor("Sensor 2", "Luftfeuchte Sensor Ding ILT", "text", "Some metadata.");
        service.create(sensor2);
        sensors.add(sensor2);

        ObservedProperty obsProp1 = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "The temperature of the thing.");
        service.create(obsProp1);
        oProps.add(obsProp1);

        ObservedProperty obsProp2 = new ObservedProperty("Humidity", new URI("http://ucom.org/humidity"), "The humidity of the thing.");
        service.create(obsProp2);
        oProps.add(obsProp2);

        thing = things.get(0);
        Datastream datastream1 = new Datastream("Datastream Temp", "The temperature of thing 1, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
        datastream1.setThing(thing);
        datastream1.setSensor(sensor1);
        datastream1.setObservedProperty(obsProp1);
        service.create(datastream1);
        datastreams.add(datastream1);

        Datastream datastream2 = new Datastream("Datastream LF", "The humidity of thing 1, sensor 2.", "someType", new UnitOfMeasurement("relative humidity", "%", "ucum:Humidity"));
        datastream2.setThing(thing);
        datastream2.setSensor(sensor2);
        datastream2.setObservedProperty(obsProp2);
        service.create(datastream2);
        datastreams.add(datastream2);
    }

    private void createEntities() throws ServiceFailureException, URISyntaxException, MalformedURLException {
        Map<String, Object> deep1 = new HashMap<>();
        Map<String, Object> deep2 = new HashMap<>();
        Map<String, Object> properties1 = new HashMap<>();
        properties1.put("name", "properties1");
        properties1.put("string", "yes");
        properties1.put("boolean", true);
        properties1.put("integer", 9);
        properties1.put("array", new int[]{1, 2, 3, 4});
        properties1.put("array2", new int[][]{{0, 1}, {1, 2}, {2, 3}, {3, 4}});
        properties1.put("array3", new Object[]{deep1, deep2});
        deep1.put("deep1.1", new int[]{1, 2, 3, 4});
        deep1.put("deep1.2", new int[]{2, 3, 4, 5});
        deep2.put("string", "yes");
        deep2.put("boolean", true);
        deep2.put("integer", 9);
        Thing thing = new Thing("Thing 1", "The first thing.");
        thing.setProperties(properties1);
        service.create(thing);
        LOGGER.info("id: " + thing.getId());
        things.add(thing);

        Map<String, Object> properties2 = new HashMap<>();
        properties2.put("name", "properties2");
        properties2.put("string", "no");
        properties2.put("boolean", false);
        properties2.put("integer", 10);
        properties2.put("array", new int[]{4, 3, 2, 1});
        properties2.put("deep", properties1);
        thing = new Thing("Thing 2", "The second thing.");
        thing.setProperties(properties2);
        service.create(thing);
        things.add(thing);

        Map<String, Object> properties3 = new HashMap<>();
        properties3.put("name", "properties3");
        properties3.put("string", "yes");
        properties3.put("boolean", true);
        properties3.put("integer", 11);
        properties3.put("array", new int[]{2, 1, 4, 3});
        thing = new Thing("Thing 3", "The third thing.");
        thing.setProperties(properties3);
        service.create(thing);
        things.add(thing);

        Map<String, Object> properties4 = new HashMap<>();
        properties4.put("name", "properties4");
        properties4.put("string", "no");
        properties4.put("boolean", false);
        properties4.put("integer", 1);
        properties4.put("array", new int[]{3, 4, 1, 2});
        thing = new Thing("Thing 4", "The fourt thing.");
        thing.setProperties(properties4);
        service.create(thing);
        things.add(thing);

        Location location = new Location("Location 1.0", "First Location of Thing 1.", "application/vnd.geo+json", new Point(8, 52));
        location.getThings().add(things.get(0));
        service.create(location);
        locations.add(location);

        location = new Location("Location 1.1", "Second Location of Thing 1.", "application/vnd.geo+json", new Point(8, 52));
        location.getThings().add(things.get(0));
        service.create(location);
        locations.add(location);

        location = new Location("Location 2", "Location of Thing 2.", "application/vnd.geo+json", new Point(8, 53));
        location.getThings().add(things.get(1));
        service.create(location);
        locations.add(location);

        location = new Location("Location 3", "Location of Thing 3.", "application/vnd.geo+json", new Point(8, 54));
        location.getThings().add(things.get(2));
        service.create(location);
        locations.add(location);

        // Locations 4
        location = new Location("Location 4", "Location of Thing 4.", "application/vnd.geo+json",
                new Polygon(
                        new LngLatAlt(8, 53),
                        new LngLatAlt(7, 52),
                        new LngLatAlt(7, 53),
                        new LngLatAlt(8, 53)));
        location.getThings().add(things.get(3));
        service.create(location);
        locations.add(location);

        // Locations 5
        location = new Location("Location 5", "A line.", "application/vnd.geo+json",
                new LineString(
                        new LngLatAlt(5, 52),
                        new LngLatAlt(5, 53)));
        service.create(location);
        locations.add(location);

        // Locations 6
        location = new Location("Location 6", "A longer line.", "application/vnd.geo+json",
                new LineString(
                        new LngLatAlt(5, 52),
                        new LngLatAlt(6, 53)));
        service.create(location);
        locations.add(location);

        // Locations 7
        location = new Location("Location 7", "The longest line.", "application/vnd.geo+json",
                new LineString(
                        new LngLatAlt(4, 52),
                        new LngLatAlt(8, 52)));
        service.create(location);
        locations.add(location);

        Sensor sensor1 = new Sensor("Sensor 1", "The first sensor.", "text", "Some metadata.");
        service.create(sensor1);
        sensors.add(sensor1);

        Sensor sensor2 = new Sensor("Sensor 2", "The second sensor.", "text", "Some metadata.");
        service.create(sensor2);
        sensors.add(sensor2);

        ObservedProperty obsProp1 = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "The temperature of the thing.");
        service.create(obsProp1);
        oProps.add(obsProp1);

        ObservedProperty obsProp2 = new ObservedProperty("Humidity", new URI("http://ucom.org/humidity"), "The humidity of the thing.");
        service.create(obsProp2);
        oProps.add(obsProp2);

        thing = things.get(0);
        Datastream datastream1 = new Datastream("Datastream 1", "The temperature of thing 1, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
        datastream1.setThing(thing);
        datastream1.setSensor(sensor1);
        datastream1.setObservedProperty(obsProp1);
        service.create(datastream1);
        datastreams.add(datastream1);

        Datastream datastream2 = new Datastream("Datastream 2", "The humidity of thing 1, sensor 2.", "someType", new UnitOfMeasurement("relative humidity", "%", "ucum:Humidity"));
        datastream2.setThing(thing);
        datastream2.setSensor(sensor2);
        datastream2.setObservedProperty(obsProp2);
        service.create(datastream2);
        datastreams.add(datastream2);

        Datastream datastream3 = new Datastream("Datastream 3", "The humidity of thing 2, sensor 2.", "someType", new UnitOfMeasurement("relative humidity", "%", "ucum:Humidity"));
        datastream3.setThing(things.get(1));
        datastream3.setSensor(sensor2);
        datastream3.setObservedProperty(obsProp2);
        service.create(datastream3);
        datastreams.add(datastream3);

        Observation o = new Observation(new int[]{1, 2, 3, 4}, datastream1);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-01T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-01T01:01:01.000Z"), Instant.parse("2016-01-01T23:59:59.999Z")));
        service.create(o);
        observations.add(o);

        o = new Observation(2, datastream1);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-02T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-02T01:01:01.000Z"), Instant.parse("2016-01-02T23:59:59.999Z")));
        service.create(o);
        observations.add(o);

        o = new Observation(3, datastream1);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-03T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-03T01:01:01.000Z"), Instant.parse("2016-01-03T23:59:59.999Z")));
        service.create(o);
        observations.add(o);

        o = new Observation(4, datastream1);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-04T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-04T01:01:01.000Z"), Instant.parse("2016-01-04T23:59:59.999Z")));
        service.create(o);
        observations.add(o);

        o = new Observation(5, datastream2);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-04T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-04T01:01:01.000Z"), Instant.parse("2016-01-04T23:59:59.999Z")));
        service.create(o);
        observations.add(o);

        {
            Map<String, Object> parameters = new HashMap<>();
            o = new Observation("bad", datastream1);
            parameters.put("int", generateString(0, 10));
            parameters.put("string", 0 % 2 == 0);
            parameters.put("boolean", 0);
            parameters.put("intArray", generateIntArray(0, 5));
            parameters.put("intIntArray", generateIntIntArray(0, 3));
            parameters.put("objArray", generateObjectList(0, 3));
            o.setParameters(parameters);
            service.create(o);
            observations.add(o);
        }

        ExecutorService pool = Executors.newFixedThreadPool(5);

        int totalCount = OBSERVATION_COUNT;
        int perTask = 10000;

        long startTime = Calendar.getInstance().getTimeInMillis();
        Duration delta = Duration.standardMinutes(1);
        DateTime dtStart = DateTime.now().minus(delta.multipliedBy(totalCount));

        int start = 0;
        while (start < totalCount) {
            if (start + perTask >= totalCount) {
                perTask = totalCount - start;
            }
            obsCreator obsCreator = new obsCreator(
                    new SensorThingsService(new URL(Constants.BASE_URL)).setTokenManager(service.getTokenManager()),
                    datastream1, start, perTask, dtStart, delta);
            pool.submit(obsCreator);
            LOGGER.info("Submitted task for {} observations starting at {}.", perTask, start);
            start += perTask;
        }
        try {
            pool.shutdown();
            pool.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException ex) {
            LOGGER.info("Pool prepaturely interrupted.", ex);
        }

        long endTime = Calendar.getInstance().getTimeInMillis();
        long duration = endTime - startTime;
        double secs = duration / 1000.0;
        LOGGER.info("Created {} obs in {}ms, {}/s.", totalCount, duration, totalCount / secs);
    }

    /**
     * Generates a string of letters, with the given length, starting at the
     * given letter, where a=0.
     *
     * @param startLetter the starting letter (a=0).
     * @param length The length of the string to generate.
     * @return The string.
     */
    public static String generateString(int startLetter, int length) {
        StringBuilder sb = new StringBuilder();
        char curLetter = (char) ('a' + startLetter % 26);
        for (int i = 0; i < length; i++) {
            sb.append(curLetter);
            curLetter++;
            if (curLetter > 'z') {
                curLetter = 'a';
            }
        }
        return sb.toString();
    }

    /**
     * Generates an array of numbers, with the given length, starting at the
     * given number.
     *
     * @param startValue the starting number.
     * @param length The length of the array to generate.
     * @return The string.
     */
    public static int[] generateIntArray(int startValue, int length) {
        int[] value = new int[length];
        int curVal = startValue;
        for (int i = 0; i < length; i++) {
            value[i] = curVal;
            curVal++;
        }
        return value;
    }

    public static int[][] generateIntIntArray(int startValue, int length) {
        int[][] value = new int[length][];
        int curVal = startValue;
        for (int i = 0; i < length; i++) {
            value[i] = generateIntArray(curVal, length);
            curVal++;
        }
        return value;
    }

    public static List<Object> generateObjectList(int startValue, int length) {
        List<Object> value = new ArrayList<>();
        int curVal = startValue;
        for (int i = 0; i < length; i++) {
            Map<String, Object> newObject = new HashMap<>();
            newObject.put("string", generateString(curVal, 10));
            newObject.put("boolean", curVal % 2 == 0);
            newObject.put("int", curVal);
            newObject.put("intArray", generateIntArray(curVal, 3));
            value.add(newObject);
            curVal++;
        }
        return value;
    }

    private final static class obsCreator implements Runnable {

        private final SensorThingsService service;
        private final Datastream datastream;
        private final int start;
        private final int count;
        private final DateTime startTime;
        private final Duration deltaPerObs;
        private final Map<String, Object> parameters = new HashMap<>();

        public obsCreator(SensorThingsService service, Datastream datastream, int start, int count, DateTime startTime, Duration deltaPerObs) {
            this.service = service;
            this.datastream = datastream;
            this.start = start;
            this.count = count;
            this.startTime = startTime;
            this.deltaPerObs = deltaPerObs;
        }

        @Override
        public void run() {
            int end = start + count;
            int i = 0;
            LOGGER.info("Creating {} observations from {} to {}.", count, start, end);
            try {
                for (i = start; i < end; i++) {
                    Observation o = new Observation(i, datastream);
                    parameters.put("string", generateString(i, 10));
                    parameters.put("boolean", i % 2 == 0);
                    parameters.put("int", i);
                    parameters.put("intArray", generateIntArray(i, 5));
                    parameters.put("intIntArray", generateIntIntArray(i, 3));
                    parameters.put("objArray", generateObjectList(i, 3));
                    o.setParameters(parameters);
                    long millis = startTime.plus(deltaPerObs.multipliedBy(i)).getMillis();
                    o.setPhenomenonTimeFrom(ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()));
                    service.create(o);
                }
            } catch (ServiceFailureException ex) {
                LOGGER.error("Failed to create observation {}: {}", i);
                LOGGER.error("", ex);

            }
            LOGGER.info("Done creating {} observations from {} to {}.", count, start, end);
        }

    }

}
