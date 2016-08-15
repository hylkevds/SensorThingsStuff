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

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.geojson.Polygon;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;

/**
 *
 * @author Hylke van der Schaaf
 */
public class CreateEntities {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateEntities.class.getName());
    private static final int OBSERVATION_COUNT = 500000;
    private final URI baseUri;
    private SensorThingsService service;
    private final List<Thing> things = new ArrayList<>();
    private final List<Location> locations = new ArrayList<>();
    private final List<Sensor> sensors = new ArrayList<>();
    private final List<ObservedProperty> oProps = new ArrayList<>();
    private final List<Datastream> datastreams = new ArrayList<>();
    private final List<Observation> observations = new ArrayList<>();

    /**
     * @param args the command line arguments
     * @throws ServiceFailureException when there is an error.
     */
    public static void main(String[] args) throws ServiceFailureException, URISyntaxException {
        URI baseUri = URI.create("http://localhost:8080/SensorThingsService/v1.0/");
        CreateEntities tester = new CreateEntities(baseUri);
        tester.createEntities();
    }

    public CreateEntities() {
        this.baseUri = null;
    }

    public CreateEntities(URI baseUri) throws URISyntaxException {
        this.baseUri = baseUri;
        service = new SensorThingsService(baseUri);
    }

    private void createEntities() throws ServiceFailureException, URISyntaxException {
        Thing thing = new Thing("Thing 1", "The first thing.");
        service.create(thing);
        things.add(thing);

        thing = new Thing("Thing 2", "The second thing.");
        service.create(thing);
        things.add(thing);

        thing = new Thing("Thing 3", "The third thing.");
        service.create(thing);
        things.add(thing);

        thing = new Thing("Thing 4", "The fourt thing.");
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

        Observation o = new Observation(1, datastream1);
        o.setPhenomenonTime(ZonedDateTime.parse("2016-01-01T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-01T01:01:01.000Z"), Instant.parse("2016-01-01T23:59:59.999Z")));
        service.create(o);
        observations.add(o);

        o = new Observation(2, datastream1);
        o.setPhenomenonTime(ZonedDateTime.parse("2016-01-02T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-02T01:01:01.000Z"), Instant.parse("2016-01-02T23:59:59.999Z")));
        service.create(o);
        observations.add(o);

        o = new Observation(3, datastream1);
        o.setPhenomenonTime(ZonedDateTime.parse("2016-01-03T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-03T01:01:01.000Z"), Instant.parse("2016-01-03T23:59:59.999Z")));
        service.create(o);
        observations.add(o);

        o = new Observation(4, datastream1);
        o.setPhenomenonTime(ZonedDateTime.parse("2016-01-04T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-04T01:01:01.000Z"), Instant.parse("2016-01-04T23:59:59.999Z")));
        service.create(o);
        observations.add(o);

        ExecutorService pool = Executors.newFixedThreadPool(5);

        int totalCount = OBSERVATION_COUNT;
        int perTask = 10000;

        long startTime = Calendar.getInstance().getTimeInMillis();
        Duration delta = Duration.standardMinutes(1);
        DateTime dtStart = DateTime.now().minus(delta.multipliedBy(totalCount));

        int start = 0;
        while (start < totalCount) {
            obsCreator obsCreator = new obsCreator(new SensorThingsService(baseUri), datastream1, start, perTask, dtStart, delta);
            pool.submit(obsCreator);
            LOGGER.info("Submitted task for {} observations starting at {}.", perTask, start);
            start += perTask;
        }
        start = 0;
        while (start < totalCount) {
            obsCreator obsCreator = new obsCreator(new SensorThingsService(baseUri), datastream2, start, perTask, dtStart, delta);
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
        LOGGER.info("Created {} obs in {}ms.", totalCount, duration);
    }

    private final static class obsCreator implements Runnable {

        private final SensorThingsService service;
        private final Datastream datastream;
        private final int start;
        private final int count;
        private final DateTime startTime;
        private final Duration deltaPerObs;

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
                    long millis = startTime.plus(deltaPerObs.multipliedBy(i)).getMillis();
                    o.setPhenomenonTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()));
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
