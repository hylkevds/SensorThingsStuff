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
package de.fraunhofer.ilt.sta;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.dao.BaseDao;
import de.fraunhofer.iosb.ilt.sta.dao.ObservationDao;
import de.fraunhofer.iosb.ilt.sta.dao.ThingDao;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import de.fraunhofer.iosb.ilt.tests.Utils;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import org.geojson.LineString;
import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.geojson.Polygon;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threeten.extra.Interval;

/**
 * TODO.
 * <ul>
 * <li>geo.length</li>
 * </ul>
 *
 * @author Hylke van der Schaaf
 */
public class FilterTests {

    private static final String BASE_URI = "http://localhost:8080/SensorThingsService/v1.0";
    private static SensorThingsService service;
    private static final List<Thing> THINGS = new ArrayList<>();
    private static final List<Location> LOCATIONS = new ArrayList<>();
    private static final List<Sensor> SENSORS = new ArrayList<>();
    private static final List<ObservedProperty> O_PROPS = new ArrayList<>();
    private static final List<Datastream> DATASTREAMS = new ArrayList<>();
    private static final List<Observation> OBSERVATIONS = new ArrayList<>();

    public FilterTests() {
    }

    @BeforeClass
    public static void setUpClass() throws URISyntaxException, ServiceFailureException, MalformedURLException {
        URL uri = new URL(BASE_URI);
        service = new SensorThingsService(uri);
        createEntities();
    }

    @AfterClass
    public static void tearDownClass() throws ServiceFailureException {
        Utils.deleteAll(service);
    }

    private static void createEntities() throws ServiceFailureException, URISyntaxException {
        Thing thing = new Thing("Thing 1", "The first thing.");
        service.create(thing);
        THINGS.add(thing);

        thing = new Thing("Thing 2", "The second thing.");
        service.create(thing);
        THINGS.add(thing);

        thing = new Thing("Thing 3", "The third thing.");
        service.create(thing);
        THINGS.add(thing);

        thing = new Thing("Thing 4", "The fourt thing.");
        service.create(thing);
        THINGS.add(thing);

        // Locations 0
        Location location = new Location("Location 1.0", "First Location of Thing 1.", "application/vnd.geo+json", new Point(8, 51));
        location.getThings().add(THINGS.get(0));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 1
        location = new Location("Location 1.1", "Second Location of Thing 1.", "application/vnd.geo+json", new Point(8, 52));
        location.getThings().add(THINGS.get(0));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 2
        location = new Location("Location 2", "Location of Thing 2.", "application/vnd.geo+json", new Point(8, 53));
        location.getThings().add(THINGS.get(1));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 3
        location = new Location("Location 3", "Location of Thing 3.", "application/vnd.geo+json", new Point(8, 54));
        location.getThings().add(THINGS.get(2));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 4
        location = new Location("Location 4", "Location of Thing 4.", "application/vnd.geo+json",
                new Polygon(
                        new LngLatAlt(8, 53),
                        new LngLatAlt(7, 52),
                        new LngLatAlt(7, 53),
                        new LngLatAlt(8, 53)));
        location.getThings().add(THINGS.get(3));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 5
        location = new Location("Location 5", "A line.", "application/vnd.geo+json",
                new LineString(
                        new LngLatAlt(5, 52),
                        new LngLatAlt(5, 53)));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 6
        location = new Location("Location 6", "A longer line.", "application/vnd.geo+json",
                new LineString(
                        new LngLatAlt(5, 52),
                        new LngLatAlt(6, 53)));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 7
        location = new Location("Location 7", "The longest line.", "application/vnd.geo+json",
                new LineString(
                        new LngLatAlt(4, 52),
                        new LngLatAlt(8, 52)));
        service.create(location);
        LOCATIONS.add(location);

        Sensor sensor = new Sensor("Sensor 1", "The first sensor.", "text", "Some metadata.");
        service.create(sensor);
        SENSORS.add(sensor);

        ObservedProperty obsProp = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "The temperature of the thing.");
        service.create(obsProp);
        O_PROPS.add(obsProp);

        Datastream datastream = new Datastream("Datastream 1", "The temperature of thing 1, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
        datastream.setThing(thing);
        datastream.setSensor(sensor);
        datastream.setObservedProperty(obsProp);
        service.create(datastream);
        DATASTREAMS.add(datastream);

        Observation o = new Observation(1, datastream);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-01T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-01T01:01:01.000Z"), Instant.parse("2016-01-01T23:59:59.999Z")));
        service.create(o);
        OBSERVATIONS.add(o);

        o = new Observation(2, datastream);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-02T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-02T01:01:01.000Z"), Instant.parse("2016-01-02T23:59:59.999Z")));
        service.create(o);
        OBSERVATIONS.add(o);

        o = new Observation(3, datastream);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-03T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-03T01:01:01.000Z"), Instant.parse("2016-01-03T23:59:59.999Z")));
        service.create(o);
        OBSERVATIONS.add(o);

        o = new Observation(4, datastream);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-04T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-04T01:01:01.000Z"), Instant.parse("2016-01-04T23:59:59.999Z")));
        service.create(o);
        OBSERVATIONS.add(o);

    }

    public void filterAndCheck(BaseDao doa, String filter, List<? extends Entity> expected) {
        try {
            EntityList<Observation> result = doa.query().filter(filter).list();
            Utils.TestResult check = Utils.resultContains(result, expected);
            Assert.assertTrue("Failed on filter: " + filter + " Cause: " + check.message, check.testOk);
        } catch (ServiceFailureException ex) {
            Assert.fail("Failed to call service.");
        }
    }

    @Test
    public void testIndirectFilter() throws ServiceFailureException {
        ThingDao doa = service.things();
        filterAndCheck(doa, "Locations/name eq 'Location 2'", getFromList(THINGS, 1));
        filterAndCheck(doa, "startswith(HistoricalLocations/Location/name, 'Location 1')", getFromList(THINGS, 0));
    }

    @Test
    public void testTimeInterval() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, "validTime gt 2016-01-03T01:01:00Z", getFromList(OBSERVATIONS, 2, 3));

        filterAndCheck(doa, "validTime lt 2016-01-03T01:01:00Z", getFromList(OBSERVATIONS, 0, 1));

        filterAndCheck(doa, "validTime gt 2016-01-03T01:01:02Z", getFromList(OBSERVATIONS, 3));
        filterAndCheck(doa, "validTime lt 2016-01-03T01:01:02Z", getFromList(OBSERVATIONS, 0, 1));

        // time interval >= or <= time instant
        filterAndCheck(doa, "validTime ge 2016-01-03T01:01:02Z", getFromList(OBSERVATIONS, 2, 3));
        filterAndCheck(doa, "validTime le 2016-01-03T01:01:02Z", getFromList(OBSERVATIONS, 0, 1, 2));

        // time instant >= or <= time interval
        filterAndCheck(doa, "2016-01-03T01:01:02Z le validTime", getFromList(OBSERVATIONS, 2, 3));
        filterAndCheck(doa, "2016-01-03T01:01:02Z ge validTime", getFromList(OBSERVATIONS, 0, 1, 2));

        // time instant < or > time instant
        filterAndCheck(doa, "validTime lt 2016-01-02T01:01:01.000Z/2016-01-03T23:59:59.999Z", getFromList(OBSERVATIONS, 0));
        filterAndCheck(doa, "validTime gt 2016-01-02T01:01:01.000Z/2016-01-03T23:59:59.999Z", getFromList(OBSERVATIONS, 3));

        // time interval eq time instant
        filterAndCheck(doa, "not validTime lt 2016-01-03T12:00:00Z and not validTime gt 2016-01-03T12:00:00Z", getFromList(OBSERVATIONS, 2));
        filterAndCheck(doa, "validTime eq 2016-01-03T12:00:00Z", getFromList(OBSERVATIONS, 2));
        filterAndCheck(doa, "validTime ne 2016-01-03T12:00:00Z", getFromList(OBSERVATIONS, 0, 1, 3));

        // Durations
        filterAndCheck(doa, "validTime add duration'P1D' gt 2016-01-03T01:01:00Z", getFromList(OBSERVATIONS, 1, 2, 3));
        filterAndCheck(doa, "validTime gt 2016-01-03T01:01:00Z sub duration'P1D'", getFromList(OBSERVATIONS, 1, 2, 3));
        filterAndCheck(doa, "validTime sub duration'P1D' gt 2016-01-03T01:01:00Z", getFromList(OBSERVATIONS, 3));

        filterAndCheck(doa, "validTime lt 2016-01-02T01:01:01.000Z/2016-01-03T23:59:59.999Z add duration'P1D'", getFromList(OBSERVATIONS, 0, 1));
        filterAndCheck(doa, "validTime gt 2016-01-02T01:01:01.000Z/2016-01-03T23:59:59.999Z sub duration'P1D'", getFromList(OBSERVATIONS, 2, 3));

        // interval eq interval
        filterAndCheck(doa, "validTime eq 2016-01-02T01:01:01.000Z/2016-01-02T23:59:59.999Z", getFromList(OBSERVATIONS, 1));
        filterAndCheck(doa, "validTime ne 2016-01-02T01:01:01.000Z/2016-01-02T23:59:59.999Z", getFromList(OBSERVATIONS, 0, 2, 3));

        filterAndCheck(doa, "phenomenonTime sub 2016-01-03T01:01:01.000Z eq duration'P1D'", getFromList(OBSERVATIONS, 3));

    }

    @Test
    public void testGeoDistance() throws ServiceFailureException {
        filterAndCheck(service.locations(), "geo.distance(location, geography'POINT(8 54.1)') lt 1", getFromList(LOCATIONS, 3));
    }

    @Test
    public void testGeoIntersects() throws ServiceFailureException {
        filterAndCheck(service.locations(), "geo.intersects(location, geography'LINESTRING(7.5 51, 7.5 54)')", getFromList(LOCATIONS, 4, 7));
    }

    @Test
    public void testGeoLength() throws ServiceFailureException {
        filterAndCheck(service.locations(), "geo.length(location) gt 1", getFromList(LOCATIONS, 6, 7));
        filterAndCheck(service.locations(), "geo.length(location) ge 1", getFromList(LOCATIONS, 5, 6, 7));
        filterAndCheck(service.locations(), "geo.length(location) eq 1", getFromList(LOCATIONS, 5));
        filterAndCheck(service.locations(), "geo.length(location) ne 1", getFromList(LOCATIONS, 0, 1, 2, 3, 4, 6, 7));
        filterAndCheck(service.locations(), "geo.length(location) le 4", getFromList(LOCATIONS, 0, 1, 2, 3, 4, 5, 6, 7));
        filterAndCheck(service.locations(), "geo.length(location) lt 4", getFromList(LOCATIONS, 0, 1, 2, 3, 4, 5, 6));
    }

    @Test
    public void testStContains() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_contains(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', location)", getFromList(LOCATIONS, 1, 2));
    }

    @Test
    public void testStCrosses() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_crosses(geography'LINESTRING(7.5 51.5, 7.5 53.5)', location)", getFromList(LOCATIONS, 4, 7));
    }

    @Test
    public void testStDisjoint() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_disjoint(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', location)", getFromList(LOCATIONS, 0, 3, 5, 6));
    }

    @Test
    public void testStEquals() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_equals(location, geography'POINT(8 53)')", getFromList(LOCATIONS, 2));
    }

    @Test
    public void testStIntersects() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_intersects(location, geography'LINESTRING(7.5 51, 7.5 54)')", getFromList(LOCATIONS, 4, 7));
    }

    @Test
    public void testStOverlaps() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_overlaps(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', location)", getFromList(LOCATIONS, 4));
    }

    @Test
    public void testStRelate() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_relate(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', location, 'T********')", getFromList(LOCATIONS, 1, 2, 4, 7));
    }

    @Test
    public void testStTouches() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_touches(geography'POLYGON((8 53, 7.5 54.5, 8.5 54.5, 8 53))', location)", getFromList(LOCATIONS, 2, 4));
    }

    @Test
    public void testStWithin() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_within(geography'POINT(7.5 52.75)', location)", getFromList(LOCATIONS, 4));
    }

    public static <T extends Entity<T>> List<T> getFromList(List<T> list, int... ids) {
        List<T> result = new ArrayList<>();
        for (int i : ids) {
            result.add(list.get(i));
        }
        return result;
    }
}
