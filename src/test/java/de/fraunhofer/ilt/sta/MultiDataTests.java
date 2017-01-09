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
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.EntityType;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
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
import java.util.ArrayList;
import java.util.List;
import org.geojson.Point;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests date and time functions.
 *
 * @author Hylke van der Schaaf
 */
public class MultiDataTests {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiDataTests.class);
    private static String rootUri;
    private static SensorThingsService service;
    private static final List<Thing> THINGS = new ArrayList<>();
    private static final List<Location> LOCATIONS = new ArrayList<>();
    private static final List<Sensor> SENSORS = new ArrayList<>();
    private static final List<ObservedProperty> OBSERVED_PROPS = new ArrayList<>();
    private static final List<Datastream> DATASTREAMS = new ArrayList<>();
    private static final List<MultiDatastream> MULTIDATASTREAMS = new ArrayList<>();
    private static final List<Observation> OBSERVATIONS = new ArrayList<>();

    public MultiDataTests() {
    }

    @BeforeClass
    public static void setUp() {

        rootUri = "http://localhost:8080/SensorThingsService/v1.0";
        rootUri = rootUri.trim();
        if (rootUri.lastIndexOf('/') == rootUri.length() - 1) {
            rootUri = rootUri.substring(0, rootUri.length() - 1);
        }
        URL url;
        try {
            url = new URL(rootUri);
            service = new SensorThingsService(url);
            createEntities();
        } catch (MalformedURLException | URISyntaxException ex) {
            LOGGER.error("Failed to create service uri.", ex);
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to create entities.", ex);
        } catch (Exception ex) {
            LOGGER.error("Unknown Exception.", ex);
        }
    }

    @AfterClass
    public static void tearDown() {
        LOGGER.info("tearing down class.");
        try {
            Utils.deleteAll(service);
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to clean database.", ex);
        }
    }

    /**
     * Creates some basic non-MultiDatastream entities.
     *
     * @throws ServiceFailureException
     * @throws URISyntaxException
     */
    private static void createEntities() throws ServiceFailureException, URISyntaxException {
        Location location = new Location("Location 1.0", "Location of Thing 1.", "application/vnd.geo+json", new Point(8, 51));
        service.create(location);
        LOCATIONS.add(location);

        Thing thing = new Thing("Thing 1", "The first thing.");
        thing.getLocations().add(location.withOnlyId());
        service.create(thing);
        THINGS.add(thing);

        thing = new Thing("Thing 2", "The second thing.");
        thing.getLocations().add(location.withOnlyId());
        service.create(thing);
        THINGS.add(thing);

        Sensor sensor = new Sensor("Sensor 1", "The first sensor.", "text", "Some metadata.");
        service.create(sensor);
        SENSORS.add(sensor);

        sensor = new Sensor("Sensor 2", "The second sensor.", "text", "Some metadata.");
        service.create(sensor);
        SENSORS.add(sensor);

        ObservedProperty obsProp = new ObservedProperty("ObservedProperty 1", new URI("http://ucom.org/temperature"), "The temperature of the thing.");
        service.create(obsProp);
        OBSERVED_PROPS.add(obsProp);

        obsProp = new ObservedProperty("ObservedProperty 2", new URI("http://ucom.org/humidity"), "The humidity of the thing.");
        service.create(obsProp);
        OBSERVED_PROPS.add(obsProp);

        Datastream datastream = new Datastream("Datastream 1", "The temperature of thing 1, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
        DATASTREAMS.add(datastream);
        datastream.setThing(THINGS.get(0).withOnlyId());
        datastream.setSensor(SENSORS.get(0).withOnlyId());
        datastream.setObservedProperty(OBSERVED_PROPS.get(0).withOnlyId());
        service.create(datastream);

        datastream = new Datastream("Datastream 2", "The temperature of thing 2, sensor 2.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
        DATASTREAMS.add(datastream);
        datastream.setThing(THINGS.get(1).withOnlyId());
        datastream.setSensor(SENSORS.get(1).withOnlyId());
        datastream.setObservedProperty(OBSERVED_PROPS.get(0).withOnlyId());
        service.create(datastream);

        createObservation(DATASTREAMS.get(0).withOnlyId(), -1);
        createObservation(DATASTREAMS.get(1).withOnlyId(), 0);
    }

    private static void createObservation(Datastream ds, double... result) throws ServiceFailureException {
        Observation o = new Observation(result, ds);
        service.create(o);
        OBSERVATIONS.add(o);
    }

    private static void createObservation(MultiDatastream ds, double... result) throws ServiceFailureException {
        Observation o = new Observation(result, ds);
        service.create(o);
        OBSERVATIONS.add(o);
    }

    private void updateForException(String test, Entity entity) {
        try {
            service.update(entity);
        } catch (ServiceFailureException ex) {
            return;
        }
        Assert.fail(test + " Update did not respond with 400 Bad Request.");
    }

    private void checkResult(String test, Utils.TestResult result) {
        Assert.assertTrue(test + " " + result.message, result.testOk);
    }

    @Test
    public void testMultiDatastream() throws ServiceFailureException {
        // Create a MultiDatastream with one ObservedProperty.
        MultiDatastream md1 = new MultiDatastream();
        md1.setName("MultiDatastream 1");
        md1.setDescription("The first test MultiDatastream.");
        md1.addUnitOfMeasurement(new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));

        List<String> dataTypes1 = new ArrayList<>();
        dataTypes1.add("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement");
        md1.setMultiObservationDataTypes(dataTypes1);

        md1.setThing(THINGS.get(0).withOnlyId());
        md1.setSensor(SENSORS.get(0).withOnlyId());

        EntityList<ObservedProperty> observedProperties = new EntityList<>(EntityType.OBSERVED_PROPERTIES);
        observedProperties.add(OBSERVED_PROPS.get(0).withOnlyId());
        md1.setObservedProperties(observedProperties);

        service.create(md1);
        MULTIDATASTREAMS.add(md1);

        // Create a MultiDatastream with two different ObservedProperties.
        MultiDatastream md2 = new MultiDatastream();
        md2.setName("MultiDatastream 2");
        md2.setDescription("The second test MultiDatastream.");
        md2.addUnitOfMeasurement(new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
        md2.addUnitOfMeasurement(new UnitOfMeasurement("percent", "%", "ucum:%"));

        List<String> dataTypes2 = new ArrayList<>();
        dataTypes2.add("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement");
        dataTypes2.add("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement");
        md2.setMultiObservationDataTypes(dataTypes2);

        md2.setThing(THINGS.get(0).withOnlyId());
        md2.setSensor(SENSORS.get(0).withOnlyId());

        EntityList<ObservedProperty> observedProperties2 = new EntityList<>(EntityType.OBSERVED_PROPERTIES);
        observedProperties2.add(OBSERVED_PROPS.get(0).withOnlyId());
        observedProperties2.add(OBSERVED_PROPS.get(1).withOnlyId());
        md2.setObservedProperties(observedProperties2);

        service.create(md2);
        MULTIDATASTREAMS.add(md2);

        // Create a MultiDatastream with two different ObservedProperties, in the opposite order.
        MultiDatastream md3 = new MultiDatastream();
        md3.setName("MultiDatastream 3");
        md3.setDescription("The third test MultiDatastream.");
        md3.addUnitOfMeasurement(new UnitOfMeasurement("percent", "%", "ucum:%"));
        md3.addUnitOfMeasurement(new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));

        List<String> dataTypes3 = new ArrayList<>();
        dataTypes3.add("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement");
        dataTypes3.add("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement");
        md3.setMultiObservationDataTypes(dataTypes3);

        md3.setThing(THINGS.get(0).withOnlyId());
        md3.setSensor(SENSORS.get(0).withOnlyId());

        EntityList<ObservedProperty> observedProperties3 = new EntityList<>(EntityType.OBSERVED_PROPERTIES);
        observedProperties3.add(OBSERVED_PROPS.get(1).withOnlyId());
        observedProperties3.add(OBSERVED_PROPS.get(0).withOnlyId());
        md3.setObservedProperties(observedProperties3);

        service.create(md3);
        MULTIDATASTREAMS.add(md3);

        // Create a MultiDatastream with two of the same ObservedProperties.
        MultiDatastream md4 = new MultiDatastream();
        md4.setName("MultiDatastream 4");
        md4.setDescription("The fourth test MultiDatastream.");
        md4.addUnitOfMeasurement(new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
        md4.addUnitOfMeasurement(new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));

        List<String> dataTypes4 = new ArrayList<>();
        dataTypes4.add("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement");
        dataTypes4.add("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement");
        md4.setMultiObservationDataTypes(dataTypes4);

        md4.setThing(THINGS.get(0).withOnlyId());
        md4.setSensor(SENSORS.get(1).withOnlyId());

        EntityList<ObservedProperty> observedProperties4 = new EntityList<>(EntityType.OBSERVED_PROPERTIES);
        observedProperties4.add(OBSERVED_PROPS.get(0).withOnlyId());
        observedProperties4.add(OBSERVED_PROPS.get(0).withOnlyId());
        md4.setObservedProperties(observedProperties4);

        service.create(md4);
        MULTIDATASTREAMS.add(md4);

        createObservation(md1, 1);
        createObservation(md1, 2);
        createObservation(md1, 3);

        createObservation(md2, 4, 1);
        createObservation(md2, 5, 2);
        createObservation(md2, 6, 3);

        createObservation(md3, 7, 4);
        createObservation(md3, 8, 5);
        createObservation(md3, 9, 6);

        createObservation(md4, 10, 7);
        createObservation(md4, 11, 8);
        createObservation(md4, 12, 9);

        {
            // Check if all Datastreams and MultiDatastreams are linked to Thing 1.
            Thing fetchedThing = service.things().find(THINGS.get(0).getId());
            EntityList<Datastream> fetchedDatastreams = fetchedThing.datastreams().query().list();
            checkResult("Check Datastreams linked to Thing 1.", Utils.resultContains(fetchedDatastreams, DATASTREAMS.get(0)));
            EntityList<MultiDatastream> fetchedMultiDatastreams = fetchedThing.multiDatastreams().query().list();
            checkResult("Check MultiDatastreams linked to Thing 1.", Utils.resultContains(fetchedMultiDatastreams, new ArrayList<>(MULTIDATASTREAMS)));
        }
        {
            // Check if all Datastreams and MultiDatastreams are linked to Sensor 1.
            Sensor fetchedSensor = service.sensors().find(SENSORS.get(0).getId());
            EntityList<Datastream> fetchedDatastreams = fetchedSensor.datastreams().query().list();
            checkResult("Check Datastreams linked to Sensor 1.", Utils.resultContains(fetchedDatastreams, DATASTREAMS.get(0)));
            EntityList<MultiDatastream> fetchedMultiDatastreams = fetchedSensor.multiDatastreams().query().list();
            checkResult(
                    "Check MultiDatastreams linked to Sensor 1.",
                    Utils.resultContains(fetchedMultiDatastreams, getFromList(MULTIDATASTREAMS, 0, 1, 2)));
        }
        {
            // Check if all Datastreams and MultiDatastreams are linked to ObservedProperty 1.
            ObservedProperty fetchedObservedProp = service.observedProperties().find(OBSERVED_PROPS.get(0).getId());
            EntityList<Datastream> fetchedDatastreams = fetchedObservedProp.datastreams().query().list();
            checkResult(
                    "Check Datastreams linked to ObservedProperty 1.",
                    Utils.resultContains(fetchedDatastreams, getFromList(DATASTREAMS, 0, 1)));
            EntityList<MultiDatastream> fetchedMultiDatastreams = fetchedObservedProp.multiDatastreams().query().list();
            checkResult(
                    "Check MultiDatastreams linked to ObservedProperty 1.",
                    Utils.resultContains(fetchedMultiDatastreams, new ArrayList<>(MULTIDATASTREAMS)));
        }
        {
            // Check if MultiDatastreams 2 and 3 are linked to ObservedProperty 2.
            ObservedProperty fetchedObservedProp = service.observedProperties().find(OBSERVED_PROPS.get(1).getId());
            EntityList<Datastream> fetchedDatastreams = fetchedObservedProp.datastreams().query().list();
            checkResult(
                    "Check Datastreams linked to ObservedProperty 2.",
                    Utils.resultContains(fetchedDatastreams, new ArrayList<>()));
            EntityList<MultiDatastream> fetchedMultiDatastreams = fetchedObservedProp.multiDatastreams().query().list();
            checkResult(
                    "Check MultiDatastreams linked to ObservedProperty 2.",
                    Utils.resultContains(fetchedMultiDatastreams, getFromList(MULTIDATASTREAMS, 1, 2)));
        }
        {
            // First Observation should have a Datastream but not a MultiDatasteam.
            Observation fetchedObservation = service.observations().find(OBSERVATIONS.get(0).getId());
            Datastream fetchedDatastream = fetchedObservation.getDatastream();
            Assert.assertEquals("Observation has wrong or no Datastream", fetchedDatastream, DATASTREAMS.get(0));
            MultiDatastream fetchedMultiDatastream = fetchedObservation.getMultiDatastream();
            Assert.assertEquals("Observation should not have a MultiDatastream", fetchedMultiDatastream, null);
        }
        {
            // Second Observation should not have a Datastream but a MultiDatasteam.
            Observation fetchedObservation = service.observations().find(OBSERVATIONS.get(2).getId());
            Datastream fetchedDatastream = fetchedObservation.getDatastream();
            Assert.assertEquals("Observation should not have a Datastream", fetchedDatastream, null);
            MultiDatastream fetchedMultiDatastream = fetchedObservation.getMultiDatastream();
            Assert.assertEquals("Observation has wrong or no MultiDatastream", fetchedMultiDatastream, MULTIDATASTREAMS.get(0));
        }
        {
            // Check if the MultiDatastreams have the correct ObservedProperties in the correct order.
            checkObservedPropertiesFor(md1, OBSERVED_PROPS.get(0));
            checkObservedPropertiesFor(md2, OBSERVED_PROPS.get(0), OBSERVED_PROPS.get(1));
            checkObservedPropertiesFor(md3, OBSERVED_PROPS.get(1), OBSERVED_PROPS.get(0));
            checkObservedPropertiesFor(md4, OBSERVED_PROPS.get(0), OBSERVED_PROPS.get(0));
        }
        {
            // Try to give Observation 1 a MultiDatastream without removing the Datastream. Should give an error.
            Observation modifiedObservation = OBSERVATIONS.get(0).withOnlyId();
            modifiedObservation.setMultiDatastream(MULTIDATASTREAMS.get(0).withOnlyId());
            updateForException("Linking Observation to Datastream AND MultiDatastream.", modifiedObservation);
        }
        {
            // Try to add a MultiDatastream to an ObservedProperty. Should give an error.
            ObservedProperty modifiedObservedProp = OBSERVED_PROPS.get(1).withOnlyId();
            modifiedObservedProp.getMultiDatastreams().add(md1.withOnlyId());
            updateForException("Linking MultiDatastream to Observed property.", modifiedObservedProp);
        }
        {
            // Check if all observations are there.
            EntityList<Observation> fetchedObservations = service.observations().query().list();
            checkResult("Looking for all observations", Utils.resultContains(fetchedObservations, new ArrayList<>(OBSERVATIONS)));
        }
        {
            // Deleting ObservedProperty 2 should delete MultiDatastream 2 and 3 and their Observations.
            service.delete(OBSERVED_PROPS.get(1));
            EntityList<MultiDatastream> fetchedMultiDatastreams = service.multiDatastreams().query().list();
            checkResult(
                    "Checking if MultiDatastreams are automatically deleted.",
                    Utils.resultContains(fetchedMultiDatastreams, getFromList(MULTIDATASTREAMS, 0, 3)));
            EntityList<Observation> fetchedObservations = service.observations().query().list();
            checkResult(
                    "Checking if Observations are automatically deleted.",
                    Utils.resultContains(fetchedObservations, getFromList(OBSERVATIONS, 0, 1, 2, 3, 4, 11, 12, 13)));
        }
        {
            // Deleting Sensor 2 should delete MultiDatastream 4
            service.delete(SENSORS.get(1));
            EntityList<MultiDatastream> fetchedMultiDatastreams = service.multiDatastreams().query().list();
            checkResult(
                    "Checking if MultiDatastreams are automatically deleted.",
                    Utils.resultContains(fetchedMultiDatastreams, getFromList(MULTIDATASTREAMS, 0)));
        }
        {
            // Deleting Thing 1 should delete the last MultiDatastream.
            service.delete(THINGS.get(0));
            EntityList<MultiDatastream> fetchedMultiDatastreams = service.multiDatastreams().query().list();
            checkResult(
                    "Checking if MultiDatastreams are automatically deleted.",
                    Utils.resultContains(fetchedMultiDatastreams, new ArrayList<>()));
        }
    }

    private void checkObservedPropertiesFor(MultiDatastream md2, ObservedProperty... expectedObservedProps) throws ArrayComparisonFailure, ServiceFailureException {
        ObservedProperty[] fetchedObservedProps2 = md2.observedProperties().query().list().toArray(new ObservedProperty[0]);
        Assert.assertArrayEquals("Incorrect Observed Properties returned.", expectedObservedProps, fetchedObservedProps2);
    }

    public static <T extends Entity<T>> List<T> getFromList(List<T> list, int... ids) {
        List<T> result = new ArrayList<>();
        for (int i : ids) {
            result.add(list.get(i));
        }
        return result;
    }
}
