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
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.geojson.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Hylke van der Schaaf
 */
public class CreateDefaultEntities {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateDefaultEntities.class);
    private final SensorThingsService service;

    /**
     * @param args the command line arguments
     * @throws ServiceFailureException when there is an error.
     * @throws java.net.URISyntaxException
     * @throws java.net.MalformedURLException
     */
    public static void main(String[] args) throws ServiceFailureException, URISyntaxException, MalformedURLException {
        LOGGER.info("Creating test entities in {}", Constants.BASE_URL);
        CreateDefaultEntities tester = new CreateDefaultEntities();
        tester.createEntities();
    }

    public CreateDefaultEntities() throws URISyntaxException, MalformedURLException {
        service = Constants.createService();
    }

    private void createEntities() throws ServiceFailureException, URISyntaxException {
        Thing thing = new Thing();
        thing.setName("thing name 1");
        thing.setDescription("thing 1");
        {
            Map<String, Object> properties = new HashMap<>();
            properties.put("reference", "firstThing");
            thing.setProperties(properties);
        }

        {
            Location location = new Location();
            location.setName("location name 1");
            location.setDescription("location 1");
            location.setLocation(new Point(-117.05, 51.05));
            location.setEncodingType("application/vnd.geo+json");
            Map<String, Object> properties = new HashMap<>();
            properties.put("reference", "firstLocation");
//            location.setProperties(properties);
            thing.getLocations().add(location);
        }

        {
            UnitOfMeasurement um1 = new UnitOfMeasurement("Lumen", "lm", "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen");
            Datastream ds = new Datastream("datastream name 1", "datastream 1", "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement", um1);
            {
                Map<String, Object> properties = new HashMap<>();
                properties.put("reference", "firstDatastream");
//                ds.setProperties(properties);
            }
            {
                ObservedProperty op = new ObservedProperty("Luminous Flux", new URI("http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/LuminousFlux"), "observedProperty 1");
                Map<String, Object> properties = new HashMap<>();
                properties.put("reference", "firstObservedProperty");
//                op.setProperties(properties);
                ds.setObservedProperty(op);
            }
            {
                Sensor s = new Sensor("sensor name 1", "sensor 1", "application/pdf", "Light flux sensor");
                Map<String, Object> properties = new HashMap<>();
                properties.put("reference", "firstSensor");
//                s.setProperties(properties);
                ds.setSensor(s);
            }
            ds.getObservations().add(new Observation(7, ZonedDateTime.parse("2019-03-07T00:00:00Z")));
            ds.getObservations().add(new Observation(8, ZonedDateTime.parse("2019-03-08T00:00:00Z")));
            ds.getObservations().add(new Observation(9, ZonedDateTime.parse("2019-03-09T00:00:00Z")));
            ds.getObservations().add(new Observation(10, ZonedDateTime.parse("2019-03-10T00:00:00Z")));
            ds.getObservations().add(new Observation(11, ZonedDateTime.parse("2019-03-11T00:00:00Z")));
            ds.getObservations().add(new Observation(12, ZonedDateTime.parse("2019-03-12T00:00:00Z")));
            thing.getDatastreams().add(ds);
        }
        {
            UnitOfMeasurement um2 = new UnitOfMeasurement("Centigrade", "C", "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen");
            Datastream ds = new Datastream("datastream name 2", "datastream 2", "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement", um2);
            {
                Map<String, Object> properties = new HashMap<>();
                properties.put("reference", "secondDatastream");
//                ds.setProperties(properties);
            }
            ds.setObservedProperty(new ObservedProperty("Temperature", new URI("http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/Tempreture"), "observedProperty 2"));
            ds.setSensor(new Sensor("sensor name 2", "sensor 2", "application/pdf", "Tempreture sensor"));
            ds.getObservations().add(new Observation("7", ZonedDateTime.parse("2019-03-07T00:00:00Z")));
            ds.getObservations().add(new Observation("8", ZonedDateTime.parse("2019-03-08T00:00:00Z")));
            ds.getObservations().add(new Observation("9", ZonedDateTime.parse("2019-03-09T00:00:00Z")));
            ds.getObservations().add(new Observation("10", ZonedDateTime.parse("2019-03-10T00:00:00Z")));
            ds.getObservations().add(new Observation("11", ZonedDateTime.parse("2019-03-11T00:00:00Z")));
            ds.getObservations().add(new Observation("12", ZonedDateTime.parse("2019-03-12T00:00:00Z")));
            thing.getDatastreams().add(ds);
        }
        {
            UnitOfMeasurement um3 = new UnitOfMeasurement("Lumen", "lm", "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen");
            UnitOfMeasurement um4 = new UnitOfMeasurement("Centigrade", "C", "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen");
            List<String> dataTypes = Arrays.asList(
                    "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                    "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement");
            List<UnitOfMeasurement> uoms = Arrays.asList(um3, um4);
            MultiDatastream mds = new MultiDatastream(
                    "datastream name 1",
                    "datastream 1",
                    dataTypes,
                    uoms);
            {
                Map<String, Object> properties = new HashMap<>();
                properties.put("reference", "firstMultiDatastream");
                mds.setProperties(properties);
            }
            {
                ObservedProperty op = new ObservedProperty("Luminous Flux 2", new URI("http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/LuminousFlux"), "observedProperty 1");
                Map<String, Object> properties = new HashMap<>();
                properties.put("reference", "thirdObservedProperty");
                op.setProperties(properties);
                mds.getObservedProperties().add(op);
            }
            {
                ObservedProperty op = new ObservedProperty("Temperature", new URI("http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/Tempreture"), "observedProperty 2");
                Map<String, Object> properties = new HashMap<>();
                properties.put("reference", "fourthObservedProperty");
                op.setProperties(properties);
                mds.getObservedProperties().add(op);
            }
            {
                Sensor s = new Sensor("sensor name 3", "sensor 3", "application/pdf", "Fancy Sensor");
                Map<String, Object> properties = new HashMap<>();
                properties.put("reference", "ThirdSensor");
//                s.setProperties(properties);
                mds.setSensor(s);
            }
            mds.getObservations().add(new Observation(Arrays.asList(7, 13), ZonedDateTime.parse("2019-03-07T00:00:00Z")));
            mds.getObservations().add(new Observation(Arrays.asList(8, 14), ZonedDateTime.parse("2019-03-08T00:00:00Z")));
            mds.getObservations().add(new Observation(Arrays.asList(9, 15), ZonedDateTime.parse("2019-03-09T00:00:00Z")));
            mds.getObservations().add(new Observation(Arrays.asList(10, 16), ZonedDateTime.parse("2019-03-10T00:00:00Z")));
            mds.getObservations().add(new Observation(Arrays.asList(11, 17), ZonedDateTime.parse("2019-03-11T00:00:00Z")));
            mds.getObservations().add(new Observation(Arrays.asList(12, 18), ZonedDateTime.parse("2019-03-12T00:00:00Z")));
            thing.getMultiDatastreams().add(mds);
        }
        service.create(thing);
    }

}
