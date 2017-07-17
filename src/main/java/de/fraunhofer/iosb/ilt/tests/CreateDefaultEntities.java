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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.HashMap;
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
        Map<String, Object> properties = new HashMap<>();
        properties.put("reference", "first");
        thing.setProperties(properties);

        Location location = new Location();
        location.setName("location name 1");
        location.setDescription("location 1");
        location.setLocation(new Point(-117.05, 51.05));
        location.setEncodingType("application/vnd.geo+json");
        thing.getLocations().add(location);

        {
            UnitOfMeasurement um1 = new UnitOfMeasurement("Lumen", "lm", "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen");
            Datastream ds1 = new Datastream("datastream name 1", "datastream 1", "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement", um1);
            ds1.setObservedProperty(new ObservedProperty("Luminous Flux", new URI("http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/LuminousFlux"), "observedProperty 1"));
            ds1.setSensor(new Sensor("sensor name 1", "sensor 1", "application/pdf", "Light flux sensor"));
            ds1.getObservations().add(new Observation(7, ZonedDateTime.parse("2019-03-07T00:00:00Z")));
            ds1.getObservations().add(new Observation(8, ZonedDateTime.parse("2019-03-08T00:00:00Z")));
            ds1.getObservations().add(new Observation(9, ZonedDateTime.parse("2019-03-09T00:00:00Z")));
            ds1.getObservations().add(new Observation(10, ZonedDateTime.parse("2019-03-10T00:00:00Z")));
            ds1.getObservations().add(new Observation(11, ZonedDateTime.parse("2019-03-11T00:00:00Z")));
            ds1.getObservations().add(new Observation(12, ZonedDateTime.parse("2019-03-12T00:00:00Z")));
            thing.getDatastreams().add(ds1);
        }
        {
            UnitOfMeasurement um2 = new UnitOfMeasurement("Centigrade", "C", "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen");
            Datastream ds2 = new Datastream("datastream name 2", "datastream 2", "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement", um2);
            ds2.setObservedProperty(new ObservedProperty("Tempretaure", new URI("http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html/Tempreture"), "observedProperty 2"));
            ds2.setSensor(new Sensor("sensor name 2", "sensor 2", "application/pdf", "Tempreture sensor"));
            ds2.getObservations().add(new Observation("7", ZonedDateTime.parse("2019-03-07T00:00:00Z")));
            ds2.getObservations().add(new Observation("8", ZonedDateTime.parse("2019-03-08T00:00:00Z")));
            ds2.getObservations().add(new Observation("9", ZonedDateTime.parse("2019-03-09T00:00:00Z")));
            ds2.getObservations().add(new Observation("10", ZonedDateTime.parse("2019-03-10T00:00:00Z")));
            ds2.getObservations().add(new Observation("11", ZonedDateTime.parse("2019-03-11T00:00:00Z")));
            ds2.getObservations().add(new Observation("12", ZonedDateTime.parse("2019-03-12T00:00:00Z")));
            thing.getDatastreams().add(ds2);
        }
        service.create(thing);
    }

}
