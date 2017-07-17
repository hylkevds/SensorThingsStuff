/*
 * Copyright (C) 2017 Fraunhofer Institut IOSB, Fraunhoferstr. 1, D 76131
 * Karlsruhe, Germany.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.fraunhofer.iosb.ilt.tests;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class TimedPoster implements Runnable {

    /**
     * The logger for this class.
     */
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TimedPoster.class);
    private static final long DATASTREAM_ID = 1658;
    private static final long POSTDELAY = 2 * 1000;
    private static final long MAX_COUNT = 1000;
    private SensorThingsService service;
    private boolean stopped = false;
    private long datastreamId;

    public TimedPoster() throws MalformedURLException, URISyntaxException {
        service = Constants.createService();
    }

    public TimedPoster(long datastreamId) throws MalformedURLException, URISyntaxException {
        this();
        this.datastreamId = datastreamId;
    }

    @Override
    public void run() {
        final Map<String, Object> params = new HashMap<>();
        params.put("a", 10);
        params.put("b", 20);
        params.put("c", 30);
        try {
            Datastream datastream = service.datastreams().find(datastreamId);
            int count = 0;
            while (!stopped) {
                Calendar now = Calendar.getInstance();
                Observation o = new Observation(now.get(Calendar.SECOND), datastream);
                o.setParameters(params);
                service.create(o);
                LOGGER.info("Created obs with result {}.", o.getResult());
                count++;
                if (count >= MAX_COUNT) {
                    stopped = true;
                    break;
                }
                Thread.sleep(POSTDELAY);
            }
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to fetch Datastream", ex);
        } catch (InterruptedException ex) {
            LOGGER.warn("Rude wakeup.", ex);
        }
    }

    /**
     * @param args the command line arguments
     * @throws java.net.MalformedURLException If url is wrong.
     * @throws java.net.URISyntaxException If url is wrong.
     */
    public static void main(String[] args) throws MalformedURLException, URISyntaxException {
        TimedPoster timedPoster = new TimedPoster(DATASTREAM_ID);
        timedPoster.run();
    }

}
