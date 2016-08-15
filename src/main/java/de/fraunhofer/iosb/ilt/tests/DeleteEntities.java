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

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;

/**
 *
 * @author Hylke van der Schaaf
 */
public class DeleteEntities {

    /**
     * @param args the command line arguments
     * @throws de.fraunhofer.iosb.ilt.sta.ServiceFailureException
     */
    public static void main(String[] args) throws ServiceFailureException, URISyntaxException {
        URI baseUri = URI.create("http://localhost:8080/SensorThingsService/v1.0/");
        SensorThingsService service = new SensorThingsService(baseUri);
        Utils.deleteAll(service);
    }

}
