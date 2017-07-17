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

import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import de.fraunhofer.iosb.ilt.sta.service.TokenManagerOpenIDConnect;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 *
 * @author scf
 */
public class Constants {

    public static String BASE_URL = "http://localhost:8080/SensorThingsService/v1.0/";
    public static boolean USE_OPENID_CONNECT = false;
    public static boolean USE_BASIC_AUTH = false;
    public static String TOKEN_SERVER_URL = "http://localhost:8180/auth/realms/sensorThings/protocol/openid-connect/token";
    public static String CLIENT_ID = "";
    public static String USERNAME = "";
    public static String PASSWORD = "";

    public static SensorThingsService createService() throws MalformedURLException, URISyntaxException {
        return createService(BASE_URL);
    }

    public static SensorThingsService createService(String serviceUrl) throws MalformedURLException, URISyntaxException {
        URL url = new URL(serviceUrl);
        return createService(url);
    }

    public static SensorThingsService createService(URL serviceUrl) throws MalformedURLException, URISyntaxException {
        SensorThingsService service = new SensorThingsService(serviceUrl);
        if (USE_OPENID_CONNECT) {
            service.setTokenManager(
                    new TokenManagerOpenIDConnect()
                            .setTokenServerUrl(TOKEN_SERVER_URL)
                            .setClientId(CLIENT_ID)
                            .setUserName(USERNAME)
                            .setPassword(PASSWORD)
            );
        }
        if (USE_BASIC_AUTH) {
            CredentialsProvider credsProvider = new BasicCredentialsProvider();
            URL url = new URL(BASE_URL);
            credsProvider.setCredentials(
                    new AuthScope(url.getHost(), url.getPort()),
                    new UsernamePasswordCredentials(USERNAME, PASSWORD));
            CloseableHttpClient httpclient = HttpClients.custom()
                    .setDefaultCredentialsProvider(credsProvider)
                    .build();
            service.setClient(httpclient);
        }
        return service;
    }
}
