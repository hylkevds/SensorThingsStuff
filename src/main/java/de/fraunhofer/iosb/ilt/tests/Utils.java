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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.dao.BaseDao;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;

/**
 *
 * @author Hylke van der Schaaf
 */
public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class.getName());

    public static boolean resultContains(EntityList<? extends Entity> result, Entity... entities) {
        return resultContains(result, new ArrayList(Arrays.asList(entities)));
    }

    /**
     * Checks if the list contains all the given entities exactly once.
     *
     * @param result
     * @param entityList
     * @return
     */
    public static boolean resultContains(EntityList<? extends Entity> result, List<? extends Entity> entityList) {
        long count = result.getCount();
        if (count != -1 && count != entityList.size()) {
            LOGGER.info("Result count ({}) not equal to expected count ({})", count, entityList.size());
            return false;
        }
        Iterator<? extends Entity> it;
        for (it = result.fullIterator(); it.hasNext();) {
            Entity next = it.next();
            Entity inList = findEntityIn(next, entityList);
            if (!entityList.remove(inList)) {
                LOGGER.info("Entity found in result that is not expected.");
                return false;
            }
        }
        if (!entityList.isEmpty()) {
            LOGGER.info("Expected entity not found in result.");
        }
        return entityList.isEmpty();
    }

    public static Entity findEntityIn(Entity entity, List<? extends Entity> entities) {
        Long id = entity.getId();
        for (Entity inList : entities) {
            if (Objects.equals(inList.getId(), id)) {
                return inList;
            }
        }
        return null;
    }

    public static void deleteAll(SensorThingsService sts) throws ServiceFailureException {
        deleteAll(sts.things());
        deleteAll(sts.locations());
        deleteAll(sts.sensors());
        deleteAll(sts.featuresOfInterest());
        deleteAll(sts.observedProperties());
        deleteAll(sts.observations());
    }

    public static <T extends Entity> void deleteAll(BaseDao<T> doa) throws ServiceFailureException {
        boolean more = true;
        int count = 0;
        while (more) {
            EntityList<T> entities = doa.query().list();
            if (entities.getCount() == 0) {
                more = false;
            } else {
                LOGGER.info("{} to go.", entities.getCount());
            }
            for (T entity : entities) {
                doa.delete(entity);
                count++;
            }
        }
        LOGGER.info("Deleted {} using {}.", count, doa.getClass().getName());
    }

}
