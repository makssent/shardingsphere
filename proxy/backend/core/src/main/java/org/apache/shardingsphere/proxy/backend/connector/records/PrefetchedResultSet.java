/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.proxy.backend.connector.records;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;

/** Replays a result row consumed while capturing execution counts. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PrefetchedResultSet {
    
    /**
     * Replay the first fetched row before reading the remaining rows.
     *
     * @param resultSet underlying result set positioned after the first fetch
     * @param hasFirstRow whether the first fetch returned a row
     * @return result set that replays the first fetch
     */
    public static ResultSet wrap(final ResultSet resultSet, final boolean hasFirstRow) {
        boolean[] first = {true};
        return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(), new Class<?>[]{ResultSet.class}, (proxy, method, args) -> {
            if (first[0] && "next".equals(method.getName())) {
                first[0] = false;
                return hasFirstRow;
            }
            try {
                return method.invoke(resultSet, args);
            } catch (final InvocationTargetException ex) {
                throw ex.getCause();
            }
        });
    }
}
