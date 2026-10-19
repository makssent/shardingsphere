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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.database.exception.core.exception.protocol.DatabaseProtocolException;
import org.apache.shardingsphere.infra.exception.ShardingSpherePreconditions;

import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Object handle generator for Firebird.
 *
 * <p>Firebird issues the handles of transactions, statements and BLOBs of a connection from one table, taking the lowest free slot from 1
 * and releasing it when the object is closed. Clients such as fbclient keep their objects in a table indexed by these handles,
 * so handles of different objects of one connection must not coincide.</p>
 */
@NoArgsConstructor(access = AccessLevel.NONE)
public final class FirebirdObjectHandleGenerator {
    
    private static final FirebirdObjectHandleGenerator INSTANCE = new FirebirdObjectHandleGenerator();
    
    private static final int MAX_OBJECT_HANDLE = 0xFFFE;
    
    private final Map<Integer, BitSet> connectionRegistry = new ConcurrentHashMap<>();
    
    /**
     * Get object handle generator instance.
     *
     * @return object handle generator instance
     */
    public static FirebirdObjectHandleGenerator getInstance() {
        return INSTANCE;
    }
    
    /**
     * Generate next object handle for connection.
     *
     * @param connectionId connection ID
     * @return generated object handle within the range 1 to {@code 0xFFFE}
     * @throws DatabaseProtocolException when no free object handle is available
     */
    public int nextHandle(final int connectionId) {
        BitSet handles = connectionRegistry.computeIfAbsent(connectionId, unused -> new BitSet());
        synchronized (handles) {
            int result = handles.nextClearBit(1);
            ShardingSpherePreconditions.checkState(MAX_OBJECT_HANDLE >= result, () -> new DatabaseProtocolException("No free object handles are available for connection %d.", connectionId));
            handles.set(result);
            return result;
        }
    }
    
    /**
     * Release object handle of connection.
     *
     * @param connectionId connection ID
     * @param handle object handle
     */
    public void releaseHandle(final int connectionId, final int handle) {
        BitSet handles = connectionRegistry.get(connectionId);
        if (null == handles) {
            return;
        }
        synchronized (handles) {
            handles.clear(handle);
        }
    }
    
    /**
     * Unregister connection.
     *
     * @param connectionId connection ID
     */
    public void unregisterConnection(final int connectionId) {
        connectionRegistry.remove(connectionId);
    }
}
