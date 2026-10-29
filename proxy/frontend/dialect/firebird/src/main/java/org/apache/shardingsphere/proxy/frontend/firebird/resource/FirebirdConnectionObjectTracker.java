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

package org.apache.shardingsphere.proxy.frontend.firebird.resource;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the last object allocated on each Firebird connection.
 */
@NoArgsConstructor(access = AccessLevel.NONE)
public final class FirebirdConnectionObjectTracker {
    
    private static final FirebirdConnectionObjectTracker INSTANCE = new FirebirdConnectionObjectTracker();
    
    private final Map<Integer, ConnectionState> connections = new ConcurrentHashMap<>();
    
    /**
     * Get the connection object tracker.
     *
     * @return connection object tracker
     */
    @HighFrequencyInvocation
    public static FirebirdConnectionObjectTracker getInstance() {
        return INSTANCE;
    }
    
    /**
     * Register a connection.
     *
     * @param connectionId connection ID
     */
    public void registerConnection(final int connectionId) {
        connections.put(connectionId, new ConnectionState());
    }
    
    /**
     * Remove connection state.
     *
     * @param connectionId connection ID
     */
    void unregisterConnection(final int connectionId) {
        connections.remove(connectionId);
    }
    
    /**
     * Record the negotiated lazy mode.
     *
     * @param connectionId connection ID
     * @param lazy whether the port uses lazy mode
     */
    public void setLazy(final int connectionId, final boolean lazy) {
        connections.computeIfPresent(connectionId, (key, state) -> {
            state.lazy = lazy;
            return state;
        });
    }
    
    /**
     * Record an allocated statement.
     *
     * @param connectionId connection ID
     * @param statementId statement ID
     */
    @HighFrequencyInvocation
    public void recordStatement(final int connectionId, final int statementId) {
        connections.computeIfPresent(connectionId, (key, state) -> {
            state.statements.add(statementId);
            state.statementId = statementId;
            state.transactionId = 0;
            return state;
        });
    }
    
    /**
     * Record an allocated transaction.
     *
     * @param connectionId connection ID
     * @param transactionId transaction ID
     */
    @HighFrequencyInvocation
    public void recordTransaction(final int connectionId, final int transactionId) {
        connections.computeIfPresent(connectionId, (key, state) -> {
            state.statementId = 0;
            state.transactionId = transactionId;
            return state;
        });
    }
    
    /**
     * Record an allocated non-statement object.
     *
     * @param connectionId connection ID
     */
    @HighFrequencyInvocation
    public void recordOtherObject(final int connectionId) {
        connections.computeIfPresent(connectionId, (key, state) -> {
            state.statementId = 0;
            state.transactionId = 0;
            return state;
        });
    }
    
    /**
     * Record a dropped statement.
     *
     * @param connectionId connection ID
     * @param statementId statement ID
     */
    @HighFrequencyInvocation
    public void dropStatement(final int connectionId, final int statementId) {
        connections.computeIfPresent(connectionId, (key, state) -> {
            state.statements.remove(statementId);
            if (state.statementId == statementId) {
                state.statementId = 0;
            }
            return state;
        });
    }
    
    /**
     * Resolve a deferred statement handle.
     *
     * @param connectionId connection ID
     * @param handle incoming handle
     * @return resolved handle
     */
    @HighFrequencyInvocation
    public int resolveStatementHandle(final int connectionId, final int handle) {
        if (0xFFFF != (handle & 0xFFFF)) {
            return handle;
        }
        ConnectionState state = connections.get(connectionId);
        return null != state && state.lazy && 0 != state.statementId ? state.statementId : handle;
    }
    
    /**
     * Resolve a deferred transaction handle.
     *
     * @param connectionId connection ID
     * @param handle incoming handle
     * @return resolved handle
     */
    @HighFrequencyInvocation
    public int resolveTransactionHandle(final int connectionId, final int handle) {
        if (0xFFFF != (handle & 0xFFFF)) {
            return handle;
        }
        ConnectionState state = connections.get(connectionId);
        return null != state && state.lazy && 0 != state.transactionId ? state.transactionId : handle;
    }
    
    /**
     * Judge whether a statement handle has been allocated.
     *
     * @param connectionId connection ID
     * @param statementId statement ID
     * @return whether the statement is allocated
     */
    @HighFrequencyInvocation
    public boolean isStatementAllocated(final int connectionId, final int statementId) {
        ConnectionState state = connections.get(connectionId);
        return null != state && state.statements.contains(statementId);
    }
    
    private static final class ConnectionState {
        
        private final Set<Integer> statements = ConcurrentHashMap.newKeySet();
        
        private volatile boolean lazy;
        
        private volatile int statementId;
        
        private volatile int transactionId;
    }
}
