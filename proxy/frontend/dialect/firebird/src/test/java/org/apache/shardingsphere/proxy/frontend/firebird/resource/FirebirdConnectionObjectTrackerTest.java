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

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FirebirdConnectionObjectTrackerTest {
    
    @Test
    void assertResolveLastStatementForLazyConnections() {
        FirebirdConnectionObjectTracker tracker = FirebirdConnectionObjectTracker.getInstance();
        tracker.registerConnection(481);
        tracker.registerConnection(482);
        try {
            tracker.recordStatement(481, 7);
            tracker.recordStatement(482, 8);
            assertTrue(tracker.isStatementAllocated(481, 7));
            assertFalse(tracker.isStatementAllocated(482, 7));
            assertThat(tracker.resolveStatementHandle(481, 0xFFFF), is(0xFFFF));
            tracker.setLazy(481, true);
            assertThat(tracker.resolveStatementHandle(481, 0xFFFF), is(7));
            assertThat(tracker.resolveStatementHandle(481, -1), is(7));
            assertThat(tracker.resolveStatementHandle(481, 3), is(3));
            assertThat(tracker.resolveStatementHandle(482, 0xFFFF), is(0xFFFF));
            tracker.recordOtherObject(481);
            assertThat(tracker.resolveStatementHandle(481, 0xFFFF), is(0xFFFF));
            tracker.recordStatement(481, 9);
            tracker.dropStatement(481, 7);
            assertFalse(tracker.isStatementAllocated(481, 7));
            assertThat(tracker.resolveStatementHandle(481, 0xFFFF), is(9));
            tracker.dropStatement(481, 9);
            assertThat(tracker.resolveStatementHandle(481, 0xFFFF), is(0xFFFF));
        } finally {
            FirebirdConnectionResourceManager.getInstance().unregisterConnection(481);
            FirebirdConnectionResourceManager.getInstance().unregisterConnection(482);
        }
        assertThat(tracker.resolveStatementHandle(481, 0xFFFF), is(0xFFFF));
    }
}
