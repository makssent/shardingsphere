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

package org.apache.shardingsphere.governance.core.registry.listener.impl;

import org.apache.shardingsphere.governance.core.registry.listener.event.GovernanceEvent;
import org.apache.shardingsphere.governance.repository.api.GovernanceRepository;
import org.apache.shardingsphere.governance.repository.api.listener.DataChangedEvent;
import org.apache.shardingsphere.governance.repository.api.listener.DataChangedEvent.Type;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

public final class LockChangedListenerTest {
    
    @Test
    public void assertCreateEventWithInvalidPath() {
        Optional<GovernanceEvent> actual = new LockChangedListener(mock(GovernanceRepository.class)).createEvent(new DataChangedEvent("/lock/glock", "", Type.ADDED));
        assertFalse(actual.isPresent());
    }
}
