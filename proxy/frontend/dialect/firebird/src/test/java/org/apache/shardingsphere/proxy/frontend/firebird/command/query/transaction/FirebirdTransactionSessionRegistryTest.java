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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.transaction;

import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.infra.metadata.user.Grantee;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.test.infra.framework.extension.mock.AutoMockExtension;
import org.apache.shardingsphere.test.infra.framework.extension.mock.StaticMockSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
@StaticMockSettings(ProxyContext.class)
class FirebirdTransactionSessionRegistryTest {
    
    @Test
    void assertConcurrentTransactionsHaveIndependentSessions() throws SQLException {
        ConnectionSession parent = new ConnectionSession(mock(DatabaseType.class), null);
        parent.setConnectionId(921);
        parent.setGrantee(mock(Grantee.class));
        parent.setCurrentDatabaseName("db");
        FirebirdTransactionSessionRegistry actualRegistry = FirebirdTransactionSessionRegistry.getInstance();
        try {
            ConnectionSession actualFirst = actualRegistry.register(parent, 1);
            ConnectionSession actualSecond = actualRegistry.register(parent, 2);
            assertThat(actualFirst, sameInstance(parent));
            assertThat(actualSecond, not(sameInstance(actualFirst)));
            assertThat(actualSecond.getDatabaseConnectionManager(), not(sameInstance(actualFirst.getDatabaseConnectionManager())));
            assertThat(actualSecond.getTransactionStatus(), not(sameInstance(actualFirst.getTransactionStatus())));
            assertThat(actualSecond.getConnectionId(), is(parent.getConnectionId()));
            assertThat(actualSecond.getCurrentDatabaseName(), is("db"));
            actualFirst.setAutoCommit(false);
            assertTrue(actualSecond.isAutoCommit());
            actualRegistry.unregister(parent, 1);
            assertNull(actualRegistry.get(parent, 1));
            assertThat(actualRegistry.get(parent, 2), sameInstance(actualSecond));
            actualRegistry.unregister(parent, 2);
            assertNull(actualRegistry.get(parent, 2));
        } finally {
            actualRegistry.unregisterConnection(parent.getConnectionId());
        }
    }
    
    @Test
    void assertSlowRegistrationDoesNotBlockOtherConnections() throws Exception {
        FirebirdTransactionSessionRegistry registry = FirebirdTransactionSessionRegistry.getInstance();
        ConnectionSession first = mock(ConnectionSession.class);
        ConnectionSession second = mock(ConnectionSession.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(first.getConnectionId()).thenAnswer(invocation -> {
            entered.countDown();
            release.await(10, TimeUnit.SECONDS);
            return 922;
        });
        when(second.getConnectionId()).thenReturn(923);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<ConnectionSession> slow = executor.submit(() -> registry.register(first, 1));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            Future<ConnectionSession> independent = executor.submit(() -> registry.register(second, 1));
            assertThat(independent.get(5, TimeUnit.SECONDS), sameInstance(second));
            release.countDown();
            assertThat(slow.get(5, TimeUnit.SECONDS), sameInstance(first));
        } finally {
            release.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            registry.unregisterConnection(922);
            registry.unregisterConnection(923);
        }
    }
}
