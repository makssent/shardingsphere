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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob;

import org.apache.shardingsphere.database.exception.firebird.exception.protocol.UnsupportedBlobFilterException;
import org.apache.shardingsphere.database.protocol.firebird.constant.FirebirdConstant;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.blob.FirebirdCreateBlobCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.cache.FirebirdBlobWriteCache;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.generator.FirebirdBlobHandleGenerator;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.generator.FirebirdBlobIdGenerator;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement.FirebirdStatementIdGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FirebirdCreateBlobCommandExecutorTest {
    
    private static final int CONNECTION_ID = 1;
    
    private static final int UTF8_CHARSET_ID = 4;
    
    private static final int WIN1251_CHARSET_ID = 52;
    
    @Mock
    private FirebirdCreateBlobCommandPacket packet;
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConnectionSession connectionSession;
    
    @BeforeEach
    void setup() {
        FirebirdStatementIdGenerator.getInstance().registerConnection(CONNECTION_ID);
        FirebirdBlobIdGenerator.getInstance().registerConnection(CONNECTION_ID);
        FirebirdBlobHandleGenerator.getInstance().registerConnection(CONNECTION_ID);
        FirebirdBlobWriteCache.getInstance().registerConnection(CONNECTION_ID);
    }
    
    @AfterEach
    void tearDown() {
        FirebirdStatementIdGenerator.getInstance().unregisterConnection(CONNECTION_ID);
        FirebirdBlobIdGenerator.getInstance().unregisterConnection(CONNECTION_ID);
        FirebirdBlobHandleGenerator.getInstance().unregisterConnection(CONNECTION_ID);
        FirebirdBlobWriteCache.getInstance().unregisterConnection(CONNECTION_ID);
    }
    
    @Test
    void assertExecute() {
        when(packet.getBlobParameterBuffer()).thenReturn(new byte[0]);
        when(connectionSession.getAttributeMap().attr(FirebirdConstant.CONNECTION_CHARSET_ID).get()).thenReturn(UTF8_CHARSET_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        FirebirdCreateBlobCommandExecutor executor = new FirebirdCreateBlobCommandExecutor(packet, connectionSession);
        Collection<DatabasePacket> actual = executor.execute();
        assertThat(actual.size(), is(1));
        DatabasePacket response = actual.iterator().next();
        assertThat(response, isA(FirebirdGenericResponsePacket.class));
        assertThat(((FirebirdGenericResponsePacket) response).getHandle(), is(1));
        assertThat(((FirebirdGenericResponsePacket) response).getId(), is(1L));
    }
    
    @Test
    void assertExecuteWithTransliterationBlobParameterBuffer() {
        when(packet.getBlobParameterBuffer()).thenReturn(new byte[]{1, 1, 1, 1, 2, 1, 1, 4, 1, (byte) WIN1251_CHARSET_ID, 5, 1, (byte) UTF8_CHARSET_ID});
        when(connectionSession.getAttributeMap().attr(FirebirdConstant.CONNECTION_CHARSET_ID).get()).thenReturn(UTF8_CHARSET_ID);
        when(connectionSession.getConnectionId()).thenReturn(CONNECTION_ID);
        new FirebirdCreateBlobCommandExecutor(packet, connectionSession).execute();
        String text = String.valueOf(new char[]{0x422, 0x435, 0x43A, 0x441, 0x442});
        FirebirdBlobWriteCache.getInstance().appendSegment(CONNECTION_ID, 1, text.getBytes(Charset.forName("windows-1251")));
        FirebirdBlobWriteCache.getInstance().closeWrite(CONNECTION_ID, 1);
        assertThat(FirebirdBlobWriteCache.getInstance().getBlobData(CONNECTION_ID, 1L).orElse(null), is(text.getBytes(StandardCharsets.UTF_8)));
    }
    
    @Test
    void assertExecuteWithUnsupportedBlobFilter() {
        when(packet.getBlobParameterBuffer()).thenReturn(new byte[]{1, 1, 1, 2, 2, 1, 1});
        when(connectionSession.getAttributeMap().attr(FirebirdConstant.CONNECTION_CHARSET_ID).get()).thenReturn(UTF8_CHARSET_ID);
        assertThrows(UnsupportedBlobFilterException.class, () -> new FirebirdCreateBlobCommandExecutor(packet, connectionSession).execute());
        assertFalse(FirebirdBlobHandleGenerator.getInstance().isAllocated(CONNECTION_ID, 1));
    }
}
