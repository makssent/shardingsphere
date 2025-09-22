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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AttributeMap;
import io.netty.util.DefaultAttributeMap;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.protocol.firebird.constant.protocol.FirebirdProtocolVersion;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketFactory;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.blob.FirebirdCloseBlobCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.blob.FirebirdCreateBlobCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.blob.FirebirdPutSegmentCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.execute.FirebirdExecuteStatementPacket;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.binder.context.statement.type.dml.UpdateStatementContext;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.infra.metadata.statistics.ShardingSphereStatistics;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandler;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandlerFactory;
import org.apache.shardingsphere.proxy.backend.response.header.update.UpdateResponseHeader;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.command.executor.CommandExecutor;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.FirebirdServerPreparedStatement;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement.execute.FirebirdExecuteStatementCommandExecutor;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.FirebirdBlobManager;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.FirebirdCloseBlobCommandExecutor;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.FirebirdCreateBlobCommandExecutor;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.FirebirdPutSegmentCommandExecutor;
import org.apache.shardingsphere.test.infra.framework.mock.AutoMockExtension;
import org.apache.shardingsphere.test.infra.framework.mock.StaticMockSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
@StaticMockSettings({ProxyBackendHandlerFactory.class, ProxyContext.class})
@MockitoSettings(strictness = Strictness.LENIENT)
class FirebirdBlobCommandIntegrationTest {

    private final DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "Firebird");

    private ConnectionSession connectionSession;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ContextManager contextManager;

    @Mock
    private ProxyBackendHandler proxyBackendHandler;

    @BeforeEach
    void setUp() {
        AttributeMap attributeMap = new DefaultAttributeMap(null);
        connectionSession = new ConnectionSession(databaseType, attributeMap);
        connectionSession.setConnectionId(1);
        connectionSession.setGrantee(new org.apache.shardingsphere.infra.metadata.user.Grantee("test", "localhost"));
        connectionSession.getServerPreparedStatementRegistry().addPreparedStatement(1,
                new FirebirdServerPreparedStatement("INSERT INTO t VALUES (?)", mock(UpdateStatementContext.class, Answers.RETURNS_DEEP_STUBS), new HintValueContext()));
        connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(1).getParameterTypes().add(FirebirdBinaryColumnType.BLOB);
        when(ProxyContext.getInstance().getContextManager()).thenReturn(contextManager);
        when(contextManager.getMetaDataContexts()).thenReturn(new MetaDataContexts(new ShardingSphereMetaData(), new ShardingSphereStatistics()));
    }

    @Test
    void assertBlobLifecycleAndExecution() throws SQLException {
        byte[] blobData = "blob-value".getBytes(StandardCharsets.UTF_8);
        FirebirdGenericResponsePacket createResponse = executeCreateBlob();
        int blobHandle = createResponse.getHandle();
        long blobId = createResponse.getId();
        executePutSegment(blobHandle, blobData);
        executeCloseBlob(blobHandle);
        FirebirdExecuteStatementPacket packet = mock(FirebirdExecuteStatementPacket.class, Answers.RETURNS_DEEP_STUBS);
        List<Object> parameterValues = new ArrayList<>();
        ByteBuf blobIdBuffer = Unpooled.buffer(8);
        blobIdBuffer.writeInt((int) (blobId >> 32));
        blobIdBuffer.writeInt((int) blobId);
        parameterValues.add(blobIdBuffer);
        when(packet.getStatementId()).thenReturn(1);
        when(packet.getParameterTypes()).thenReturn(Collections.singletonList(FirebirdBinaryColumnType.BLOB));
        when(packet.getParameterValues()).thenReturn(parameterValues);
        when(packet.isStoredProcedure()).thenReturn(false);
        AtomicReference<List<Object>> capturedParameters = new AtomicReference<>();
        when(ProxyBackendHandlerFactory.newInstance(eq(databaseType), any(), eq(connectionSession), eq(true))).thenAnswer(invocation -> {
            capturedParameters.set(invocation.<org.apache.shardingsphere.infra.session.query.QueryContext>getArgument(1).getParameters());
            return proxyBackendHandler;
        });
        when(proxyBackendHandler.execute()).thenReturn(new UpdateResponseHeader(new org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.UpdateStatement(databaseType)));
        when(proxyBackendHandler.next()).thenReturn(false);
        CommandExecutor executor = new FirebirdExecuteStatementCommandExecutor(packet, connectionSession);
        Collection<DatabasePacket> response = executor.execute();
        assertThat(response.iterator().next(), isA(FirebirdGenericResponsePacket.class));
        assertArrayEquals(blobData, connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(1).getLongData().get(0));
        assertArrayEquals(blobData, (byte[]) capturedParameters.get().get(0));
        assertTrue(FirebirdBlobManager.get(connectionSession).consumeBlob(blobId).isEmpty());
    }

    private FirebirdGenericResponsePacket executeCreateBlob() {
        FirebirdPacketPayload payload = createPayload(buffer -> {
            buffer.writeInt(FirebirdCommandPacketType.CREATE_BLOB2.getValue());
            new FirebirdPacketPayload(buffer, StandardCharsets.UTF_8).writeBuffer(new byte[0]);
            buffer.writeInt(1);
            buffer.writeLong(0L);
        });
        FirebirdCreateBlobCommandPacket packet = (FirebirdCreateBlobCommandPacket) FirebirdCommandPacketFactory.newInstance(FirebirdCommandPacketType.CREATE_BLOB2, payload, FirebirdProtocolVersion.PROTOCOL_VERSION13);
        FirebirdCreateBlobCommandExecutor executor = new FirebirdCreateBlobCommandExecutor(packet, connectionSession);
        return (FirebirdGenericResponsePacket) executor.execute().iterator().next();
    }

    private void executePutSegment(final int blobHandle, final byte[] blobData) {
        FirebirdPacketPayload payload = createPayload(buffer -> {
            buffer.writeInt(FirebirdCommandPacketType.PUT_SEGMENT.getValue());
            buffer.writeInt(blobHandle);
            buffer.writeInt(blobData.length);
            new FirebirdPacketPayload(buffer, StandardCharsets.UTF_8).writeBuffer(blobData);
        });
        FirebirdPutSegmentCommandPacket packet = (FirebirdPutSegmentCommandPacket) FirebirdCommandPacketFactory.newInstance(FirebirdCommandPacketType.PUT_SEGMENT, payload, FirebirdProtocolVersion.PROTOCOL_VERSION13);
        new FirebirdPutSegmentCommandExecutor(packet, connectionSession).execute();
    }

    private void executeCloseBlob(final int blobHandle) {
        FirebirdPacketPayload payload = createPayload(buffer -> {
            buffer.writeInt(FirebirdCommandPacketType.CLOSE_BLOB.getValue());
            buffer.writeInt(blobHandle);
        });
        FirebirdCloseBlobCommandPacket packet = (FirebirdCloseBlobCommandPacket) FirebirdCommandPacketFactory.newInstance(FirebirdCommandPacketType.CLOSE_BLOB, payload, FirebirdProtocolVersion.PROTOCOL_VERSION13);
        new FirebirdCloseBlobCommandExecutor(packet, connectionSession).execute();
    }

    private FirebirdPacketPayload createPayload(final java.util.function.Consumer<ByteBuf> writer) {
        ByteBuf buffer = Unpooled.buffer();
        writer.accept(buffer);
        buffer.readerIndex(0);
        return new FirebirdPacketPayload(buffer, StandardCharsets.UTF_8);
    }
}

