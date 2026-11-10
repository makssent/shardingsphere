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

package org.apache.shardingsphere.proxy.frontend.firebird;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import lombok.SneakyThrows;
import org.apache.shardingsphere.database.connector.core.spi.DatabaseTypedSPILoader;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.database.exception.core.exception.transaction.InTransactionException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchAlreadyOpenedException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBlobBufferFormatException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBlobContinuationBpbException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBpbTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchDefaultBpbChangeException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchParametersRequiredException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSegmentExceedsBlobException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSegmentTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSmallDataException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchWithoutBlobsException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BlobFilterNotFoundException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchBlobPolicyException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchHandleException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchMessageFormatException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchParameterVersionException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBpbVersionException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidClumpletStructureException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidStatementHandleException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.ParameterConversionException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.RepeatedBatchBlobIdException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.StringTruncationException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.TransliterationFailedException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.UnknownBatchBlobIdException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.UnsupportedBlobFilterException;
import org.apache.shardingsphere.database.protocol.codec.PacketCodec;
import org.apache.shardingsphere.database.protocol.constant.CommonConstants;
import org.apache.shardingsphere.database.protocol.firebird.codec.FirebirdPacketCodecEngine;
import org.apache.shardingsphere.database.protocol.firebird.constant.FirebirdConstant;
import org.apache.shardingsphere.database.protocol.firebird.constant.protocol.FirebirdProtocolVersion;
import org.apache.shardingsphere.database.protocol.firebird.err.FirebirdErrorPacketFactory;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchColumnDescriptor;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchRegistry;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchStatement;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.FirebirdFreeStatementPacket;
import org.apache.shardingsphere.infra.session.connection.ConnectionContext;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.backend.session.transaction.TransactionStatus;
import org.apache.shardingsphere.proxy.frontend.event.WriteCompleteEvent;
import org.apache.shardingsphere.proxy.frontend.firebird.authentication.FirebirdAuthenticationEngine;
import org.apache.shardingsphere.proxy.frontend.firebird.command.FirebirdCommandExecuteEngine;
import org.apache.shardingsphere.proxy.frontend.firebird.resource.FirebirdConnectionResourceManager;
import org.apache.shardingsphere.proxy.frontend.netty.ProxyFlowControlHandler;
import org.apache.shardingsphere.proxy.frontend.spi.DatabaseProtocolFrontendEngine;
import org.apache.shardingsphere.test.infra.framework.extension.mock.AutoMockExtension;
import org.apache.shardingsphere.test.infra.framework.extension.mock.StaticMockSettings;
import org.firebirdsql.gds.BlrConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.internal.configuration.plugins.Plugins;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
@StaticMockSettings({ProxyContext.class, FirebirdConnectionResourceManager.class})
class FirebirdFrontendEngineTest {
    
    private final DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "Firebird");
    
    private final DatabaseProtocolFrontendEngine engine = DatabaseTypedSPILoader.getService(DatabaseProtocolFrontendEngine.class, databaseType);
    
    @Mock
    private ConnectionSession connectionSession;
    
    @Mock
    private FirebirdConnectionResourceManager connectionResourceManager;
    
    @Test
    void assertGetAuthenticationEngine() {
        assertThat(engine.getAuthenticationEngine(), isA(FirebirdAuthenticationEngine.class));
    }
    
    @Test
    void assertGetCommandExecuteEngine() {
        assertThat(engine.getCommandExecuteEngine(), isA(FirebirdCommandExecuteEngine.class));
    }
    
    @Test
    void assertGetCodecEngine() {
        assertThat(engine.getCodecEngine(), isA(FirebirdPacketCodecEngine.class));
    }
    
    @Test
    void assertRejectedBatchCreatePreservesBlobStreamContinuation() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        FirebirdBatchRegistry.getInstance().registerBatchStatement(1, 42, new FirebirdBatchStatement(42));
        RecordingHandler handler = new RecordingHandler(false);
        EmbeddedChannel channel = createChannel(handler);
        ByteBuf input = Unpooled.buffer();
        writeBlobStreamHeader(input, 20);
        writeBatchCreate(input);
        writeBlobContinuation(input);
        channel.writeInbound(input);
        assertThat(handler.messages.size(), is(1));
        finishCommand(channel);
        assertThat(handler.messages.size(), is(2));
        assertThat(handler.messages.get(1).getInt(0), is(FirebirdCommandPacketType.BATCH_CREATE.getValue()));
        finishCommand(channel);
        assertThat(handler.messages.size(), is(3));
        assertThat(handler.messages.get(2).readableBytes(), is(16));
        assertTrue(channel.isActive());
        releaseChannel(channel, handler);
    }
    
    @Test
    void assertBatchCreateWaitsForReleaseAndResetsBlobStream() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        FirebirdBatchRegistry.getInstance().registerBatchStatement(1, 42, new FirebirdBatchStatement(42));
        RecordingHandler handler = new RecordingHandler(false);
        EmbeddedChannel channel = createChannel(handler);
        ByteBuf input = Unpooled.buffer();
        writeBlobStreamHeader(input, 20);
        input.writeInt(FirebirdCommandPacketType.BATCH_RLS.getValue()).writeInt(42);
        writeBatchCreate(input);
        writeBlobStreamHeader(input, 0);
        channel.writeInbound(input);
        finishCommand(channel);
        assertThat(handler.messages.size(), is(2));
        FirebirdBatchRegistry.getInstance().unregisterBatchStatement(1, 42);
        finishCommand(channel);
        assertThat(handler.messages.size(), is(3));
        FirebirdBatchRegistry.getInstance().registerBatchStatement(1, 42, new FirebirdBatchStatement(42));
        finishCommand(channel);
        assertThat(handler.messages.size(), is(4));
        assertThat(handler.messages.get(3).readableBytes(), is(28));
        assertTrue(channel.isActive());
        releaseChannel(channel, handler);
    }
    
    @Test
    void assertBatchCreatePreservesFragmentOrderWhileWaiting() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        RecordingHandler handler = new RecordingHandler(false);
        EmbeddedChannel channel = createChannel(handler);
        ByteBuf input = Unpooled.buffer();
        writeBatchCreate(input);
        input.writeByte(0).writeByte(0);
        channel.writeInbound(input);
        ByteBuf rest = Unpooled.buffer().writeInt(FirebirdCommandPacketType.BATCH_EXEC.getValue()).writeInt(42).writeInt(1);
        rest.skipBytes(2);
        channel.writeInbound(rest.readRetainedSlice(1));
        channel.writeInbound(rest.readRetainedSlice(2));
        channel.writeInbound(rest.readRetainedSlice(rest.readableBytes()));
        rest.release();
        assertThat(handler.messages.size(), is(1));
        finishCommand(channel);
        assertThat(handler.messages.size(), is(2));
        assertThat(handler.messages.get(1).getInt(0), is(FirebirdCommandPacketType.BATCH_EXEC.getValue()));
        releaseChannel(channel, handler);
    }
    
    @Test
    void assertSynchronousCreateFailureResumesDecoding() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        RecordingHandler handler = new RecordingHandler(true);
        EmbeddedChannel channel = createChannel(handler);
        ByteBuf input = Unpooled.buffer();
        writeBatchCreate(input);
        input.writeInt(FirebirdCommandPacketType.BATCH_EXEC.getValue()).writeInt(42).writeInt(1);
        channel.writeInbound(input);
        channel.runPendingTasks();
        assertThat(handler.messages.size(), is(1));
        assertThat(handler.messages.get(0).getInt(0), is(FirebirdCommandPacketType.BATCH_EXEC.getValue()));
        releaseChannel(channel, handler);
    }
    
    @Test
    void assertEmptyBatchFormatFailureResumesDecoding() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        RecordingHandler handler = new RecordingHandler(true);
        EmbeddedChannel channel = createChannel(handler);
        ByteBuf input = Unpooled.buffer().writeInt(FirebirdCommandPacketType.BATCH_CREATE.getValue()).writeInt(42).writeInt(0).writeInt(0).writeInt(0);
        input.writeInt(FirebirdCommandPacketType.BATCH_EXEC.getValue()).writeInt(42).writeInt(1);
        channel.writeInbound(input);
        channel.runPendingTasks();
        assertThat(handler.messages.size(), is(1));
        assertThat(handler.messages.get(0).getInt(0), is(FirebirdCommandPacketType.BATCH_EXEC.getValue()));
        assertTrue(channel.isActive());
        releaseChannel(channel, handler);
        assertThat(input.refCnt(), is(0));
    }
    
    @Test
    void assertSynchronousCreateAfterPreviousCommandResumesDecoding() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        RecordingHandler handler = new RecordingHandler(true);
        EmbeddedChannel channel = createChannel(handler);
        ByteBuf input = Unpooled.buffer().writeInt(FirebirdCommandPacketType.BATCH_EXEC.getValue()).writeInt(42).writeInt(1);
        writeBatchCreate(input);
        input.writeInt(FirebirdCommandPacketType.BATCH_EXEC.getValue()).writeInt(42).writeInt(1);
        channel.writeInbound(input);
        assertThat(handler.messages.size(), is(1));
        finishCommand(channel);
        assertThat(handler.messages.size(), is(2));
        assertThat(handler.messages.get(1).getInt(0), is(FirebirdCommandPacketType.BATCH_EXEC.getValue()));
        releaseChannel(channel, handler);
    }
    
    @Test
    void assertRejectedBatchCreateKeepsExistingMessageFormat() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        FirebirdBatchRegistry.getInstance().registerBatchStatement(1, 42, new FirebirdBatchStatement(42,
                Collections.singletonList(new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.BLOB, Long.BYTES, 0, 0)), 1024));
        RecordingHandler handler = new RecordingHandler(false);
        EmbeddedChannel channel = createChannel(handler);
        ByteBuf input = Unpooled.buffer();
        writeBatchCreate(input);
        input.writeInt(FirebirdCommandPacketType.BATCH_MSG.getValue()).writeInt(42).writeInt(1).writeZero(4).writeLong(123L);
        channel.writeInbound(input);
        finishCommand(channel);
        assertThat(handler.messages.size(), is(2));
        assertThat(handler.messages.get(1).readableBytes(), is(24));
        assertTrue(channel.isActive());
        releaseChannel(channel, handler);
    }
    
    @Test
    void assertFailedBatchCreateKeepsMessageFormatWithoutExistingBatch() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        RecordingHandler handler = new RecordingHandler(false);
        EmbeddedChannel channel = createChannel(handler);
        ByteBuf input = Unpooled.buffer();
        writeBatchCreate(input);
        input.writeInt(FirebirdCommandPacketType.BATCH_MSG.getValue()).writeInt(42).writeInt(1).writeInt(17).writeInt(0);
        channel.writeInbound(input);
        finishCommand(channel);
        assertThat(handler.messages.size(), is(2));
        assertThat(handler.messages.get(1).readableBytes(), is(20));
        finishCommand(channel);
        ByteBuf later = Unpooled.buffer().writeInt(FirebirdCommandPacketType.BATCH_MSG.getValue()).writeInt(42).writeInt(1).writeInt(18).writeInt(0);
        channel.writeInbound(later);
        assertThat(handler.messages.size(), is(3));
        assertTrue(channel.isActive());
        releaseChannel(channel, handler);
    }
    
    @Test
    void assertCloseDiscardsBytesWaitingForBatchCreate() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        RecordingHandler handler = new RecordingHandler(false);
        EmbeddedChannel channel = createChannel(handler);
        ByteBuf input = Unpooled.buffer();
        writeBatchCreate(input);
        input.writeInt(FirebirdCommandPacketType.BATCH_EXEC.getValue()).writeInt(42).writeInt(1);
        channel.writeInbound(input);
        channel.close();
        handler.messages.forEach(ByteBuf::release);
        assertThat(input.refCnt(), is(0));
        channel.finishAndReleaseAll();
        FirebirdBatchRegistry.getInstance().unregisterConnection(1);
    }
    
    @Test
    void assertFailedBatchCreateAfterReleasePreservesBlobStreamContinuation() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        FirebirdBatchRegistry.getInstance().registerBatchStatement(1, 42, new FirebirdBatchStatement(42));
        FirebirdFrontendEngine actual = new FirebirdFrontendEngine();
        RecordingHandler handler = new RecordingHandler(false);
        EmbeddedChannel channel = createChannel(actual, handler);
        ByteBuf input = Unpooled.buffer();
        writeBlobStreamHeader(input, 20);
        input.writeInt(FirebirdCommandPacketType.BATCH_RLS.getValue()).writeInt(42);
        input.writeInt(FirebirdCommandPacketType.BATCH_CREATE.getValue()).writeInt(42).writeInt(0).writeInt(0).writeInt(0);
        input.writeInt(FirebirdCommandPacketType.BATCH_BLOB_STREAM.getValue()).writeInt(42).writeInt(4).writeInt(0x41424344);
        input.writeInt(FirebirdCommandPacketType.BATCH_EXEC.getValue()).writeInt(42).writeInt(1);
        channel.writeInbound(input);
        finishCommand(channel);
        FirebirdBatchRegistry.getInstance().unregisterBatchStatement(1, 42);
        finishCommand(channel);
        finishCommand(channel);
        assertThat(handler.messages.get(3).readableBytes(), is(16));
        finishCommand(channel);
        assertThat(handler.messages.get(4).getInt(0), is(FirebirdCommandPacketType.BATCH_EXEC.getValue()));
        assertTrue(channel.isActive());
        assertThat(getCodecState(actual, "batchBlobStreams").size(), is(1));
        channel.writeInbound(Unpooled.buffer().writeInt(FirebirdCommandPacketType.FREE_STATEMENT.getValue()).writeInt(42).writeInt(FirebirdFreeStatementPacket.DROP));
        assertTrue(getCodecState(actual, "batchBlobStreams").isEmpty());
        releaseChannel(channel, handler);
    }
    
    @Test
    void assertFreeStatementDiscardsDeferredFormat() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        FirebirdFrontendEngine actual = new FirebirdFrontendEngine();
        RecordingHandler handler = new RecordingHandler(true);
        EmbeddedChannel channel = createChannel(actual, handler);
        ByteBuf input = Unpooled.buffer();
        writeBatchCreate(input);
        channel.writeInbound(input);
        channel.runPendingTasks();
        assertThat(getCodecState(actual, "deferredBatchFormats").size(), is(1));
        channel.writeInbound(Unpooled.buffer().writeInt(FirebirdCommandPacketType.FREE_STATEMENT.getValue()).writeInt(42).writeInt(FirebirdFreeStatementPacket.DROP));
        assertTrue(getCodecState(actual, "deferredBatchFormats").isEmpty());
        releaseChannel(channel, handler);
    }
    
    @Test
    void assertInvalidHandlesDoNotRetainBatchState() {
        FirebirdBatchRegistry.getInstance().registerConnection(1);
        FirebirdFrontendEngine actual = new FirebirdFrontendEngine();
        RecordingHandler handler = new RecordingHandler(true, true);
        EmbeddedChannel channel = createChannel(actual, handler);
        for (int handle = 1; handle <= 32; handle++) {
            ByteBuf input = Unpooled.buffer();
            writeBatchCreate(input, handle);
            channel.writeInbound(input);
            channel.runPendingTasks();
        }
        assertTrue(getCodecState(actual, "deferredBatchFormats").isEmpty());
        channel.writeInbound(Unpooled.buffer().writeInt(FirebirdCommandPacketType.BATCH_BLOB_STREAM.getValue()).writeInt(999).writeInt(0));
        assertTrue(getCodecState(actual, "batchBlobStreams").isEmpty());
        releaseChannel(channel, handler);
    }
    
    @SneakyThrows(ReflectiveOperationException.class)
    private Map<?, ?> getCodecState(final FirebirdFrontendEngine frontendEngine, final String fieldName) {
        return (Map<?, ?>) Plugins.getMemberAccessor().get(FirebirdPacketCodecEngine.class.getDeclaredField(fieldName), frontendEngine.getCodecEngine());
    }
    
    private EmbeddedChannel createChannel(final RecordingHandler handler) {
        return createChannel(new FirebirdFrontendEngine(), handler);
    }
    
    private EmbeddedChannel createChannel(final FirebirdFrontendEngine actual, final RecordingHandler handler) {
        EmbeddedChannel result = new EmbeddedChannel();
        result.pipeline().addLast(new PacketCodec(actual.getCodecEngine()));
        result.pipeline().addLast(ProxyFlowControlHandler.class.getSimpleName(), new ProxyFlowControlHandler());
        result.pipeline().addLast(handler);
        actual.initChannel(result);
        result.attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).set(StandardCharsets.UTF_8);
        result.attr(FirebirdConstant.CURRENT_CONNECTION).set(1);
        result.attr(FirebirdConstant.CONNECTION_PROTOCOL_VERSION).set(FirebirdProtocolVersion.PROTOCOL_VERSION10);
        return result;
    }
    
    private void finishCommand(final EmbeddedChannel channel) {
        channel.pipeline().fireUserEventTriggered(new WriteCompleteEvent());
        channel.runPendingTasks();
    }
    
    private void releaseChannel(final EmbeddedChannel channel, final RecordingHandler handler) {
        channel.finishAndReleaseAll();
        handler.messages.forEach(ByteBuf::release);
        FirebirdBatchRegistry.getInstance().unregisterConnection(1);
    }
    
    private void writeBatchCreate(final ByteBuf buffer) {
        writeBatchCreate(buffer, 42);
    }
    
    private void writeBatchCreate(final ByteBuf buffer, final int statementHandle) {
        ByteBuf blr = Unpooled.buffer().writeByte(BlrConstants.blr_version5).writeByte(BlrConstants.blr_begin).writeByte(BlrConstants.blr_message).writeByte(0)
                .writeShortLE(2).writeByte(BlrConstants.blr_long).writeByte(0).writeByte(BlrConstants.blr_short).writeByte(0)
                .writeByte(BlrConstants.blr_end).writeByte(BlrConstants.blr_eoc);
        int length = blr.readableBytes();
        buffer.writeInt(FirebirdCommandPacketType.BATCH_CREATE.getValue()).writeInt(statementHandle).writeInt(length).writeBytes(blr).writeZero((4 - length % 4) % 4).writeInt(6).writeInt(0);
        blr.release();
    }
    
    private void writeBlobStreamHeader(final ByteBuf buffer, final int blobLength) {
        buffer.writeInt(FirebirdCommandPacketType.BATCH_BLOB_STREAM.getValue()).writeInt(42).writeInt(16 + Math.min(blobLength, 16))
                .writeLong(1L).writeInt(blobLength).writeInt(0).writeZero(Math.min(blobLength, 16));
    }
    
    private void writeBlobContinuation(final ByteBuf buffer) {
        buffer.writeInt(FirebirdCommandPacketType.BATCH_BLOB_STREAM.getValue()).writeInt(42).writeInt(4).writeZero(4);
    }
    
    @Test
    void assertRelease() {
        int connectionId = 1;
        when(connectionSession.getConnectionId()).thenReturn(connectionId);
        when(FirebirdConnectionResourceManager.getInstance()).thenReturn(connectionResourceManager);
        engine.release(connectionSession);
        verify(connectionResourceManager).unregisterConnection(connectionId);
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("provideHandleExceptionParameters")
    void assertHandleException(final String testName, final boolean exceptionOccur, final Exception exception) {
        ConnectionContext connectionContext = mockConnectionSession(true, exceptionOccur);
        engine.handleException(connectionSession, exception);
        assertTrue(connectionContext.getTransactionContext().isExceptionOccur());
    }
    
    private ConnectionContext mockConnectionSession(final boolean inTransaction, final boolean exceptionOccur) {
        TransactionStatus transactionStatus = new TransactionStatus();
        transactionStatus.setInTransaction(inTransaction);
        ConnectionContext result = new ConnectionContext(Collections::emptyList);
        result.getTransactionContext().setExceptionOccur(exceptionOccur);
        when(connectionSession.getTransactionStatus()).thenReturn(transactionStatus);
        if (inTransaction) {
            when(connectionSession.getConnectionContext()).thenReturn(result);
        }
        return result;
    }
    
    private static Stream<Arguments> provideHandleExceptionParameters() {
        return Stream.of(Arguments.of("mark when in transaction", false, new Exception("error")), Arguments.of("keep marked when already occurred", true, new Exception("error")),
                Arguments.of("mark SQL execution failure", false, new SQLException("error")));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("provideIgnoredExceptionParameters")
    void assertHandleIgnoredException(final String testName, final boolean inTransaction, final Exception exception) {
        ConnectionContext connectionContext = mockConnectionSession(inTransaction, false);
        engine.handleException(connectionSession, exception);
        assertFalse(connectionContext.getTransactionContext().isExceptionOccur());
    }
    
    private static Stream<Arguments> provideIgnoredExceptionParameters() {
        return Stream.of(
                Arguments.of("skip when in transaction exception", true, new InTransactionException()),
                Arguments.of("skip when batch already opened", true, new BatchAlreadyOpenedException(1)),
                Arguments.of("skip when batch BLOB buffer is invalid", true, new BatchBlobBufferFormatException(1)),
                Arguments.of("skip when batch blob continuation bpb exception", true, new BatchBlobContinuationBpbException(4L)),
                Arguments.of("skip when batch bpb too big exception", true, new BatchBpbTooBigException(8L, 4L)),
                Arguments.of("skip when default bpb cannot change", true, new BatchDefaultBpbChangeException(1)),
                Arguments.of("skip when batch parameters are missing", true, new BatchParametersRequiredException(1)),
                Arguments.of("skip when batch segment exceeds blob exception", true, new BatchSegmentExceedsBlobException(50, 10)),
                Arguments.of("skip when batch segment too big exception", true, new BatchSegmentTooBigException(50, 10)),
                Arguments.of("skip when batch small data exception", true, new BatchSmallDataException("BLOB")),
                Arguments.of("skip when batch buffer exceeds limit", true, new BatchTooBigException(1, 20L, 20L, 39L)),
                Arguments.of("skip when batch has no BLOB fields", true, new BatchWithoutBlobsException(1)),
                Arguments.of("skip when blob filter not found exception", true, new BlobFilterNotFoundException(0, 2)),
                Arguments.of("skip when batch BLOB policy is invalid", true, new InvalidBatchBlobPolicyException("addBlobStream")),
                Arguments.of("skip when batch handle is invalid", true, new InvalidBatchHandleException(1)),
                Arguments.of("skip when batch message format is invalid", true, new InvalidBatchMessageFormatException("invalid length")),
                Arguments.of("skip when batch parameters have an invalid version", true, new InvalidBatchParameterVersionException(2, 1)),
                Arguments.of("skip when invalid bpb version exception", true, new InvalidBpbVersionException(2, 1)),
                Arguments.of("skip when BLOB parameter buffer is invalid", true, new InvalidClumpletStructureException("invalid", 1)),
                Arguments.of("skip when parameter conversion exception", true, new ParameterConversionException(new StringTruncationException(20, 32))),
                Arguments.of("skip when batch BLOB ID is repeated", true, new RepeatedBatchBlobIdException(1L)),
                Arguments.of("skip when transliteration failed exception", true, new TransliterationFailedException()),
                Arguments.of("skip when unknown batch blob id exception", true, new UnknownBatchBlobIdException(1L)),
                Arguments.of("skip when blob filter is unsupported", true, new UnsupportedBlobFilterException()),
                Arguments.of("skip when not in transaction", false, new Exception("error")));
    }
    
    private static final class RecordingHandler extends ChannelInboundHandlerAdapter {
        
        private final List<ByteBuf> messages = new ArrayList<>();
        
        private final boolean synchronousCreate;
        
        private final boolean invalidStatement;
        
        RecordingHandler(final boolean synchronousCreate) {
            this(synchronousCreate, false);
        }
        
        RecordingHandler(final boolean synchronousCreate, final boolean invalidStatement) {
            this.synchronousCreate = synchronousCreate;
            this.invalidStatement = invalidStatement;
        }
        
        @Override
        public void channelRead(final ChannelHandlerContext context, final Object message) {
            ByteBuf packet = (ByteBuf) message;
            if (synchronousCreate && FirebirdCommandPacketType.BATCH_CREATE.getValue() == packet.getInt(packet.readerIndex())) {
                packet.release();
                context.writeAndFlush(invalidStatement ? FirebirdErrorPacketFactory.newInstance(new InvalidStatementHandleException(42)) : Unpooled.EMPTY_BUFFER);
                return;
            }
            context.channel().config().setAutoRead(false);
            messages.add(packet);
        }
    }
}
