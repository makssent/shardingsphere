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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.batch;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchDefaultBpbChangeException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchWithoutBlobsException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BlobFilterNotFoundException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchBlobPolicyException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchHandleException;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchBlobStreamCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchColumnDescriptor;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchMessageCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchSetBpbCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchStatement;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdParseBatchBlr;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.cache.FirebirdBlobWriteCache;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.generator.FirebirdBlobIdGenerator;
import org.firebirdsql.gds.BlrConstants;
import org.firebirdsql.jaybird.fb.constants.BatchItems;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FirebirdBatchStatementManagerTest {
    
    private static final int CONNECTION_ID = 1;
    
    private static final int STATEMENT_ID = 2;
    
    private static final int TRANSACTION_ID = 3;
    
    private final FirebirdBatchStatementManager manager = FirebirdBatchStatementManager.getInstance();
    
    @Mock
    private FirebirdBatchMessageCommandPacket packet;
    
    @Mock
    private FirebirdBatchBlobStreamCommandPacket blobStreamPacket;
    
    @Mock
    private FirebirdBatchSetBpbCommandPacket setBpbPacket;
    
    @BeforeEach
    void setUp() {
        manager.registerConnection(CONNECTION_ID);
        FirebirdBlobIdGenerator.getInstance().registerConnection(CONNECTION_ID);
        FirebirdBlobWriteCache.getInstance().registerConnection(CONNECTION_ID);
    }
    
    @AfterEach
    void tearDown() {
        manager.unregisterConnection(CONNECTION_ID);
        FirebirdBlobIdGenerator.getInstance().unregisterConnection(CONNECTION_ID);
        FirebirdBlobWriteCache.getInstance().unregisterConnection(CONNECTION_ID);
    }
    
    @Test
    void assertRegisterBatchStatement() {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 6, 8, 16L, true, true, BatchItems.BLOB_STREAM);
        FirebirdBatchStatement actual = manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertThat(actual.getStatementHandle(), is(STATEMENT_ID));
        assertThat(actual.getBufferSize(), is(16L));
        assertTrue(actual.isRecordCounts());
        assertTrue(actual.isMultiError());
        assertTrue(actual.isBlobStreamAllowed());
        assertThat(actual.getMessageLength(), is(6));
        assertThat(actual.getAlignedMessageLength(), is(8));
    }
    
    @Test
    void assertAppendBatchMessage() {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 6, 8, 16L, false, false, BatchItems.BLOB_NONE);
        List<List<Object>> expectedParameterValues = Arrays.asList(Collections.singletonList("foo_value"), Collections.singletonList("bar_value"));
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(packet.getMessageCount()).thenReturn(2L);
        when(packet.readParameterValues(Collections.emptyList())).thenReturn(expectedParameterValues);
        manager.appendBatchMessage(CONNECTION_ID, packet);
        FirebirdBatchStatement actual = manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertThat(actual.getParameterValues(), is(expectedParameterValues));
        assertThat(actual.getDataSize(), is(16L));
    }
    
    @Test
    void assertAppendBatchMessageWithLastMessageUnaligned() {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 6, 8, 14L, false, false, BatchItems.BLOB_NONE);
        manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID).addParameterValues(Collections.singletonList("foo_value"));
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(packet.getMessageCount()).thenReturn(1L);
        when(packet.readParameterValues(Collections.emptyList())).thenReturn(Collections.singletonList(Collections.singletonList("bar_value")));
        manager.appendBatchMessage(CONNECTION_ID, packet);
        assertThat(manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID).getParameterValues().size(), is(2));
    }
    
    @Test
    void assertAppendBatchMessageWhenBatchNotFound() {
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        assertThrows(InvalidBatchHandleException.class, () -> manager.appendBatchMessage(CONNECTION_ID, packet));
        verify(packet, never()).readParameterValues(anyList());
    }
    
    @Test
    void assertAppendBatchMessageWhenBufferSizeExceeded() {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 6, 8, 13L, false, false, BatchItems.BLOB_NONE);
        manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID).addParameterValues(Collections.singletonList("foo_value"));
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(packet.getMessageCount()).thenReturn(1L);
        assertThrows(BatchTooBigException.class, () -> manager.appendBatchMessage(CONNECTION_ID, packet));
        verify(packet, never()).readParameterValues(anyList());
        assertThat(manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID).getParameterValues().size(), is(1));
    }
    
    @Test
    void assertAppendBlobStream() {
        FirebirdBatchStatement batchStatement = registerBlobBatchStatement(64L, true);
        mockBlobStreamPacket(createBlobHeader(1L, 3, 0).writeBytes(new byte[]{1, 2, 3}), 20L);
        manager.appendBlobStream(CONNECTION_ID, blobStreamPacket);
        assertThat(batchStatement.getBlobStreamSize(), is(20L));
        assertThat(batchStatement.getBlobBuffer().size(), is(1));
    }
    
    @Test
    void assertAppendBlobStreamWhenBatchNotFound() {
        when(blobStreamPacket.getStatementHandle()).thenReturn(STATEMENT_ID);
        assertThrows(InvalidBatchHandleException.class, () -> manager.appendBlobStream(CONNECTION_ID, blobStreamPacket));
    }
    
    @Test
    void assertAppendBlobStreamWithoutBlobPolicy() {
        FirebirdBatchStatement batchStatement = registerBlobBatchStatement(64L, false);
        mockBlobStreamPacket(createBlobHeader(1L, 3, 0).writeBytes(new byte[]{1, 2, 3}), 20L);
        assertThrows(InvalidBatchBlobPolicyException.class, () -> manager.appendBlobStream(CONNECTION_ID, blobStreamPacket));
        assertTrue(batchStatement.getBlobBuffer().isEmpty());
        assertThat(batchStatement.getBlobStreamSize(), is(0L));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("blobPolicyArguments")
    void assertAppendBlobStreamWithoutBlobFields(final String name, final int blobPolicy) {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 8, 8, 64L, false, false, blobPolicy);
        mockBlobStreamPacket(createBlobHeader(1L, 3, 0).writeBytes(new byte[]{1, 2, 3}), 20L);
        assertThrows(BatchWithoutBlobsException.class, () -> manager.appendBlobStream(CONNECTION_ID, blobStreamPacket));
        assertTrue(manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID).getBlobBuffer().isEmpty());
    }
    
    private static Stream<Arguments> blobPolicyArguments() {
        return Stream.of(Arguments.of("stream_policy", BatchItems.BLOB_STREAM), Arguments.of("none_policy", BatchItems.BLOB_NONE));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("emptyPortionArguments")
    void assertAppendEmptyBlobStreamWithoutBlobPolicy(final String name, final long length) {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 8, 8, 64L, false, false, BatchItems.BLOB_NONE);
        mockBlobStreamPacket(Unpooled.buffer().writeZero((int) length), length);
        manager.appendBlobStream(CONNECTION_ID, blobStreamPacket);
        assertTrue(manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID).getBlobBuffer().isEmpty());
    }
    
    private static Stream<Arguments> emptyPortionArguments() {
        return Stream.of(Arguments.of("zero_length", 0L), Arguments.of("incomplete_blob_header", 8L));
    }
    
    @Test
    void assertSetDefaultBpb() {
        FirebirdBatchStatement batchStatement = registerBlobBatchStatement(64L, true);
        when(setBpbPacket.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(setBpbPacket.getBpb()).thenReturn(Unpooled.wrappedBuffer(new byte[]{1, 3, 1, 0}));
        manager.setDefaultBpb(CONNECTION_ID, setBpbPacket);
        mockBlobStreamPacket(createBlobHeader(1L, 4, 0).writeInt(2).writeBytes(new byte[]{5, 6}), 20L);
        manager.appendBlobStream(CONNECTION_ID, blobStreamPacket);
        assertThat(batchStatement.getDefaultBpb(), is(new byte[]{1, 3, 1, 0}));
        assertThat(batchStatement.readStreamBlobs().iterator().next().getData().toByteArray(), is(new byte[]{5, 6}));
    }
    
    @Test
    void assertSetDefaultBpbWhenBatchNotFound() {
        when(setBpbPacket.getStatementHandle()).thenReturn(STATEMENT_ID);
        assertThrows(InvalidBatchHandleException.class, () -> manager.setDefaultBpb(CONNECTION_ID, setBpbPacket));
    }
    
    @Test
    void assertSetDefaultBpbAfterBlobStream() {
        registerBlobBatchStatement(64L, true);
        mockBlobStreamPacket(createBlobHeader(1L, 0, 0), 16L);
        manager.appendBlobStream(CONNECTION_ID, blobStreamPacket);
        when(setBpbPacket.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(setBpbPacket.getBpb()).thenReturn(Unpooled.wrappedBuffer(new byte[]{1, 3, 1, 0}));
        FirebirdBatchStatement actual = manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertThrows(BatchDefaultBpbChangeException.class, () -> manager.setDefaultBpb(CONNECTION_ID, setBpbPacket));
        assertThat(actual.getDefaultBpb(), is(new byte[]{1, 3, 1, 1}));
    }
    
    @Test
    void assertRegisterStreamBlobs() {
        FirebirdBatchStatement batchStatement = registerBlobBatchStatement(128L, true);
        ByteBuf data = createBlobHeader(1L, 2, 0).writeBytes(new byte[]{1, 2});
        data.writeBytes(createBlobHeader(0L, 1, 0)).writeByte(3);
        data.writeBytes(createBlobHeader(2L, 1, 0)).writeByte(4);
        batchStatement.appendBlobStream(batchStatement.readBlobStream(data, 60L));
        manager.registerStreamBlobs(batchStatement, CONNECTION_ID, TRANSACTION_ID, 4);
        Map<Long, Long> expectedBlobIds = new HashMap<>(2, 1F);
        expectedBlobIds.put(1L, 1L);
        expectedBlobIds.put(2L, 2L);
        assertThat(batchStatement.getBlobIds(), is(expectedBlobIds));
        assertThat(FirebirdBlobWriteCache.getInstance().useBlobData(CONNECTION_ID, 1L, TRANSACTION_ID).orElse(null), is(new byte[]{1, 2, 3}));
        assertThat(FirebirdBlobWriteCache.getInstance().useBlobData(CONNECTION_ID, 2L, TRANSACTION_ID).orElse(null), is(new byte[]{4}));
        assertTrue(batchStatement.getBlobBuffer().isEmpty());
        assertThat(batchStatement.getBlobStreamSize(), is(0L));
    }
    
    @Test
    void assertRegisterStreamBlobWithTransliterationDefaultBpb() {
        FirebirdBatchStatement batchStatement = registerBlobBatchStatement(128L, true);
        batchStatement.setDefaultBpb(new byte[]{1, 1, 1, 1, 2, 1, 1, 4, 1, 4, 5, 1, 53, 3, 1, 1});
        byte[] data = "é".getBytes(StandardCharsets.UTF_8);
        batchStatement.appendBlobStream(batchStatement.readBlobStream(createBlobHeader(1L, data.length, 0).writeBytes(data), 20L));
        manager.registerStreamBlobs(batchStatement, CONNECTION_ID, TRANSACTION_ID, 4);
        assertThat(batchStatement.getBlobIds(), is(Collections.singletonMap(1L, 2L)));
        assertThat(FirebirdBlobWriteCache.getInstance().useBlobData(CONNECTION_ID, 2L, TRANSACTION_ID).orElse(null), is("é".getBytes(Charset.forName("windows-1252"))));
    }
    
    @Test
    void assertRegisterStreamBlobWithMissingFilterBpb() {
        FirebirdBatchStatement batchStatement = registerBlobBatchStatement(128L, true);
        batchStatement.appendBlobStream(batchStatement.readBlobStream(createBlobHeader(1L, 7, 7).writeBytes(new byte[]{1, 2, 1, 2, 3, 1, 1}), 24L));
        assertThrows(BlobFilterNotFoundException.class, () -> manager.registerStreamBlobs(batchStatement, CONNECTION_ID, TRANSACTION_ID, 4));
    }
    
    private FirebirdBatchStatement registerBlobBatchStatement(final long bufferSize, final boolean blobStreamAllowed) {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.singletonList(createBatchBlobIdColumn()), 8, 8, bufferSize, false, false,
                blobStreamAllowed ? BatchItems.BLOB_STREAM : BatchItems.BLOB_NONE);
        return manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
    }
    
    private FirebirdBatchColumnDescriptor createBatchBlobIdColumn() {
        ByteBuf blr = Unpooled.buffer().writeByte(BlrConstants.blr_version5).writeByte(BlrConstants.blr_begin).writeByte(BlrConstants.blr_message).writeByte(0).writeShortLE(2)
                .writeByte(BlrConstants.blr_blob2).writeZero(4).writeByte(BlrConstants.blr_short).writeByte(0).writeByte(BlrConstants.blr_end).writeByte(BlrConstants.blr_eoc);
        return FirebirdParseBatchBlr.parse(blr, blr.readableBytes()).getFields().get(0);
    }
    
    private void mockBlobStreamPacket(final ByteBuf data, final long length) {
        when(blobStreamPacket.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(blobStreamPacket.getData()).thenReturn(data);
        when(blobStreamPacket.getLength()).thenReturn(length);
    }
    
    private static ByteBuf createBlobHeader(final long batchBlobId, final int blobLength, final int bpbLength) {
        return Unpooled.buffer().writeLong(batchBlobId).writeInt(blobLength).writeInt(bpbLength);
    }
    
    @Test
    void assertResetBatchStatement() {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 6, 8, 8L, false, false, BatchItems.BLOB_NONE);
        FirebirdBatchStatement batchStatement = manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        batchStatement.addParameterValues(Collections.singletonList("foo_value"));
        manager.resetBatchStatement(batchStatement);
        assertThat(batchStatement.getParameterValues(), is(Collections.emptyList()));
        assertThat(batchStatement.getDataSize(), is(0L));
    }
    
    @Test
    void assertUnregisterBatchStatement() {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 6, 8, 8L, false, false, BatchItems.BLOB_NONE);
        manager.unregisterBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertNull(manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID));
    }
    
    @Test
    void assertUnregisterBatchStatementWhenBatchNotFound() {
        assertDoesNotThrow(() -> manager.unregisterBatchStatement(CONNECTION_ID, STATEMENT_ID));
    }
    
    @Test
    void assertUnregisterConnection() {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 6, 8, 8L, false, false, BatchItems.BLOB_NONE);
        manager.unregisterConnection(CONNECTION_ID);
        assertNull(manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID));
    }
}
