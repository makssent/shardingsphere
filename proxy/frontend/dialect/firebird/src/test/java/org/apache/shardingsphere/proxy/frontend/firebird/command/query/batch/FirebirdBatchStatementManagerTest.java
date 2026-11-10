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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchHandleException;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchColumnDescriptor;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchMessageCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchStatement;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.batch.FirebirdBatchInfoPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.batch.FirebirdBatchInfoReturnPacket;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
    
    private final FirebirdBatchStatementManager manager = FirebirdBatchStatementManager.getInstance();
    
    @Mock
    private FirebirdBatchMessageCommandPacket packet;
    
    @BeforeEach
    void setUp() {
        manager.registerConnection(CONNECTION_ID);
    }
    
    @AfterEach
    void tearDown() {
        manager.unregisterConnection(CONNECTION_ID);
    }
    
    @Test
    void assertRegisterBatchStatement() {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 16L, true, true, true);
        FirebirdBatchStatement actual = manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertThat(actual.getStatementHandle(), is(STATEMENT_ID));
        assertThat(actual.getBufferSize(), is(16L));
        assertTrue(actual.isRecordCounts());
        assertTrue(actual.isMultiError());
        assertTrue(actual.isBlobStreamAllowed());
    }
    
    @Test
    void assertAppendBatchMessage() {
        List<FirebirdBatchColumnDescriptor> descriptors = Collections.singletonList(new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.SHORT, 2, 0, 0));
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, descriptors, 4L, false, false, false);
        manager.appendBatchMessage(CONNECTION_ID, createMessagePacket(1, Unpooled.buffer().writeInt(0).writeInt(42)));
        FirebirdBatchStatement actual = manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertThat(actual.getParameterValues(), is(Collections.singletonList(Collections.singletonList(42))));
        assertThat(actual.getAccumulatedSize(), is(4L));
        assertThat(writeDataSize(actual), is("0b04000400000001"));
    }
    
    private FirebirdBatchMessageCommandPacket createMessagePacket(final int count, final ByteBuf data) {
        ByteBuf input = Unpooled.buffer().writeInt(0).writeInt(STATEMENT_ID).writeInt(count).writeBytes(data);
        return new FirebirdBatchMessageCommandPacket(new FirebirdPacketPayload(input, StandardCharsets.UTF_8));
    }
    
    private String writeDataSize(final FirebirdBatchStatement batchStatement) {
        ByteBuf result = Unpooled.buffer();
        DatabasePacket packet = new FirebirdBatchInfoReturnPacket(Collections.singletonList(FirebirdBatchInfoPacketType.DATA_BYTES_SIZE), batchStatement, 64);
        packet.write(new FirebirdPacketPayload(result, StandardCharsets.UTF_8));
        return ByteBufUtil.hexDump(result);
    }
    
    @Test
    void assertAppendBatchMessageWhenBatchNotFound() {
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        assertThrows(InvalidBatchHandleException.class, () -> manager.appendBatchMessage(CONNECTION_ID, packet));
        verify(packet, never()).getMessageCount();
        verify(packet, never()).readParameterValues(anyList());
    }
    
    @Test
    void assertAppendBatchMessageWhenBufferSizeExceeded() {
        List<FirebirdBatchColumnDescriptor> descriptors = Collections.singletonList(new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.SHORT, 2, 0, 0));
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, descriptors, 3L, false, false, false);
        when(packet.getStatementHandle()).thenReturn(STATEMENT_ID);
        when(packet.getMessageCount()).thenReturn(1L);
        assertThrows(BatchTooBigException.class, () -> manager.appendBatchMessage(CONNECTION_ID, packet));
        verify(packet, never()).readParameterValues(anyList());
        FirebirdBatchStatement actual = manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertThat(actual.getParameterValues(), is(Collections.emptyList()));
        assertThat(actual.getAccumulatedSize(), is(0L));
    }
    
    @Test
    void assertAppendBatchMessageWithNullValue() {
        List<FirebirdBatchColumnDescriptor> descriptors = Collections.singletonList(new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.SHORT, 2, 0, 0));
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, descriptors, 4L, false, false, false);
        manager.appendBatchMessage(CONNECTION_ID, createMessagePacket(1, Unpooled.buffer().writeByte(1).writeZero(3)));
        FirebirdBatchStatement actual = manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertThat(actual.getParameterValues().get(0), is(Collections.singletonList(null)));
        assertThat(writeDataSize(actual), is("0b04000400000001"));
    }
    
    @Test
    void assertAppendEmptyBatchMessage() {
        List<FirebirdBatchColumnDescriptor> descriptors = Collections.singletonList(new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.SHORT, 2, 0, 0));
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, descriptors, 4L, false, false, false);
        manager.appendBatchMessage(CONNECTION_ID, createMessagePacket(0, Unpooled.EMPTY_BUFFER));
        FirebirdBatchStatement actual = manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertTrue(actual.getParameterValues().isEmpty());
        assertThat(actual.getAccumulatedSize(), is(0L));
    }
    
    @Test
    void assertAppendBatchMessageWithAlignedRows() {
        List<FirebirdBatchColumnDescriptor> descriptors = Arrays.asList(new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.SHORT, 2, 0, 0),
                new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.INT64, 8, 0, 8));
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, descriptors, 42L, false, false, false);
        manager.appendBatchMessage(CONNECTION_ID, createMessagePacket(1, Unpooled.buffer().writeInt(0).writeInt(1).writeLong(2)));
        FirebirdBatchStatement actual = manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertThat(actual.getAccumulatedSize(), is(18L));
        assertThat(writeDataSize(actual), is("0b04001800000001"));
        manager.appendBatchMessage(CONNECTION_ID, createMessagePacket(1, Unpooled.buffer().writeInt(0).writeInt(3).writeLong(4)));
        assertThat(actual.getAccumulatedSize(), is(42L));
        assertThat(writeDataSize(actual), is("0b04003000000001"));
    }
    
    @Test
    void assertAppendBatchMessageWithAlignedRowsExceedingLimit() {
        List<FirebirdBatchColumnDescriptor> descriptors = Arrays.asList(new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.SHORT, 2, 0, 0),
                new FirebirdBatchColumnDescriptor(FirebirdBinaryColumnType.INT64, 8, 0, 8));
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, descriptors, 41L, false, false, false);
        ByteBuf data = Unpooled.buffer().writeInt(0).writeInt(1).writeLong(2).writeInt(0).writeInt(3).writeLong(4);
        assertThrows(BatchTooBigException.class, () -> manager.appendBatchMessage(CONNECTION_ID, createMessagePacket(2, data)));
        assertTrue(manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID).getParameterValues().isEmpty());
    }
    
    @Test
    void assertResetBatchStatement() {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 8L, false, false, false);
        FirebirdBatchStatement batchStatement = manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID);
        batchStatement.addParameterValues(Collections.singletonList("foo_value"));
        batchStatement.addSize(4L);
        manager.resetBatchStatement(batchStatement);
        assertThat(batchStatement.getParameterValues(), is(Collections.emptyList()));
        assertThat(batchStatement.getAccumulatedSize(), is(0L));
    }
    
    @Test
    void assertUnregisterBatchStatement() {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 8L, false, false, false);
        manager.unregisterBatchStatement(CONNECTION_ID, STATEMENT_ID);
        assertNull(manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID));
    }
    
    @Test
    void assertUnregisterBatchStatementWhenBatchNotFound() {
        assertDoesNotThrow(() -> manager.unregisterBatchStatement(CONNECTION_ID, STATEMENT_ID));
    }
    
    @Test
    void assertUnregisterConnection() {
        manager.registerBatchStatement(CONNECTION_ID, STATEMENT_ID, Collections.emptyList(), 8L, false, false, false);
        manager.unregisterConnection(CONNECTION_ID);
        assertNull(manager.getBatchStatement(CONNECTION_ID, STATEMENT_ID));
    }
}
