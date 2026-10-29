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

package org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.sql;

import io.netty.buffer.Unpooled;
import org.apache.shardingsphere.database.exception.core.exception.protocol.DatabaseProtocolException;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.common.FirebirdCommonInfoPacketType;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;

@ExtendWith(MockitoExtension.class)
class FirebirdSQLInfoReturnPacketTest {
    
    private static final int REQ_SELECT_COUNT = 13;
    
    private static final int REQ_INSERT_COUNT = 14;
    
    private static final int REQ_UPDATE_COUNT = 15;
    
    private static final int REQ_DELETE_COUNT = 16;
    
    @Mock
    private FirebirdPacketPayload payload;
    
    @Test
    void assertWriteExecutionPlan() {
        FirebirdPacketPayload actual = new FirebirdPacketPayload(Unpooled.buffer(), StandardCharsets.UTF_8);
        FirebirdSQLInfoReturnPacket packet = new FirebirdSQLInfoReturnPacket(Arrays.asList(FirebirdSQLInfoPacketType.GET_PLAN, FirebirdCommonInfoPacketType.END),
                new FirebirdSQLRecordsInfo(0L, 0L, 0L), "PLAN (FOO NATURAL)", 1024);
        packet.write(actual);
        assertThat((int) actual.getByteBuf().readUnsignedByte(), is(22));
        assertThat(actual.getByteBuf().readUnsignedShortLE(), is(19));
        byte[] plan = new byte[19];
        actual.getByteBuf().readBytes(plan);
        assertThat(new String(plan, StandardCharsets.UTF_8), is("\nPLAN (FOO NATURAL)"));
        assertThat((int) actual.getByteBuf().readUnsignedByte(), is(1));
    }
    
    @Test
    void assertWriteTruncatedExecutionPlan() {
        FirebirdPacketPayload actual = new FirebirdPacketPayload(Unpooled.buffer(), StandardCharsets.UTF_8);
        FirebirdSQLInfoReturnPacket packet = new FirebirdSQLInfoReturnPacket(Arrays.asList(FirebirdSQLInfoPacketType.GET_PLAN, FirebirdCommonInfoPacketType.END),
                new FirebirdSQLRecordsInfo(0L, 0L, 0L), "PLAN (FOO NATURAL)", 17);
        packet.write(actual);
        assertThat((int) actual.getByteBuf().readUnsignedByte(), is(22));
        assertThat(actual.getByteBuf().readUnsignedShortLE(), is(9));
        byte[] plan = new byte[9];
        actual.getByteBuf().readBytes(plan);
        assertThat(new String(plan, StandardCharsets.UTF_8), is("\nPLAN ..."));
        assertThat((int) actual.getByteBuf().readUnsignedByte(), is(2));
    }
    
    @Test
    void assertWriteEmptyExecutionPlan() {
        FirebirdPacketPayload actual = new FirebirdPacketPayload(Unpooled.buffer(), StandardCharsets.UTF_8);
        FirebirdSQLInfoReturnPacket packet = new FirebirdSQLInfoReturnPacket(Arrays.asList(FirebirdSQLInfoPacketType.GET_PLAN, FirebirdCommonInfoPacketType.END),
                new FirebirdSQLRecordsInfo(0L, 0L, 0L), "", 1024);
        packet.write(actual);
        assertThat((int) actual.getByteBuf().readUnsignedByte(), is(1));
    }
    
    @Test
    void assertWriteExecutionPlanWithEmptyBuffer() {
        FirebirdPacketPayload actual = new FirebirdPacketPayload(Unpooled.buffer(), StandardCharsets.UTF_8);
        FirebirdSQLInfoReturnPacket packet = new FirebirdSQLInfoReturnPacket(Arrays.asList(FirebirdSQLInfoPacketType.GET_PLAN, FirebirdCommonInfoPacketType.END),
                new FirebirdSQLRecordsInfo(0L, 0L, 0L), "PLAN (FOO NATURAL)", 0);
        packet.write(actual);
        assertFalse(actual.getByteBuf().isReadable());
    }
    
    @Test
    void assertWriteExecutionPlanWithoutEndItem() {
        FirebirdPacketPayload actual = new FirebirdPacketPayload(Unpooled.buffer(), StandardCharsets.UTF_8);
        FirebirdSQLInfoReturnPacket packet = new FirebirdSQLInfoReturnPacket(Collections.singletonList(FirebirdSQLInfoPacketType.GET_PLAN),
                new FirebirdSQLRecordsInfo(0L, 0L, 0L), "PLAN (FOO NATURAL)", 1024);
        packet.write(actual);
        assertThat((int) actual.getByteBuf().getUnsignedByte(actual.getByteBuf().writerIndex() - 1), is(1));
    }
    
    @Test
    void assertWriteExecutionPlanAfterEndItem() {
        FirebirdPacketPayload actual = new FirebirdPacketPayload(Unpooled.buffer(), StandardCharsets.UTF_8);
        FirebirdSQLInfoReturnPacket packet = new FirebirdSQLInfoReturnPacket(Arrays.asList(FirebirdCommonInfoPacketType.END, FirebirdSQLInfoPacketType.GET_PLAN),
                new FirebirdSQLRecordsInfo(0L, 0L, 0L), "PLAN (FOO NATURAL)", 1024);
        packet.write(actual);
        assertThat(actual.getByteBuf().readableBytes(), is(1));
        assertThat((int) actual.getByteBuf().readUnsignedByte(), is(1));
    }
    
    @Test
    void assertWriteRecordsAndPlanWithSmallBuffer() {
        FirebirdPacketPayload actual = new FirebirdPacketPayload(Unpooled.buffer(), StandardCharsets.UTF_8);
        FirebirdSQLInfoReturnPacket packet = new FirebirdSQLInfoReturnPacket(Arrays.asList(FirebirdSQLInfoPacketType.RECORDS, FirebirdSQLInfoPacketType.GET_PLAN, FirebirdCommonInfoPacketType.END),
                new FirebirdSQLRecordsInfo(0L, 0L, 0L), "PLAN (FOO NATURAL)", 5);
        packet.write(actual);
        assertThat(actual.getByteBuf().readableBytes(), is(1));
        assertThat((int) actual.getByteBuf().readUnsignedByte(), is(2));
    }
    
    @Test
    void assertWriteRecords() {
        FirebirdSQLRecordsInfo recordsInfo = new FirebirdSQLRecordsInfo(5L, 7L, 9L);
        FirebirdSQLInfoReturnPacket packet = new FirebirdSQLInfoReturnPacket(Arrays.asList(FirebirdSQLInfoPacketType.RECORDS, FirebirdCommonInfoPacketType.END), recordsInfo, "", 1024);
        packet.write(payload);
        InOrder inOrder = inOrder(payload);
        inOrder.verify(payload).writeInt1(FirebirdSQLInfoPacketType.RECORDS.getCode());
        inOrder.verify(payload).writeInt2LE(28);
        inOrder.verify(payload).writeInt1(REQ_SELECT_COUNT);
        inOrder.verify(payload).writeInt2LE(4);
        inOrder.verify(payload).writeInt4LE(0);
        inOrder.verify(payload).writeInt1(REQ_INSERT_COUNT);
        inOrder.verify(payload).writeInt2LE(4);
        inOrder.verify(payload).writeInt4LE(5);
        inOrder.verify(payload).writeInt1(REQ_UPDATE_COUNT);
        inOrder.verify(payload).writeInt2LE(4);
        inOrder.verify(payload).writeInt4LE(7);
        inOrder.verify(payload).writeInt1(REQ_DELETE_COUNT);
        inOrder.verify(payload).writeInt2LE(4);
        inOrder.verify(payload).writeInt4LE(9);
        inOrder.verify(payload).writeInt1(FirebirdCommonInfoPacketType.END.getCode());
    }
    
    @Test
    void assertParseSQLInfoWithUnknownType() {
        FirebirdSQLInfoReturnPacket packet = new FirebirdSQLInfoReturnPacket(Collections.singletonList(FirebirdSQLInfoPacketType.STMT_TYPE), new FirebirdSQLRecordsInfo(0L, 0L, 0L), "", 1024);
        assertThrows(DatabaseProtocolException.class, () -> packet.write(payload));
    }
}
