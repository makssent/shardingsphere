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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.database.exception.core.exception.protocol.DatabaseProtocolException;
import org.apache.shardingsphere.database.protocol.firebird.packet.FirebirdPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.FirebirdInfoPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.common.FirebirdCommonInfoPacketType;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;

import java.util.Arrays;
import java.util.List;

/**
 * SQL info return data packet for Firebird.
 */
@RequiredArgsConstructor
@Getter
public final class FirebirdSQLInfoReturnPacket extends FirebirdPacket {
    
    private static final int REQ_SELECT_COUNT = 13;
    
    private static final int REQ_INSERT_COUNT = 14;
    
    private static final int REQ_UPDATE_COUNT = 15;
    
    private static final int REQ_DELETE_COUNT = 16;
    
    private static final int COUNT_VALUE_LENGTH = 4;
    
    private static final int RECORDS_CLUSTER_LENGTH = 4 * (1 + 2 + COUNT_VALUE_LENGTH);
    
    private final List<FirebirdInfoPacketType> infoItems;
    
    private final FirebirdSQLRecordsInfo recordsInfo;
    
    private final String executionPlan;
    
    private final int maxLength;
    
    @Override
    protected void write(final FirebirdPacketPayload payload) {
        boolean planRequest = infoItems.contains(FirebirdSQLInfoPacketType.GET_PLAN);
        if (0 == maxLength && planRequest) {
            return;
        }
        int startIndex = planRequest ? payload.getByteBuf().writerIndex() : 0;
        for (FirebirdInfoPacketType type : infoItems) {
            if (type.isCommon()) {
                FirebirdCommonInfoPacketType.parseCommonInfo(payload, (FirebirdCommonInfoPacketType) type);
                if (FirebirdCommonInfoPacketType.END == type) {
                    return;
                }
            } else if (!parseSQLInfo(payload, (FirebirdSQLInfoPacketType) type, startIndex)) {
                return;
            }
        }
        payload.writeInt1(FirebirdCommonInfoPacketType.END.getCode());
    }
    
    private boolean parseSQLInfo(final FirebirdPacketPayload payload, final FirebirdSQLInfoPacketType type, final int startIndex) {
        // TODO implement other request types handle
        switch (type) {
            case RECORDS:
                if (infoItems.contains(FirebirdSQLInfoPacketType.GET_PLAN) && maxLength - (payload.getByteBuf().writerIndex() - startIndex) <= RECORDS_CLUSTER_LENGTH + 3) {
                    payload.writeInt1(FirebirdCommonInfoPacketType.TRUNCATED.getCode());
                    return false;
                }
                processRecords(payload);
                return true;
            case GET_PLAN:
                return writeExecutionPlan(payload, startIndex);
            default:
                throw new DatabaseProtocolException("Unknown database information request type %d", type.getCode());
        }
    }
    
    private void processRecords(final FirebirdPacketPayload payload) {
        payload.writeInt1(FirebirdSQLInfoPacketType.RECORDS.getCode());
        payload.writeInt2LE(RECORDS_CLUSTER_LENGTH);
        writeCount(payload, REQ_SELECT_COUNT, 0L);
        writeCount(payload, REQ_INSERT_COUNT, recordsInfo.getInsertCount());
        writeCount(payload, REQ_UPDATE_COUNT, recordsInfo.getUpdateCount());
        writeCount(payload, REQ_DELETE_COUNT, recordsInfo.getDeleteCount());
    }
    
    private void writeCount(final FirebirdPacketPayload payload, final int countType, final long count) {
        payload.writeInt1(countType);
        payload.writeInt2LE(COUNT_VALUE_LENGTH);
        payload.writeInt4LE((int) count);
    }
    
    @HighFrequencyInvocation
    private boolean writeExecutionPlan(final FirebirdPacketPayload payload, final int startIndex) {
        if (executionPlan.isEmpty()) {
            return true;
        }
        byte[] plan = new StringBuilder(executionPlan.length() + 1).append('\n').append(executionPlan).toString().getBytes(payload.getCharset());
        int available = Math.min(65535, Math.max(0, maxLength - (payload.getByteBuf().writerIndex() - startIndex) - 4));
        if (plan.length > available) {
            int length = available;
            while (length > Math.max(0, available - 4)) {
                int index = findPreviousSpace(plan, length);
                if (index < 0) {
                    break;
                }
                length = index;
            }
            if (length + 4 <= available) {
                byte[] truncated = Arrays.copyOf(plan, length + 4);
                System.arraycopy(new byte[]{' ', '.', '.', '.'}, 0, truncated, length, 4);
                payload.writeInt1(FirebirdSQLInfoPacketType.GET_PLAN.getCode());
                payload.writeInt2LE(truncated.length);
                payload.writeBytes(truncated);
            }
            payload.writeInt1(FirebirdCommonInfoPacketType.TRUNCATED.getCode());
            return false;
        }
        payload.writeInt1(FirebirdSQLInfoPacketType.GET_PLAN.getCode());
        payload.writeInt2LE(plan.length);
        payload.writeBytes(plan);
        return true;
    }
    
    private int findPreviousSpace(final byte[] plan, final int length) {
        for (int i = length - 1; i >= 0; i--) {
            if (' ' == plan[i]) {
                return i;
            }
        }
        return -1;
    }
}
