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

package org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement;

import io.netty.buffer.ByteBuf;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.firebirdsql.gds.BlrConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * Firebird BLR row metadata.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public final class FirebirdBlrRowMetadata {
    
    private final ByteBuf blr;
    
    private final int length;
    
    private final List<FirebirdBinaryColumnType> columnTypes;
    
    /**
     * Parse FirebirdBlrRowMetadata from BLR buffer.
     *
     * @param blrBuffer BLR buffer
     * @return row binary
     */
    public static FirebirdBlrRowMetadata parseBLR(final ByteBuf blrBuffer) {
        int length = blrBuffer.readableBytes();
        List<FirebirdBinaryColumnType> columnTypes = parse(blrBuffer);
        return new FirebirdBlrRowMetadata(blrBuffer, length, columnTypes);
    }
    
    private static List<FirebirdBinaryColumnType> parse(final ByteBuf blrBuffer) {
        ByteBuf buffer = blrBuffer.duplicate();
        if (!buffer.isReadable()) {
            return new ArrayList<>(0);
        }
        buffer.skipBytes(4);
        int length = buffer.readUnsignedByte();
        length += 256 * buffer.readUnsignedByte();
        List<FirebirdBinaryColumnType> result = new ArrayList<>(length / 2);
        int blrType = buffer.readUnsignedByte();
        while (blrType != BlrConstants.blr_end) {
            result.add(FirebirdBinaryColumnType.valueOfBLRType(blrType));
            buffer.skipBytes(getSkipCount(blrType) + 2);
            blrType = buffer.readUnsignedByte();
        }
        return result;
    }
    
    private static int getSkipCount(final int blrType) {
        switch (blrType) {
            case BlrConstants.blr_varying2:
            case BlrConstants.blr_text2:
            case BlrConstants.blr_blob2:
                return 4;
            case BlrConstants.blr_text:
            case BlrConstants.blr_varying:
                return 2;
            case BlrConstants.blr_quad:
            case BlrConstants.blr_long:
            case BlrConstants.blr_short:
            case BlrConstants.blr_int64:
            case BlrConstants.blr_int128:
                return 1;
            default:
                return 0;
        }
    }
}
