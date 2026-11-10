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

package org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.prepare;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.database.exception.core.exception.protocol.DatabaseProtocolException;
import org.apache.shardingsphere.database.protocol.firebird.packet.FirebirdPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.info.type.sql.FirebirdSQLInfoPacketType;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;

import java.util.Collection;
import java.util.Optional;

/**
 * Firebird return column packet.
 */
@RequiredArgsConstructor
public final class FirebirdReturnColumnPacket extends FirebirdPacket {
    
    private final Collection<FirebirdSQLInfoPacketType> requestedItems;
    
    private final int index;
    
    private final ShardingSphereTable table;
    
    private final ShardingSphereColumn column;
    
    private final String tableAlias;
    
    private final String columnAlias;
    
    private final String owner;
    
    private final Integer columnLength;
    
    private final boolean blobColumn;
    
    private final Integer blobSubType;
    
    private FirebirdBinaryColumnType wireColumnType;
    
    private Integer wireSubType;
    
    private Integer wireScale;
    
    private Integer wireLength;
    
    /**
     * Set the wire representation of a parameter whose SQL type includes precision and scale.
     *
     * @param columnType physical Firebird column type
     * @param subType Firebird numeric sub type
     * @param scale Firebird numeric scale
     */
    public void setWireNumericType(final FirebirdBinaryColumnType columnType, final int subType, final int scale) {
        wireColumnType = columnType;
        wireSubType = subType;
        wireScale = scale;
    }
    
    /**
     * Set the wire representation of a text parameter.
     *
     * @param columnType physical Firebird text type
     * @param characterSetId Firebird character set ID
     * @param length text length in bytes
     */
    public void setWireTextType(final FirebirdBinaryColumnType columnType, final int characterSetId, final int length) {
        wireColumnType = columnType;
        wireSubType = characterSetId;
        wireLength = length;
    }
    
    /**
     * Find column length.
     *
     * @return column length, or empty if the column has no known length
     */
    @HighFrequencyInvocation
    public Optional<Integer> findColumnLength() {
        return Optional.ofNullable(columnLength);
    }
    
    /**
     * Find BLOB sub type.
     *
     * @return BLOB sub type, or empty if the column is not a BLOB column with a known sub type
     */
    @HighFrequencyInvocation
    public Optional<Integer> findBlobSubType() {
        return Optional.ofNullable(blobSubType);
    }
    
    /**
     * Get column type.
     *
     * @return column type
     */
    @HighFrequencyInvocation
    public FirebirdBinaryColumnType getColumnType() {
        return blobColumn ? FirebirdBinaryColumnType.BLOB : FirebirdBinaryColumnType.valueOfJDBCType(column.getDataType());
    }
    
    @Override
    protected void write(final FirebirdPacketPayload payload) {
        FirebirdBinaryColumnType columnType = null == wireColumnType ? getColumnType() : wireColumnType;
        for (FirebirdSQLInfoPacketType requestedItem : requestedItems) {
            switch (requestedItem) {
                case SQLDA_SEQ:
                    FirebirdPrepareStatementReturnPacket.writeInt(FirebirdSQLInfoPacketType.SQLDA_SEQ, index, payload);
                    break;
                case TYPE:
                    FirebirdPrepareStatementReturnPacket.writeInt(FirebirdSQLInfoPacketType.TYPE, columnType.getValue() + 1, payload);
                    break;
                case SUB_TYPE:
                    FirebirdPrepareStatementReturnPacket.writeInt(FirebirdSQLInfoPacketType.SUB_TYPE, getWireSubType(columnType), payload);
                    break;
                case SCALE:
                    FirebirdPrepareStatementReturnPacket.writeInt(FirebirdSQLInfoPacketType.SCALE, null == wireScale ? 0 : wireScale, payload);
                    break;
                case LENGTH:
                    if (FirebirdBinaryColumnType.VARYING == columnType || FirebirdBinaryColumnType.LEGACY_VARYING == columnType || FirebirdBinaryColumnType.TEXT == columnType
                            || FirebirdBinaryColumnType.LEGACY_TEXT == columnType) {
                        FirebirdPrepareStatementReturnPacket.writeInt(FirebirdSQLInfoPacketType.LENGTH, null == wireLength ? columnLength : wireLength, payload);
                    } else {
                        FirebirdPrepareStatementReturnPacket.writeInt(FirebirdSQLInfoPacketType.LENGTH, columnType.getLength(), payload);
                    }
                    break;
                case FIELD:
                    FirebirdPrepareStatementReturnPacket.writeString(FirebirdSQLInfoPacketType.FIELD, column.getName(), payload);
                    break;
                case ALIAS:
                    FirebirdPrepareStatementReturnPacket.writeString(FirebirdSQLInfoPacketType.ALIAS, columnAlias, payload);
                    break;
                case RELATION:
                    FirebirdPrepareStatementReturnPacket.writeString(FirebirdSQLInfoPacketType.RELATION, table.getName(), payload);
                    break;
                case RELATION_ALIAS:
                    FirebirdPrepareStatementReturnPacket.writeString(FirebirdSQLInfoPacketType.RELATION_ALIAS, tableAlias, payload);
                    break;
                case OWNER:
                    FirebirdPrepareStatementReturnPacket.writeString(FirebirdSQLInfoPacketType.OWNER, owner, payload);
                    break;
                case DESCRIBE_END:
                    FirebirdPrepareStatementReturnPacket.writeCode(FirebirdSQLInfoPacketType.DESCRIBE_END, payload);
                    break;
                default:
                    throw new DatabaseProtocolException("Unknown statement info request type %d", requestedItem);
            }
        }
    }
    
    private int getWireSubType(final FirebirdBinaryColumnType columnType) {
        if (null != wireSubType) {
            return wireSubType;
        }
        return FirebirdBinaryColumnType.BLOB == columnType && null != blobSubType ? blobSubType : columnType.getSubtype();
    }
}
