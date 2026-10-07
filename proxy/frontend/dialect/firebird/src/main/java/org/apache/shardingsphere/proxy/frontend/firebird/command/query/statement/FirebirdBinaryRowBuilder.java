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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.database.connector.firebird.metadata.data.FirebirdNumericColumn;
import org.apache.shardingsphere.database.protocol.binary.BinaryCell;
import org.apache.shardingsphere.database.protocol.binary.BinaryRow;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.FirebirdBinaryColumnType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.execute.protocol.util.FirebirdNumericScaleUtils;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.prepare.FirebirdReturnColumnPacket;
import org.apache.shardingsphere.proxy.backend.response.data.QueryResponseCell;
import org.apache.shardingsphere.proxy.backend.response.data.QueryResponseRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Firebird binary row builder.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FirebirdBinaryRowBuilder {
    
    /**
     * Build binary row.
     *
     * @param row query response row
     * @return binary row
     */
    public static BinaryRow build(final QueryResponseRow row) {
        return build(row, Collections.emptyList());
    }
    
    /**
     * Build binary row with the description of the columns sent to the client: NUMERIC and DECIMAL columns are written in their storage type and scale.
     *
     * @param row query response row
     * @param columns described columns
     * @return binary row
     */
    public static BinaryRow build(final QueryResponseRow row, final List<FirebirdReturnColumnPacket> columns) {
        List<BinaryCell> result = new ArrayList<>(row.getCells().size());
        int index = 0;
        for (QueryResponseCell each : row.getCells()) {
            FirebirdNumericColumn numericColumn = index < columns.size() ? columns.get(index).getNumericColumn() : null;
            index++;
            if (null == numericColumn) {
                result.add(new BinaryCell(FirebirdBinaryColumnType.valueOfJDBCType(each.getJdbcType()), each.getData()));
            } else {
                FirebirdBinaryColumnType type = FirebirdBinaryColumnType.valueOfBLRType(numericColumn.getFieldType());
                result.add(new BinaryCell(type, FirebirdNumericScaleUtils.toUnscaledValue(type, each.getData(), numericColumn.getScale())));
            }
        }
        return new BinaryRow(result);
    }
}
