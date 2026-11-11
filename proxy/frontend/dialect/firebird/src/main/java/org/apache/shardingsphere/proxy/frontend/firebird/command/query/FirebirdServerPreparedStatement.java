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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.prepare.FirebirdReturnColumnPacket;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.proxy.backend.session.ServerPreparedStatement;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Prepared statement for Firebird.
 */
@RequiredArgsConstructor
@Getter
public final class FirebirdServerPreparedStatement implements ServerPreparedStatement {
    
    private final String sql;
    
    private final SQLStatementContext sqlStatementContext;
    
    private final HintValueContext hintValueContext;
    
    private final List<FirebirdReturnColumnPacket> parameterColumns = new ArrayList<>();
    
    @Getter(AccessLevel.NONE)
    private final Map<String, List<Integer>> physicalParameterMarkers = new LinkedHashMap<>(16, 0.75F, true);
    
    @Setter
    private long affectedRows;
    
    /**
     * Get physical SQL parameter positions, retaining at most the configured parser cache size.
     *
     * @param sql physical SQL
     * @param maximumSize maximum number of cached statements
     * @param parser parameter position parser
     * @return parameter positions
     */
    @HighFrequencyInvocation
    public List<Integer> getPhysicalParameterMarkers(final String sql, final long maximumSize, final Function<String, List<Integer>> parser) {
        while (physicalParameterMarkers.size() > maximumSize) {
            physicalParameterMarkers.remove(physicalParameterMarkers.keySet().iterator().next());
        }
        if (0 == maximumSize) {
            return parser.apply(sql);
        }
        List<Integer> result = physicalParameterMarkers.get(sql);
        if (null == result) {
            if (physicalParameterMarkers.size() == maximumSize) {
                physicalParameterMarkers.remove(physicalParameterMarkers.keySet().iterator().next());
            }
            result = parser.apply(sql);
            physicalParameterMarkers.put(sql, result);
        }
        return result;
    }
}
