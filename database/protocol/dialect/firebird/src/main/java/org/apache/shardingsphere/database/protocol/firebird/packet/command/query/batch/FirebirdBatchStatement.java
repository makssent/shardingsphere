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

package org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Firebird batch statement metadata.
 */
@RequiredArgsConstructor
@Getter
public final class FirebirdBatchStatement {
    
    private final int statementHandle;
    
    private final List<FirebirdBatchColumnDescriptor> columnDescriptors;
    
    private final int messageLength;
    
    private final int alignedMessageLength;
    
    private final long bufferSize;
    
    private final boolean recordCounts;
    
    private final boolean multiError;
    
    private final List<List<Object>> parameterValues = new ArrayList<>();
    
    public FirebirdBatchStatement(final int statementHandle) {
        this(statementHandle, Collections.emptyList(), 0, 0, 0L, false, false);
    }
    
    public FirebirdBatchStatement(final int statementHandle, final List<FirebirdBatchColumnDescriptor> columnDescriptors, final long bufferSize) {
        this(statementHandle, columnDescriptors, 0, 0, bufferSize, false, false);
    }
    
    /**
     * Add batched parameter values.
     * @param values parameter values to add
     */
    public void addParameterValues(final List<Object> values) {
        parameterValues.add(values);
    }
    
    /**
     * Get size of the batch messages in the batch buffer: every stored message takes the aligned message length.
     *
     * @return size in bytes
     */
    public long getDataSize() {
        return (long) parameterValues.size() * alignedMessageLength;
    }
    
    /**
     * Reset accumulated batch state.
     */
    public void reset() {
        parameterValues.clear();
    }
}
