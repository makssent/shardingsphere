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

package org.apache.shardingsphere.database.connector.firebird.metadata.data;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Firebird numeric column.
 *
 * <p>Storage of a NUMERIC, DECIMAL, INT128 or DECFLOAT column as Firebird defines it in {@code RDB$FIELDS}: the JDBC type of the column does not tell whether
 * the value is stored as SMALLINT, INTEGER, BIGINT or INT128, nor its scale, nor the width of DECFLOAT.</p>
 */
@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
public final class FirebirdNumericColumn {
    
    private final int fieldType;
    
    private final int subType;
    
    private final int scale;
}
