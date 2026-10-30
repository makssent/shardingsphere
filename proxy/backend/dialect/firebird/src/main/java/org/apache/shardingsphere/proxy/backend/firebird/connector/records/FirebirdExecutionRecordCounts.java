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

package org.apache.shardingsphere.proxy.backend.firebird.connector.records;

import org.apache.shardingsphere.proxy.backend.connector.records.DialectExecutionRecordCounts;
import org.apache.shardingsphere.proxy.backend.connector.records.ExecutionRecordCounts;
import org.firebirdsql.gds.ng.SqlCountHolder;
import org.firebirdsql.jdbc.StatementCountsBridge;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Captures counts from a Firebird JDBC statement. */
public final class FirebirdExecutionRecordCounts implements DialectExecutionRecordCounts {
    
    /**
     * Capture the Firebird execution counts.
     *
     * @param resultSet result set if the statement returned one, otherwise null
     */
    @Override
    public ExecutionRecordCounts capture(final Statement statement, final ResultSet resultSet) throws SQLException {
        if (null != resultSet) {
            statement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
        }
        SqlCountHolder counts = StatementCountsBridge.getCounts(statement);
        return new ExecutionRecordCounts(counts.getIntegerInsertCount(), counts.getIntegerUpdateCount(), counts.getIntegerSelectCount());
    }
    
    @Override
    public String getDatabaseType() {
        return "Firebird";
    }
}
