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

package org.apache.shardingsphere.test.it.sql.parser.firebird;

import org.apache.shardingsphere.sql.parser.engine.api.CacheOption;
import org.apache.shardingsphere.sql.parser.engine.api.SQLParserEngine;
import org.apache.shardingsphere.sql.parser.engine.api.SQLStatementVisitorEngine;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.ddl.ExecuteStatement;
import org.apache.shardingsphere.test.it.sql.parser.internal.InternalSQLParserIT;
import org.apache.shardingsphere.test.it.sql.parser.internal.InternalSQLParserITSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@InternalSQLParserITSettings("Firebird")
class InternalFirebirdParserIT extends InternalSQLParserIT {
    
    @Test
    void assertExecuteBlockOutput() {
        SQLParserEngine parser = new SQLParserEngine("Firebird", new CacheOption(128, 1024L));
        SQLStatementVisitorEngine visitor = new SQLStatementVisitorEngine("Firebird");
        ExecuteStatement actualWithOutput = (ExecuteStatement) visitor.visit(parser.parse("EXECUTE BLOCK RETURNS (V INTEGER) AS BEGIN V = 1; SUSPEND; END;", false));
        ExecuteStatement actualWithoutSuspend = (ExecuteStatement) visitor.visit(parser.parse("EXECUTE BLOCK RETURNS (V INTEGER) AS BEGIN V = 1; END;", false));
        ExecuteStatement actualWithoutOutput = (ExecuteStatement) visitor.visit(parser.parse("EXECUTE BLOCK AS BEGIN END;", false));
        ExecuteStatement actualProcedure = (ExecuteStatement) visitor.visit(parser.parse("EXECUTE PROCEDURE foo_procedure", false));
        assertTrue(actualWithOutput.isReturnsRows());
        assertTrue(actualWithoutSuspend.isReturnsRows());
        assertFalse(actualWithoutOutput.isReturnsRows());
        assertFalse(actualProcedure.isReturnsRows());
    }
}
