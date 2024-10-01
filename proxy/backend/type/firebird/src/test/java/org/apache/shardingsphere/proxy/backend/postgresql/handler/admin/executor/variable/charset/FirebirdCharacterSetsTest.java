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

package org.apache.shardingsphere.proxy.backend.postgresql.handler.admin.executor.variable.charset;

import org.apache.shardingsphere.proxy.backend.firebird.handler.admin.executor.variable.charset.FirebirdCharacterSets;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FirebirdCharacterSetsTest {
    
    @Test
    void assertFindSqlAscii() {
        assertThat(FirebirdCharacterSets.findCharacterSet("ASCII"), is(StandardCharsets.US_ASCII));
    }
    
    @Test
    void assertFindUTF8() {
        assertThat(FirebirdCharacterSets.findCharacterSet("utf8"), is(StandardCharsets.UTF_8));
    }
    
    @Test
    void assertFindUTF8WithQuotes() {
        assertThat(FirebirdCharacterSets.findCharacterSet("'utf8'"), is(StandardCharsets.UTF_8));
    }
    
    @Test
    void assertFindUnsupportedCharset() {
        assertThrows(UnsupportedCharsetException.class, () -> FirebirdCharacterSets.findCharacterSet("unknown_charset"));
    }
}