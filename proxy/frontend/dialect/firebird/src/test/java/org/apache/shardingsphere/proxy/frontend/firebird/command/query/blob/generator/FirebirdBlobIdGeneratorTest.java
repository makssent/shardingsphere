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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.generator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class FirebirdBlobIdGeneratorTest {
    
    private static final int CONNECTION_ID = 1;
    
    private final FirebirdBlobIdGenerator generator = FirebirdBlobIdGenerator.getInstance();
    
    @BeforeEach
    void setUp() {
        generator.registerConnection(CONNECTION_ID);
    }
    
    @AfterEach
    void tearDown() {
        generator.unregisterConnection(CONNECTION_ID);
    }
    
    @Test
    void assertNextBlobId() {
        assertThat(generator.nextBlobId(CONNECTION_ID, false), is(1L));
    }
    
    @Test
    void assertNextFilteredBlobId() {
        assertThat(generator.nextBlobId(CONNECTION_ID, true), is(2L));
    }
    
    @Test
    void assertResetBlobId() {
        generator.nextBlobId(CONNECTION_ID, false);
        generator.resetBlobId(CONNECTION_ID);
        assertThat(generator.nextBlobId(CONNECTION_ID, false), is(1L));
    }
}
