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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class FirebirdBlobTempSpaceTest {
    
    private final FirebirdBlobTempSpace tempSpace = new FirebirdBlobTempSpace();
    
    @AfterEach
    void tearDown() {
        tempSpace.close();
    }
    
    @Test
    void assertWriteAndRead() {
        long firstOffset = tempSpace.write(new byte[]{1, 2, 3});
        long secondOffset = tempSpace.write(new byte[]{4, 5});
        assertThat(firstOffset, is(0L));
        assertThat(secondOffset, is(3L));
        assertThat(tempSpace.read(secondOffset, 2), is(new byte[]{4, 5}));
        assertThat(tempSpace.read(firstOffset, 3), is(new byte[]{1, 2, 3}));
    }
    
    @Test
    void assertClear() {
        tempSpace.write(new byte[]{1, 2, 3});
        tempSpace.clear();
        assertThat(tempSpace.write(new byte[]{4}), is(0L));
    }
}
