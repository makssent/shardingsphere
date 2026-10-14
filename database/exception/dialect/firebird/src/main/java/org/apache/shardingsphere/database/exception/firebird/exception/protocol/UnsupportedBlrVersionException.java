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

package org.apache.shardingsphere.database.exception.firebird.exception.protocol;

import lombok.Getter;
import org.apache.shardingsphere.database.exception.core.exception.SQLDialectException;

/**
 * Unsupported BLR version exception for Firebird.
 */
@Getter
public final class UnsupportedBlrVersionException extends SQLDialectException {
    
    private static final long serialVersionUID = 8460326734195021957L;
    
    private final int minVersion;
    
    private final int maxVersion;
    
    private final int version;
    
    public UnsupportedBlrVersionException(final int minVersion, final int maxVersion, final int version) {
        super(String.format("unsupported BLR version (expected between %d and %d, encountered %d)", minVersion, maxVersion, version));
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
        this.version = version;
    }
}
