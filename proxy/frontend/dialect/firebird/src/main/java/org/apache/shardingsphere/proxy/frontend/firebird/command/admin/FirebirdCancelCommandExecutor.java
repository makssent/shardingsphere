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

package org.apache.shardingsphere.proxy.frontend.firebird.command.admin;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.admin.FirebirdCancelCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.proxy.frontend.command.executor.CommandExecutor;

import java.util.Collection;
import java.util.Collections;

/**
 * Cancel command executor for Firebird.
 */
@RequiredArgsConstructor
public final class FirebirdCancelCommandExecutor implements CommandExecutor {
    
    private static final int CANCEL_DISABLE = 1;
    
    private static final int CANCEL_ENABLE = 2;
    
    private static final int CANCEL_RAISE = 3;
    
    private static final int CANCEL_ABORT = 4;
    
    private final FirebirdCancelCommandPacket packet;
    
    @Override
    public Collection<DatabasePacket> execute() {
        switch (packet.getKind()) {
            case CANCEL_DISABLE:
                return Collections.singleton(new FirebirdGenericResponsePacket());
            case CANCEL_ENABLE:
                return Collections.singleton(new FirebirdGenericResponsePacket());
            case CANCEL_RAISE:
                return Collections.singleton(new FirebirdGenericResponsePacket());
            case CANCEL_ABORT:
                return Collections.singleton(new FirebirdGenericResponsePacket());
            default:
                return Collections.singleton(new FirebirdGenericResponsePacket());
        }
    }
}
