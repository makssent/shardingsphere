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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.blob.FirebirdBatchBlobStreamCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.command.executor.CommandExecutor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

/**
 * Firebird batch blob stream command executor.
 */
@RequiredArgsConstructor
public final class FirebirdBatchBlobStreamCommandExecutor implements CommandExecutor {

    private final FirebirdBatchBlobStreamCommandPacket packet;

    private final ConnectionSession connectionSession;

    @Override
    public Collection<DatabasePacket> execute() throws SQLException {
        FirebirdBlobManager blobManager = FirebirdBlobManager.get(connectionSession);
        ByteBuffer buffer = ByteBuffer.wrap(packet.getStreamData()).order(ByteOrder.BIG_ENDIAN);
        while (buffer.remaining() >= 16) {
            long blobId = buffer.getLong();
            long blobSize = Integer.toUnsignedLong(buffer.getInt());
            long bpbSize = Integer.toUnsignedLong(buffer.getInt());
            if (buffer.remaining() < bpbSize + blobSize) {
                break;
            }
            buffer.position(buffer.position() + (int) bpbSize);
            byte[] blobBytes = new byte[(int) blobSize];
            buffer.get(blobBytes);
            blobManager.registerBlob(blobId, blobBytes);
            int padding = (int) ((4 - (blobSize & 3)) & 3);
            if (buffer.remaining() < padding) {
                break;
            }
            buffer.position(buffer.position() + padding);
        }
        return Collections.singleton(new FirebirdGenericResponsePacket());
    }
}

