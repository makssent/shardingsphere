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

package org.apache.shardingsphere.database.protocol.firebird.packet.generic;

import org.apache.shardingsphere.database.protocol.firebird.packet.FirebirdPacket;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;

/**
 * Packet that closes the connection without response for Firebird.
 *
 * <p>Firebird closes the connection without response when it cannot decode a packet whose statement handle is unknown,
 * such as {@code op_batch_set_bpb} and {@code op_batch_blob_stream} ({@code getStatement} in {@code protocol.cpp}).</p>
 */
public final class FirebirdConnectionClosePacket extends FirebirdPacket {
    
    @Override
    protected void write(final FirebirdPacketPayload payload) {
    }
}
