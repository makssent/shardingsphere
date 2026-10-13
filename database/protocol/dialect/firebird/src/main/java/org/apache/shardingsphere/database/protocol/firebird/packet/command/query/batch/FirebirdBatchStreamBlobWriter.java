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

package org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch;

/**
 * Writer of the BLOBs that Firebird creates from the BLOB buffer of a batch before executing batch messages.
 *
 * <p>Firebird creates each BLOB right after its header, puts its data and closes it when the next BLOB is created or the BLOB buffer ends;
 * a BLOB of an error is not closed.</p>
 */
public interface FirebirdBatchStreamBlobWriter {
    
    /**
     * Create BLOB.
     *
     * @param bpb BLOB parameter buffer
     * @param segmented whether the BLOB is segmented
     * @return BLOB ID of created BLOB
     */
    long create(byte[] bpb, boolean segmented);
    
    /**
     * Put data into created BLOB: one segment, or the stream data up to the next BLOB header.
     *
     * @param data BLOB data
     */
    void put(byte[] data);
    
    /**
     * Close created BLOB.
     */
    void close();
}
