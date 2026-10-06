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

import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 * Temporary space for the data of used Firebird BLOB writes of a connection.
 *
 * <p>Firebird keeps the data of temporary BLOBs of a transaction in its temporary space, in memory and then on disk, until the transaction ends.
 * The proxy writes the data of a used BLOB to a temporary file, which is created on the first write, emptied when the transaction ends and deleted when it is closed.</p>
 */
final class FirebirdBlobTempSpace {
    
    private FileChannel file;
    
    /**
     * Write data to the end of the temporary space.
     *
     * @param data data
     * @return offset of the written data
     */
    @SneakyThrows(IOException.class)
    long write(final byte[] data) {
        if (null == file) {
            file = FileChannel.open(Files.createTempFile("shardingsphere-firebird-blob", ".tmp"), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
        }
        long result = file.size();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long position = result;
        while (buffer.hasRemaining()) {
            position += file.write(buffer, position);
        }
        return result;
    }
    
    /**
     * Read data written to the temporary space.
     *
     * @param offset offset of the data
     * @param length length of the data
     * @return data
     */
    @SneakyThrows(IOException.class)
    byte[] read(final long offset, final int length) {
        ByteBuffer result = ByteBuffer.allocate(length);
        while (result.hasRemaining()) {
            file.read(result, offset + result.position());
        }
        return result.array();
    }
    
    /**
     * Discard the data of the temporary space.
     */
    @SneakyThrows(IOException.class)
    void clear() {
        if (null != file) {
            file.truncate(0L);
        }
    }
    
    /**
     * Close and delete the temporary space.
     */
    @SneakyThrows(IOException.class)
    void close() {
        if (null != file) {
            file.close();
        }
    }
}
