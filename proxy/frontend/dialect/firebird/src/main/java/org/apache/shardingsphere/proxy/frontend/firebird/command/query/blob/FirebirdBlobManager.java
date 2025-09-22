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

import io.netty.util.AttributeKey;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manager for Firebird blob contexts bound to a {@link ConnectionSession}.
 */
public final class FirebirdBlobManager {

    private static final AttributeKey<FirebirdBlobManager> ATTRIBUTE_KEY = AttributeKey.valueOf("firebird-blob-manager");

    private final AtomicInteger nextHandle = new AtomicInteger(1);

    private final AtomicLong nextBlobId = new AtomicLong(1L);

    private final Map<Integer, FirebirdBlobContext> contextsByHandle = new ConcurrentHashMap<>();

    private final Map<Long, byte[]> blobsById = new ConcurrentHashMap<>();

    private FirebirdBlobManager() {
    }

    /**
     * Get blob manager for connection session.
     *
     * @param connectionSession connection session
     * @return blob manager
     */
    public static FirebirdBlobManager get(final ConnectionSession connectionSession) {
        FirebirdBlobManager result = connectionSession.getAttributeMap().attr(ATTRIBUTE_KEY).get();
        if (null == result) {
            result = new FirebirdBlobManager();
            connectionSession.getAttributeMap().attr(ATTRIBUTE_KEY).set(result);
        }
        return result;
    }

    /**
     * Create new blob context.
     *
     * @return created blob context
     */
    public FirebirdBlobContext createBlobContext() {
        int handle = nextHandle.getAndIncrement();
        long blobId = nextBlobId.getAndIncrement();
        FirebirdBlobContext context = new FirebirdBlobContext(handle, blobId);
        contextsByHandle.put(handle, context);
        return context;
    }

    /**
     * Find blob context by handle.
     *
     * @param handle blob handle
     * @return blob context
     */
    public Optional<FirebirdBlobContext> findContext(final int handle) {
        return Optional.ofNullable(contextsByHandle.get(handle));
    }

    /**
     * Finalize blob context.
     *
     * @param handle blob handle
     */
    public void closeContext(final int handle) {
        FirebirdBlobContext context = contextsByHandle.remove(handle);
        if (null != context) {
            blobsById.put(context.getBlobId(), context.toByteArray());
        }
    }

    /**
     * Store inline blob data using supplied blob id.
     *
     * @param blobId blob identifier
     * @param data blob data
     */
    public void registerBlob(final long blobId, final byte[] data) {
        blobsById.put(blobId, data);
    }

    /**
     * Consume blob data by blob id.
     *
     * @param blobId blob identifier
     * @return blob data
     */
    public Optional<byte[]> consumeBlob(final long blobId) {
        byte[] result = blobsById.remove(blobId);
        return Optional.ofNullable(result);
    }

    /**
     * Get blob data without removing it.
     *
     * @param blobId blob identifier
     * @return blob data
     */
    public Optional<byte[]> getBlob(final long blobId) {
        return Optional.ofNullable(blobsById.get(blobId));
    }

    @RequiredArgsConstructor
    @Getter
    public static final class FirebirdBlobContext {

        private final int handle;

        private final long blobId;

        private final ByteArrayBuilder builder = new ByteArrayBuilder();

        /**
         * Append segment bytes.
         *
         * @param bytes segment bytes
         */
        public void append(final byte[] bytes) {
            builder.append(bytes);
        }

        private byte[] toByteArray() {
            return builder.build();
        }
    }

    /**
     * Simple byte array accumulator.
     */
    private static final class ByteArrayBuilder {

        private byte[] buffer = new byte[0];

        private int length;

        void append(final byte[] bytes) {
            ensureCapacity(length + bytes.length);
            System.arraycopy(bytes, 0, buffer, length, bytes.length);
            length += bytes.length;
        }

        private void ensureCapacity(final int capacity) {
            if (capacity <= buffer.length) {
                return;
            }
            int newCapacity = Math.max(buffer.length << 1, capacity);
            if (0 == newCapacity) {
                newCapacity = capacity;
            }
            byte[] newBuffer = new byte[newCapacity];
            System.arraycopy(buffer, 0, newBuffer, 0, length);
            buffer = newBuffer;
        }

        byte[] build() {
            byte[] result = new byte[length];
            System.arraycopy(buffer, 0, result, 0, length);
            return result;
        }
    }

}

