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
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.generator.FirebirdBlobHandleGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.internal.configuration.plugins.Plugins;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

class FirebirdBlobWriteCacheTest {
    
    private static final FirebirdBlobWriteCache CACHE = FirebirdBlobWriteCache.getInstance();
    
    @AfterEach
    void tearDown() {
        new ArrayList<>(getIdCache().keySet()).forEach(CACHE::unregisterConnection);
        getHandleCache().clear();
        getIdCache().clear();
    }
    
    @Test
    void assertRegisterAndUnregisterConnection() {
        int connectionId = 1;
        CACHE.registerConnection(connectionId);
        Map<Integer, Map<Integer, FirebirdBlobWrite>> handleMap = getHandleCache();
        Map<Integer, Map<Long, FirebirdBlobWrite>> idMap = getIdCache();
        assertTrue(handleMap.containsKey(connectionId));
        assertTrue(idMap.containsKey(connectionId));
        CACHE.unregisterConnection(connectionId);
        assertFalse(handleMap.containsKey(connectionId));
        assertFalse(idMap.containsKey(connectionId));
    }
    
    @Test
    void assertRegisterBlobCreatesMappingsWhenConnectionMissing() {
        CACHE.registerBlob(2, 3, 4L, 1, false);
        OptionalLong actualBlobId = CACHE.getBlobId(2, 3);
        assertTrue(actualBlobId.isPresent());
        assertThat(actualBlobId.getAsLong(), is(4L));
    }
    
    @ParameterizedTest(name = "{0}")
    @MethodSource("missingWriteProvider")
    void assertMissingWriteReturnsEmpty(final String caseName, final BooleanSupplier invocation) {
        assertFalse(invocation.getAsBoolean(), caseName);
    }
    
    @Test
    void assertIsClosedStatusTransitions() {
        int connectionId = 6;
        int blobHandle = 8;
        long blobId = 9L;
        CACHE.registerConnection(connectionId);
        assertFalse(CACHE.isClosed(connectionId, blobId));
        CACHE.registerBlob(connectionId, blobHandle, blobId, 1, false);
        OptionalInt actualSizeAfterAppend = CACHE.appendSegment(connectionId, blobHandle, new byte[]{1, 2, 3});
        assertTrue(actualSizeAfterAppend.isPresent());
        assertThat(actualSizeAfterAppend.getAsInt(), is(3));
        assertFalse(CACHE.isClosed(connectionId, blobId));
        OptionalInt actualSizeAfterClose = CACHE.closeWrite(connectionId, blobHandle);
        assertTrue(actualSizeAfterClose.isPresent());
        assertThat(actualSizeAfterClose.getAsInt(), is(3));
        assertFalse(CACHE.getBlobId(connectionId, blobHandle).isPresent());
        assertTrue(CACHE.isClosed(connectionId, blobId));
        Optional<byte[]> actualBlobData = CACHE.getBlobData(connectionId, blobId);
        assertTrue(actualBlobData.isPresent());
        assertThat(actualBlobData.get(), is(new byte[]{1, 2, 3}));
        CACHE.removeWrite(connectionId, blobId);
        assertFalse(getHandleCache().get(connectionId).containsKey(blobHandle));
        assertFalse(getIdCache().get(connectionId).containsKey(blobId));
    }
    
    @Test
    void assertGetBlobDataAfterClose() {
        CACHE.registerBlob(1, 11, 21L, 31, false);
        CACHE.appendSegment(1, 11, new byte[]{1, 2});
        CACHE.appendSegment(1, 11, new byte[]{3});
        CACHE.closeWrite(1, 11);
        byte[] actual = CACHE.getBlobData(1, 21L).get();
        assertThat(actual, is(new byte[]{1, 2, 3}));
        assertThat(CACHE.getBlobData(1, 21L).get(), sameInstance(actual));
    }
    
    @Test
    void assertReuseBlobData() {
        CACHE.registerBlob(1, 11, 21L, 31, false);
        CACHE.appendSegment(1, 11, new byte[]{1, 2, 3});
        CACHE.closeWrite(1, 11);
        byte[] actual = CACHE.useBlobData(1, 21L, 31).get();
        assertThat(actual, is(new byte[]{1, 2, 3}));
        assertThat(CACHE.useBlobData(1, 21L, 31).get(), sameInstance(actual));
    }
    
    @Test
    @SneakyThrows(ReflectiveOperationException.class)
    void assertRecoverUsedBlobAfterCachedBytesAreReleased() {
        CACHE.registerBlob(1, 11, 21L, 31, false);
        byte[] expected = new byte[128 * 1024 + 1];
        Arrays.fill(expected, (byte) 7);
        CACHE.appendSegment(1, 11, expected);
        CACHE.closeWrite(1, 11);
        CACHE.useBlobData(1, 21L, 31);
        FirebirdBlobWrite write = getIdCache().get(1).get(21L);
        SoftReference<?> cached = (SoftReference<?>) Plugins.getMemberAccessor().get(FirebirdBlobWrite.class.getDeclaredField("usedContent"), write);
        cached.clear();
        assertThat(CACHE.useBlobData(1, 21L, 31).get(), is(expected));
        CACHE.removeTransactionWrites(1, 31);
        assertFalse(CACHE.getBlobData(1, 21L).isPresent());
    }
    
    @Test
    @SneakyThrows(ReflectiveOperationException.class)
    void assertRecoverMaterializedBlobAfterCachedBytesAreReleased() {
        CACHE.registerConnection(1);
        byte[] expected = new byte[128 * 1024 + 1];
        Arrays.fill(expected, (byte) 9);
        CACHE.registerMaterializedBlob(1, 21L, 31, expected);
        FirebirdBlobWrite write = getIdCache().get(1).get(21L);
        SoftReference<?> cached = (SoftReference<?>) Plugins.getMemberAccessor().get(FirebirdBlobWrite.class.getDeclaredField("usedContent"), write);
        cached.clear();
        assertThat(CACHE.useBlobData(1, 21L, 31).get(), is(expected));
        assertFalse(CACHE.useBlobData(1, 21L, 32).isPresent());
        CACHE.removeTransactionWrites(1, 31);
        assertFalse(CACHE.useBlobData(1, 21L, 31).isPresent());
    }
    
    @Test
    void assertUseBlobDataOfOtherTransaction() {
        CACHE.registerBlob(1, 11, 21L, 31, false);
        CACHE.closeWrite(1, 11);
        assertFalse(CACHE.useBlobData(1, 21L, 32).isPresent());
    }
    
    @Test
    void assertUseBlobDataBeforeClose() {
        CACHE.registerBlob(1, 11, 21L, 31, false);
        assertFalse(CACHE.useBlobData(1, 21L, 31).isPresent());
    }
    
    @Test
    void assertRemoveTransactionWrites() {
        CACHE.registerBlob(1, 11, 21L, 31, false);
        CACHE.registerBlob(1, 13, 23L, 31, false);
        CACHE.appendSegment(1, 11, new byte[]{1, 2, 3});
        CACHE.closeWrite(1, 11);
        CACHE.registerBlob(1, 11, 22L, 32, false);
        CACHE.useBlobData(1, 21L, 31);
        CACHE.removeTransactionWrites(1, 31);
        assertFalse(CACHE.useBlobData(1, 21L, 31).isPresent());
        assertFalse(CACHE.getBlobId(1, 13).isPresent());
        assertThat(CACHE.getBlobId(1, 11).getAsLong(), is(22L));
    }
    
    @Test
    void assertRemoveTransactionWritesReleasesOnlyOwnedOpenHandles() {
        int connectionId = 41;
        FirebirdBlobHandleGenerator generator = FirebirdBlobHandleGenerator.getInstance();
        generator.registerConnection(connectionId);
        try {
            CACHE.registerConnection(connectionId);
            int openHandle = generator.nextBlobHandle(connectionId);
            CACHE.registerBlob(connectionId, openHandle, 21L, 31, false);
            int reusedHandle = generator.nextBlobHandle(connectionId);
            CACHE.registerBlob(connectionId, reusedHandle, 22L, 31, false);
            CACHE.closeWrite(connectionId, reusedHandle);
            generator.releaseBlobHandle(connectionId, reusedHandle);
            assertThat(generator.nextBlobHandle(connectionId), is(reusedHandle));
            CACHE.registerBlob(connectionId, reusedHandle, 23L, 32, false);
            CACHE.removeTransactionWrites(connectionId, 31);
            assertFalse(generator.isAllocated(connectionId, openHandle));
            assertTrue(generator.isAllocated(connectionId, reusedHandle));
            assertThat(CACHE.getBlobId(connectionId, reusedHandle).getAsLong(), is(23L));
            assertThat(generator.nextBlobHandle(connectionId), is(openHandle));
        } finally {
            generator.unregisterConnection(connectionId);
        }
    }
    
    @Test
    @SneakyThrows({InterruptedException.class, ExecutionException.class, TimeoutException.class})
    void assertUnregisterWaitsForBlobUseBeforeClosingStore() {
        int connectionId = 42;
        CACHE.registerConnection(connectionId);
        CACHE.registerBlob(connectionId, 11, 21L, 31, false);
        byte[] expected = new byte[128 * 1024 + 1];
        CACHE.appendSegment(connectionId, 11, expected);
        CACHE.closeWrite(connectionId, 11);
        FirebirdBlobWrite write = spy(getIdCache().get(connectionId).get(21L));
        getIdCache().get(connectionId).put(21L, write);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        CountDownLatch cleanupStarted = new CountDownLatch(1);
        CountDownLatch cleanupFinished = new CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            assertTrue(resume.await(5L, TimeUnit.SECONDS));
            return invocation.callRealMethod();
        }).when(write).getBytes();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Optional<byte[]>> use = executor.submit(() -> CACHE.useBlobData(connectionId, 21L, 31));
            assertTrue(entered.await(5L, TimeUnit.SECONDS));
            Future<?> cleanup = executor.submit(() -> {
                cleanupStarted.countDown();
                CACHE.unregisterConnection(connectionId);
                cleanupFinished.countDown();
            });
            assertTrue(cleanupStarted.await(5L, TimeUnit.SECONDS));
            assertFalse(cleanupFinished.await(100L, TimeUnit.MILLISECONDS));
            resume.countDown();
            assertThat(use.get(5L, TimeUnit.SECONDS).get(), is(expected));
            cleanup.get(5L, TimeUnit.SECONDS);
            assertFalse(getStores().containsKey(connectionId));
            assertFalse(getIdCache().containsKey(connectionId));
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }
    
    @Test
    @SneakyThrows({InterruptedException.class, ExecutionException.class, TimeoutException.class})
    void assertBlobUseDoesNotBlockOtherConnection() {
        int firstConnectionId = 1;
        int otherConnectionId = 1025;
        CACHE.registerBlob(firstConnectionId, 11, 21L, 31, false);
        CACHE.closeWrite(firstConnectionId, 11);
        CACHE.registerBlob(otherConnectionId, 12, 22L, 32, false);
        CACHE.appendSegment(otherConnectionId, 12, new byte[]{9});
        CACHE.closeWrite(otherConnectionId, 12);
        FirebirdBlobWrite write = spy(getIdCache().get(firstConnectionId).get(21L));
        getIdCache().get(firstConnectionId).put(21L, write);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            assertTrue(resume.await(15L, TimeUnit.SECONDS));
            return invocation.callRealMethod();
        }).when(write).getBytes();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Optional<byte[]>> first = executor.submit(() -> CACHE.useBlobData(firstConnectionId, 21L, 31));
            assertTrue(entered.await(5L, TimeUnit.SECONDS));
            Future<Optional<byte[]>> other = executor.submit(() -> CACHE.useBlobData(otherConnectionId, 22L, 32));
            assertThat(other.get(5L, TimeUnit.SECONDS).get(), is(new byte[]{9}));
            resume.countDown();
            assertTrue(first.get(5L, TimeUnit.SECONDS).isPresent());
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }
    
    @Test
    void assertRemoveWriteDoesNotRemoveReusedHandle() {
        int connectionId = 12;
        int blobHandle = 8;
        long oldBlobId = 9L;
        long expectedBlobId = 10L;
        CACHE.registerConnection(connectionId);
        CACHE.registerBlob(connectionId, blobHandle, oldBlobId, 1, false);
        CACHE.closeWrite(connectionId, blobHandle);
        CACHE.registerBlob(connectionId, blobHandle, expectedBlobId, 1, false);
        CACHE.removeWrite(connectionId, oldBlobId);
        OptionalLong actualBlobId = CACHE.getBlobId(connectionId, blobHandle);
        assertTrue(actualBlobId.isPresent());
        assertThat(actualBlobId.getAsLong(), is(expectedBlobId));
    }
    
    @Test
    void assertRemoveWriteWhenWriteMissing() {
        int connectionId = 7;
        CACHE.registerConnection(connectionId);
        CACHE.removeWrite(connectionId, 11L);
        assertTrue(getHandleCache().get(connectionId).isEmpty());
        assertTrue(getIdCache().get(connectionId).isEmpty());
    }
    
    @SuppressWarnings("unchecked")
    @SneakyThrows(ReflectiveOperationException.class)
    private Map<Integer, Map<Integer, FirebirdBlobWrite>> getHandleCache() {
        return (Map<Integer, Map<Integer, FirebirdBlobWrite>>) Plugins.getMemberAccessor().get(FirebirdBlobWriteCache.class.getDeclaredField("writesByHandle"), CACHE);
    }
    
    @SuppressWarnings("unchecked")
    @SneakyThrows(ReflectiveOperationException.class)
    private Map<Integer, Map<Long, FirebirdBlobWrite>> getIdCache() {
        return (Map<Integer, Map<Long, FirebirdBlobWrite>>) Plugins.getMemberAccessor().get(FirebirdBlobWriteCache.class.getDeclaredField("writesById"), CACHE);
    }
    
    @SneakyThrows(ReflectiveOperationException.class)
    private Map<?, ?> getStores() {
        return (Map<?, ?>) Plugins.getMemberAccessor().get(FirebirdBlobWriteCache.class.getDeclaredField("storesByTransaction"), CACHE);
    }
    
    private static Stream<Arguments> missingWriteProvider() {
        return Stream.of(
                Arguments.of("append missing write returns empty", (BooleanSupplier) () -> CACHE.appendSegment(3, 5, new byte[]{1}).isPresent()),
                Arguments.of("close missing write returns empty", (BooleanSupplier) () -> CACHE.closeWrite(4, 6).isPresent()),
                Arguments.of("get blob data missing write returns empty", (BooleanSupplier) () -> CACHE.getBlobData(5, 7L).isPresent()),
                Arguments.of("get blob id missing write returns empty", (BooleanSupplier) () -> CACHE.getBlobId(8, 10).isPresent()));
    }
}
