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

package org.apache.shardingsphere.proxy.frontend.firebird.command.query.batch;

import io.netty.buffer.ByteBufUtil;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBPBTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBlobBufferFormatException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBlobContinuationBPBException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchDefaultBPBChangeException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSegmentExceedsBlobException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSegmentTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSmallDataException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchWithoutBlobsException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BlobFilterNotFoundException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchBlobPolicyException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchHandleException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.RepeatedBatchBlobIdException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.TransliterationFailedException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.UnsupportedBlobFilterException;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchBlobStreamCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchColumnDescriptor;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchMessageCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchRegistry;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchSetBPBCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchStatement;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchStreamBlob;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.FirebirdBlobParameterBufferUtils;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.FirebirdBlobTransliterator;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.cache.FirebirdBlobWriteCache;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.generator.FirebirdBlobIdGenerator;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Batch statement manager for Firebird.
 */
@NoArgsConstructor(access = AccessLevel.NONE)
public final class FirebirdBatchStatementManager {
    
    private static final FirebirdBatchStatementManager INSTANCE = new FirebirdBatchStatementManager();
    
    /**
     * Get batch statement manager instance.
     *
     * @return batch statement manager instance
     */
    public static FirebirdBatchStatementManager getInstance() {
        return INSTANCE;
    }
    
    /**
     * Register connection.
     *
     * @param connectionId connection ID
     */
    public void registerConnection(final int connectionId) {
        FirebirdBatchRegistry.getInstance().registerConnection(connectionId);
    }
    
    /**
     * Unregister connection and its batch statements.
     *
     * @param connectionId connection ID
     */
    public void unregisterConnection(final int connectionId) {
        FirebirdBatchRegistry.getInstance().unregisterConnection(connectionId);
    }
    
    /**
     * Get batch statement.
     *
     * @param connectionId connection ID
     * @param statementId statement ID
     * @return batch statement, or {@code null} if absent
     */
    public FirebirdBatchStatement getBatchStatement(final int connectionId, final int statementId) {
        return FirebirdBatchRegistry.getInstance().getBatchStatement(connectionId, statementId);
    }
    
    /**
     * Register batch statement.
     *
     * @param connectionId connection ID
     * @param statementId statement ID
     * @param columnDescriptors column descriptors
     * @param bufferSize buffer size
     * @param recordCounts whether record counts are requested
     * @param multiError whether multiple errors are requested
     * @param blobStreamAllowed whether BLOB policy allows BLOB stream
     */
    public void registerBatchStatement(final int connectionId, final int statementId, final List<FirebirdBatchColumnDescriptor> columnDescriptors,
                                       final long bufferSize, final boolean recordCounts, final boolean multiError, final boolean blobStreamAllowed) {
        FirebirdBatchRegistry.getInstance().registerBatchStatement(connectionId, statementId,
                new FirebirdBatchStatement(statementId, columnDescriptors, bufferSize, recordCounts, multiError, blobStreamAllowed));
    }
    
    /**
     * Append batch message.
     *
     * @param connectionId connection ID
     * @param packet batch message packet
     * @throws InvalidBatchHandleException if batch statement does not exist
     * @throws BatchTooBigException if appending the message exceeds the batch buffer size
     */
    @HighFrequencyInvocation
    public void appendBatchMessage(final int connectionId, final FirebirdBatchMessageCommandPacket packet) {
        FirebirdBatchStatement batchStatement = getBatchStatement(connectionId, packet.getStatementHandle());
        ShardingSpherePreconditions.checkNotNull(batchStatement, () -> new InvalidBatchHandleException(packet.getStatementHandle()));
        long messageCount = packet.getMessageCount();
        if (0L == messageCount) {
            return;
        }
        int alignment = batchStatement.getMessageAlignment();
        long alignedSize = batchStatement.getAccumulatedSize() + alignment - 1 & -alignment;
        long alignedMessageLength = (long) batchStatement.getMessageLength() + alignment - 1 & -alignment;
        long dataLength = (messageCount - 1) * alignedMessageLength + batchStatement.getMessageLength();
        ShardingSpherePreconditions.checkState(alignedSize + dataLength <= batchStatement.getBufferSize(),
                () -> new BatchTooBigException(packet.getStatementHandle(), alignedSize, dataLength, batchStatement.getBufferSize()));
        for (List<Object> each : packet.readParameterValues(batchStatement.getColumnDescriptors())) {
            batchStatement.addParameterValues(each);
        }
        batchStatement.addSize(alignedSize - batchStatement.getAccumulatedSize() + dataLength);
    }
    
    /**
     * Append BLOB stream portion.
     *
     * @param connectionId connection ID
     * @param packet batch BLOB stream packet
     * @throws InvalidBatchHandleException if batch statement does not exist
     * @throws InvalidBatchBlobPolicyException if BLOB policy of batch does not allow BLOB stream
     */
    @HighFrequencyInvocation
    void appendBlobStream(final int connectionId, final FirebirdBatchBlobStreamCommandPacket packet) {
        FirebirdBatchStatement batchStatement = getBatchStatement(connectionId, packet.getStatementHandle());
        ShardingSpherePreconditions.checkNotNull(batchStatement, () -> new InvalidBatchHandleException(packet.getStatementHandle()));
        if (0L == packet.getLength()) {
            return;
        }
        ShardingSpherePreconditions.checkState(batchStatement.hasBatchBlobIdColumn(), () -> new BatchWithoutBlobsException(packet.getStatementHandle()));
        ShardingSpherePreconditions.checkState(batchStatement.isBlobStreamAllowed(), () -> new InvalidBatchBlobPolicyException("addBlobStream"));
        batchStatement.appendBlobStream(packet.getData(), packet.getLength());
    }
    
    /**
     * Set default BLOB parameter buffer.
     *
     * @param connectionId connection ID
     * @param packet batch set default BLOB parameter buffer packet
     * @throws InvalidBatchHandleException if batch statement does not exist
     * @throws BatchDefaultBPBChangeException if batch already contains BLOB stream data
     */
    void setDefaultBPB(final int connectionId, final FirebirdBatchSetBPBCommandPacket packet) {
        FirebirdBatchStatement batchStatement = getBatchStatement(connectionId, packet.getStatementHandle());
        ShardingSpherePreconditions.checkNotNull(batchStatement, () -> new InvalidBatchHandleException(packet.getStatementHandle()));
        batchStatement.getBlobStream().setDefaultBPB(packet.getBpb());
        ShardingSpherePreconditions.checkState(0L == batchStatement.getBlobStreamSize(), () -> new BatchDefaultBPBChangeException(packet.getStatementHandle()));
        batchStatement.setDefaultBpb(ByteBufUtil.getBytes(packet.getBpb()));
    }
    
    /**
     * Register BLOBs of the BLOB buffer under their batch BLOB IDs, as Firebird does before executing batch messages, and clear the BLOB buffer.
     *
     * @param batchStatement batch statement
     * @param connectionId connection ID
     * @param transactionId transaction ID
     * @param connectionCharsetId character set ID of the connection
     * @throws BatchSmallDataException if a BLOB header is incomplete
     * @throws BatchBlobBufferFormatException if a continuation has no BLOB to continue
     * @throws BatchBlobContinuationBPBException if a BLOB continuation contains a BLOB parameter buffer
     * @throws BatchBPBTooBigException if a BLOB parameter buffer is incomplete
     * @throws RepeatedBatchBlobIdException if batch BLOB ID is already registered
     * @throws BatchSegmentExceedsBlobException if a BLOB segment exceeds its BLOB
     * @throws BatchSegmentTooBigException if a BLOB segment is incomplete
     * @throws BlobFilterNotFoundException if Firebird has no BLOB filter for the sub type conversion of a BLOB parameter buffer
     * @throws UnsupportedBlobFilterException if a BLOB parameter buffer requires an unsupported BLOB filter conversion
     * @throws TransliterationFailedException if BLOB data cannot be transliterated as its BLOB parameter buffer requires
     */
    @HighFrequencyInvocation
    void registerStreamBlobs(final FirebirdBatchStatement batchStatement, final int connectionId, final int transactionId, final int connectionCharsetId) {
        for (FirebirdBatchStreamBlob each : batchStatement.readStreamBlobs()) {
            byte[] content = transliterate(each, connectionCharsetId);
            long blobId = FirebirdBlobIdGenerator.getInstance().nextBlobId(connectionId);
            FirebirdBlobWriteCache.getInstance().registerMaterializedBlob(connectionId, blobId, transactionId, content);
            batchStatement.getBlobIds().put(each.getBatchBlobId(), blobId);
        }
        batchStatement.clearBlobStream();
    }
    
    @HighFrequencyInvocation
    private byte[] transliterate(final FirebirdBatchStreamBlob blob, final int connectionCharsetId) {
        byte[] data = blob.getData().toByteArray();
        Optional<FirebirdBlobTransliterator> transliterator = FirebirdBlobParameterBufferUtils.createTransliterator(blob.getBpb(), connectionCharsetId);
        if (!transliterator.isPresent()) {
            return data;
        }
        if (!blob.isSegmented()) {
            return transliterator.get().put(data);
        }
        ByteArrayOutputStream result = new ByteArrayOutputStream(data.length);
        int offset = 0;
        if (blob.getEmptySegments().get(0)) {
            transliterator.get().put(new byte[0]);
        }
        for (int end = blob.getSegmentEnds().nextSetBit(0); end >= 0; end = blob.getSegmentEnds().nextSetBit(end + 1)) {
            byte[] converted = transliterator.get().put(Arrays.copyOfRange(data, offset, end));
            result.write(converted, 0, converted.length);
            if (blob.getEmptySegments().get(end)) {
                transliterator.get().put(new byte[0]);
            }
            offset = end;
        }
        return result.toByteArray();
    }
    
    /**
     * Reset batch statement.
     *
     * @param batchStatement batch statement
     */
    public void resetBatchStatement(final FirebirdBatchStatement batchStatement) {
        batchStatement.reset();
    }
    
    /**
     * Unregister batch statement.
     *
     * @param connectionId connection ID
     * @param statementId statement ID
     */
    public void unregisterBatchStatement(final int connectionId, final int statementId) {
        FirebirdBatchRegistry.getInstance().unregisterBatchStatement(connectionId, statementId);
    }
}
