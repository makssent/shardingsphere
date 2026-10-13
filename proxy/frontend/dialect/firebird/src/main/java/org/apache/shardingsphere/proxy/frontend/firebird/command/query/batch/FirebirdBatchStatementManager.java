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
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBlobBufferFormatException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBlobContinuationBpbException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBpbTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchDefaultBpbChangeException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSegmentExceedsBlobException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSegmentTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSmallDataException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchWithoutBlobsException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BlobFilterNotFoundException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BlobFilterWriteNotSupportedException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchBlobPolicyException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchHandleException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidStatementStateException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.RepeatedBatchBlobIdException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.TransliterationFailedException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.UnsupportedBlobFilterException;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchBlobStreamCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchColumnDescriptor;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchMessageCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchRegistry;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchSetBpbCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchStatement;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchStreamBlob;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.FirebirdBlobParameterBufferUtils;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.cache.FirebirdBlobWriteCache;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.blob.generator.FirebirdBlobIdGenerator;
import org.apache.shardingsphere.proxy.frontend.firebird.command.query.statement.FirebirdStatementIdGenerator;

import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

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
     * Check the statement of a batch operation as Firebird does before it checks the batch: the statement handle is allocated and the statement is prepared.
     *
     * @param connectionSession connection session
     * @param statementHandle statement handle
     * @throws InvalidStatementStateException if the statement handle is not allocated or the statement is not prepared
     */
    public void checkStatement(final ConnectionSession connectionSession, final int statementHandle) {
        ShardingSpherePreconditions.checkState(FirebirdStatementIdGenerator.getInstance().isAllocated(connectionSession.getConnectionId(), statementHandle),
                () -> new InvalidStatementStateException(statementHandle, false));
        ShardingSpherePreconditions.checkNotNull(connectionSession.getServerPreparedStatementRegistry().getPreparedStatement(statementHandle),
                () -> new InvalidStatementStateException(statementHandle, true));
    }
    
    /**
     * Register batch statement.
     *
     * @param connectionId connection ID
     * @param statementId statement ID
     * @param columnDescriptors column descriptors
     * @param messageLength message length
     * @param alignedMessageLength aligned message length
     * @param bufferSize buffer size
     * @param recordCounts whether record counts are requested
     * @param multiError whether multiple errors are requested
     * @param blobPolicy BLOB policy of batch
     */
    public void registerBatchStatement(final int connectionId, final int statementId, final List<FirebirdBatchColumnDescriptor> columnDescriptors, final int messageLength,
                                       final int alignedMessageLength, final long bufferSize, final boolean recordCounts, final boolean multiError, final int blobPolicy) {
        FirebirdBatchRegistry.getInstance().registerBatchStatement(connectionId, statementId,
                new FirebirdBatchStatement(statementId, columnDescriptors, messageLength, alignedMessageLength, bufferSize, recordCounts, multiError, blobPolicy));
    }
    
    /**
     * Append batch message.
     *
     * <p>Like Firebird, the batch buffer holds every stored message with the aligned message length, and the incoming messages take
     * the aligned length each except the last one.</p>
     *
     * @param connectionId connection ID
     * @param packet batch message packet
     * @throws InvalidBatchHandleException if batch statement does not exist
     * @throws BatchTooBigException if appending the message exceeds the batch buffer size
     */
    public void appendBatchMessage(final int connectionId, final FirebirdBatchMessageCommandPacket packet) {
        FirebirdBatchStatement batchStatement = getBatchStatement(connectionId, packet.getStatementHandle());
        ShardingSpherePreconditions.checkNotNull(batchStatement, () -> new InvalidBatchHandleException(packet.getStatementHandle()));
        long dataSize = batchStatement.getDataSize();
        long messagesSize = (packet.getMessageCount() - 1L) * batchStatement.getAlignedMessageLength() + batchStatement.getMessageLength();
        ShardingSpherePreconditions.checkState(dataSize + messagesSize <= batchStatement.getBufferSize(),
                () -> new BatchTooBigException(packet.getStatementHandle(), dataSize, messagesSize, batchStatement.getBufferSize()));
        for (List<Object> each : packet.readParameterValues(batchStatement.getColumnDescriptors())) {
            batchStatement.addParameterValues(each);
        }
    }
    
    /**
     * Append BLOB stream portion.
     *
     * <p>Like Firebird, the restored portion of length zero is accepted before the batch is checked for BLOB fields and BLOB policy.</p>
     *
     * @param connectionId connection ID
     * @param packet batch BLOB stream packet
     * @throws InvalidBatchHandleException if batch statement does not exist
     * @throws BatchWithoutBlobsException if batch message contains no BLOB fields
     * @throws InvalidBatchBlobPolicyException if BLOB policy of batch does not allow BLOB stream
     * @throws BatchTooBigException if the stream portion exceeds the batch buffer size
     */
    @HighFrequencyInvocation
    void appendBlobStream(final int connectionId, final FirebirdBatchBlobStreamCommandPacket packet) {
        FirebirdBatchStatement batchStatement = getBatchStatement(connectionId, packet.getStatementHandle());
        ShardingSpherePreconditions.checkNotNull(batchStatement, () -> new InvalidBatchHandleException(packet.getStatementHandle()));
        byte[] portion = batchStatement.readBlobStream(packet.getData(), packet.getLength());
        if (0 == portion.length) {
            return;
        }
        ShardingSpherePreconditions.checkState(batchStatement.hasBatchBlobIdColumn(), () -> new BatchWithoutBlobsException(packet.getStatementHandle()));
        ShardingSpherePreconditions.checkState(batchStatement.isBlobStreamAllowed(), () -> new InvalidBatchBlobPolicyException("addBlobStream"));
        batchStatement.appendBlobStream(portion);
    }
    
    /**
     * Set default BLOB parameter buffer.
     *
     * @param connectionId connection ID
     * @param packet batch set default BLOB parameter buffer packet
     * @throws InvalidBatchHandleException if batch statement does not exist
     * @throws BatchDefaultBpbChangeException if batch already contains BLOB stream data
     */
    void setDefaultBpb(final int connectionId, final FirebirdBatchSetBpbCommandPacket packet) {
        FirebirdBatchStatement batchStatement = getBatchStatement(connectionId, packet.getStatementHandle());
        ShardingSpherePreconditions.checkNotNull(batchStatement, () -> new InvalidBatchHandleException(packet.getStatementHandle()));
        batchStatement.getBlobStream().setDefaultBpb(packet.getBpb());
        ShardingSpherePreconditions.checkState(0L == batchStatement.getBlobStreamSize(), () -> new BatchDefaultBpbChangeException(packet.getStatementHandle()));
        batchStatement.setDefaultBpb(ByteBufUtil.getBytes(packet.getBpb()));
    }
    
    /**
     * Create temporary BLOBs of the BLOB buffer in the transaction and register them under their batch BLOB IDs, as Firebird does before executing batch messages,
     * and clear the BLOB buffer.
     *
     * @param batchStatement batch statement
     * @param connectionId connection ID
     * @param transactionId transaction ID of the batch execution
     * @param connectionCharsetId character set ID of the connection
     * @throws BatchSmallDataException if a BLOB header is incomplete
     * @throws BatchBlobBufferFormatException if a continuation has no BLOB to continue
     * @throws BatchBlobContinuationBpbException if a BLOB continuation contains a BLOB parameter buffer
     * @throws BatchBpbTooBigException if a BLOB parameter buffer is incomplete
     * @throws RepeatedBatchBlobIdException if batch BLOB ID is already registered
     * @throws BatchSegmentExceedsBlobException if a BLOB segment exceeds its BLOB
     * @throws BatchSegmentTooBigException if a BLOB segment is incomplete
     * @throws BlobFilterNotFoundException if Firebird has no BLOB filter for the sub type conversion of a BLOB parameter buffer
     * @throws BlobFilterWriteNotSupportedException if a BLOB parameter buffer requires an internal BLOB filter that does not support writing
     * @throws UnsupportedBlobFilterException if a BLOB parameter buffer requires an unsupported BLOB filter conversion
     * @throws TransliterationFailedException if BLOB data cannot be transliterated as its BLOB parameter buffer requires
     */
    @HighFrequencyInvocation
    void registerStreamBlobs(final FirebirdBatchStatement batchStatement, final int connectionId, final int transactionId, final int connectionCharsetId) {
        for (FirebirdBatchStreamBlob each : batchStatement.readStreamBlobs()) {
            byte[] data = each.getData().toByteArray();
            Optional<UnaryOperator<byte[]>> blobFilter = FirebirdBlobParameterBufferUtils.findBlobFilter(each.getBpb(), connectionCharsetId);
            long blobId = FirebirdBlobIdGenerator.getInstance().nextBlobId(connectionId, blobFilter.isPresent());
            FirebirdBlobWriteCache.getInstance().registerClosedBlob(connectionId, blobId, transactionId, !each.isSegmented(), blobFilter.map(filter -> filter.apply(data)).orElse(data));
            batchStatement.getBlobIds().put(each.getBatchBlobId(), blobId);
        }
        batchStatement.clearBlobStream();
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
