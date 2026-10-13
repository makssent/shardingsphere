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

package org.apache.shardingsphere.database.protocol.firebird.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.apache.shardingsphere.database.exception.core.exception.SQLDialectException;
import org.apache.shardingsphere.database.protocol.codec.DatabasePacketCodecEngine;
import org.apache.shardingsphere.database.protocol.constant.CommonConstants;
import org.apache.shardingsphere.database.protocol.firebird.constant.FirebirdConstant;
import org.apache.shardingsphere.database.protocol.firebird.err.FirebirdErrorPacketFactory;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketFactory;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchBlobStream;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchBlobStreamCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchColumnDescriptor;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchMessageCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchSetBpbCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdParseBatchBlr;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.FirebirdFreeStatementPacket;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.firebirdsql.jaybird.fb.constants.BatchItems;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Database packet codec for Firebird.
 */
public final class FirebirdPacketCodecEngine implements DatabasePacketCodecEngine {
    
    private static final int MESSAGE_TYPE_LENGTH = 4;
    
    private static final int ALLOCATE_STATEMENT_REQUEST_PAYLOAD_LENGTH = MESSAGE_TYPE_LENGTH + 4;
    
    private static final int FREE_STATEMENT_REQUEST_PAYLOAD_LENGTH = MESSAGE_TYPE_LENGTH + 8;
    
    private static final int RELEASE_BATCH_FREE_STATEMENT_OPTIONS = FirebirdFreeStatementPacket.CLOSE | FirebirdFreeStatementPacket.DROP | FirebirdFreeStatementPacket.UNPREPARE;
    
    private final List<ByteBuf> pendingMessages = new LinkedList<>();
    
    private final Map<Integer, List<FirebirdBatchColumnDescriptor>> deferredBatchFormats = new HashMap<>(1, 1F);
    
    private final Map<Integer, FirebirdBatchBlobStream> batchBlobStreams = new HashMap<>(1, 1F);
    
    private final Set<Integer> openBatchStatements = new HashSet<>(1, 1F);
    
    private FirebirdCommandPacketType pendingPacketType;
    
    @Override
    public boolean isValidHeader(final int readableBytes) {
        return readableBytes >= MESSAGE_TYPE_LENGTH;
    }
    
    @HighFrequencyInvocation
    @Override
    public void decode(final ChannelHandlerContext context, final ByteBuf in, final List<Object> out) {
        try {
            if (pendingMessages.isEmpty()) {
                int type = in.getInt(in.readerIndex());
                pendingPacketType = FirebirdCommandPacketType.valueOf(type);
                if (pendingPacketType == FirebirdCommandPacketType.ALLOCATE_STATEMENT) {
                    handleMultiPacket(context, in, out, ALLOCATE_STATEMENT_REQUEST_PAYLOAD_LENGTH);
                    return;
                } else if (pendingPacketType == FirebirdCommandPacketType.FREE_STATEMENT) {
                    handleMultiPacket(context, in, out, FREE_STATEMENT_REQUEST_PAYLOAD_LENGTH);
                    return;
                }
            }
            addToBuffer(context, in, out);
            // CHECKSTYLE:OFF
        } catch (final RuntimeException ex) {
            // CHECKSTYLE:ON
            for (Object each : out) {
                if (each instanceof ByteBuf) {
                    ((ByteBuf) each).release();
                }
            }
            out.clear();
            resetState();
            in.skipBytes(in.readableBytes());
            if (ex instanceof InvalidBlobStreamException) {
                context.channel().close();
            } else {
                context.channel().writeAndFlush(FirebirdErrorPacketFactory.newInstance(ex)).addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
    
    private void resetState() {
        for (ByteBuf each : pendingMessages) {
            each.release();
        }
        pendingMessages.clear();
        pendingPacketType = null;
    }
    
    private void handleMultiPacket(final ChannelHandlerContext context, final ByteBuf in, final List<Object> out, final int firstPacketLength) {
        if (FirebirdCommandPacketType.FREE_STATEMENT == pendingPacketType) {
            freeStatement(in);
        }
        out.add(in.readRetainedSlice(firstPacketLength));
        if (in.readableBytes() > MESSAGE_TYPE_LENGTH) {
            decode(context, in, out);
        }
    }
    
    private void addToBuffer(final ChannelHandlerContext context, final ByteBuf in, final List<Object> out) {
        ByteBuf buffer = mergePendingMessages(context, in);
        boolean shouldRelease = buffer != in;
        try {
            processPackets(context, buffer, out);
        } finally {
            if (shouldRelease) {
                buffer.release();
            }
        }
    }
    
    private ByteBuf mergePendingMessages(final ChannelHandlerContext context, final ByteBuf in) {
        if (pendingMessages.isEmpty()) {
            return in;
        }
        CompositeByteBuf result = context.alloc().compositeBuffer(pendingMessages.size() + 1);
        result.addComponents(true, pendingMessages);
        pendingMessages.clear();
        result.addComponent(true, in.readRetainedSlice(in.readableBytes()));
        return result;
    }
    
    private void processPackets(final ChannelHandlerContext context, final ByteBuf buffer, final List<Object> out) {
        Charset charset = context.channel().attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).get();
        while (buffer.isReadable()) {
            if (!isValidHeader(buffer.readableBytes())) {
                pendingMessages.add(buffer.readRetainedSlice(buffer.readableBytes()));
                return;
            }
            int readerIndex = buffer.readerIndex();
            FirebirdCommandPacketType commandType = (null == pendingPacketType) ? FirebirdCommandPacketType.valueOf(buffer.getInt(readerIndex)) : pendingPacketType;
            if (FirebirdCommandPacketType.VOID == commandType) {
                buffer.skipBytes(MESSAGE_TYPE_LENGTH);
                continue;
            }
            int packetLength = findPacketLength(context, buffer, commandType, charset);
            if (packetLength < 0) {
                pendingPacketType = commandType;
                pendingMessages.add(buffer.readRetainedSlice(buffer.readableBytes()));
                if (FirebirdCommandPacketType.BATCH_MSG != commandType) {
                    deferredBatchFormats.clear();
                }
                return;
            }
            trackBatchState(buffer, commandType, packetLength, charset);
            pendingPacketType = null;
            out.add(buffer.readRetainedSlice(packetLength));
        }
        deferredBatchFormats.clear();
    }
    
    private int findPacketLength(final ChannelHandlerContext context, final ByteBuf buffer, final FirebirdCommandPacketType commandType, final Charset charset) {
        int readerIndex = buffer.readerIndex();
        int readableBytes = buffer.readableBytes();
        ByteBuf slice = buffer.retainedSlice(readerIndex, readableBytes);
        try {
            FirebirdPacketPayload payload = new FirebirdPacketPayload(slice, charset);
            int expectedLength = getExpectedLength(context, buffer, commandType, payload);
            if (expectedLength < 0) {
                return -1;
            }
            if (0 == expectedLength) {
                return readableBytes;
            }
            return readableBytes >= expectedLength ? expectedLength : -1;
        } catch (final IndexOutOfBoundsException ex) {
            return -1;
        } finally {
            slice.release();
        }
    }
    
    private int getExpectedLength(final ChannelHandlerContext context, final ByteBuf buffer, final FirebirdCommandPacketType commandType, final FirebirdPacketPayload payload) {
        if (FirebirdCommandPacketType.BATCH_BLOB_STREAM == commandType) {
            return getBatchBlobStreamLength(buffer, payload);
        }
        List<FirebirdBatchColumnDescriptor> columnDescriptors = getDeferredBatchFormat(buffer, commandType);
        return null == columnDescriptors
                ? FirebirdCommandPacketFactory.getExpectedLength(commandType, payload,
                        context.channel().attr(FirebirdConstant.CONNECTION_PROTOCOL_VERSION).get(), context.channel().attr(FirebirdConstant.CURRENT_CONNECTION).get())
                : FirebirdBatchMessageCommandPacket.getLength(payload, columnDescriptors);
    }
    
    private int getBatchBlobStreamLength(final ByteBuf buffer, final FirebirdPacketPayload payload) {
        int statementHandle = buffer.getInt(buffer.readerIndex() + MESSAGE_TYPE_LENGTH);
        FirebirdBatchBlobStream blobStream = batchBlobStreams.computeIfAbsent(statementHandle, key -> new FirebirdBatchBlobStream()).copy();
        int result;
        try {
            result = FirebirdBatchBlobStreamCommandPacket.getLength(payload, blobStream);
        } catch (final SQLDialectException ex) {
            throw new InvalidBlobStreamException(ex);
        }
        if (result >= 0) {
            batchBlobStreams.put(statementHandle, blobStream);
        }
        return result;
    }
    
    private List<FirebirdBatchColumnDescriptor> getDeferredBatchFormat(final ByteBuf buffer, final FirebirdCommandPacketType commandType) {
        return FirebirdCommandPacketType.BATCH_MSG == commandType && buffer.readableBytes() >= 8
                ? deferredBatchFormats.get(buffer.getInt(buffer.readerIndex() + MESSAGE_TYPE_LENGTH))
                : null;
    }
    
    private void trackBatchState(final ByteBuf buffer, final FirebirdCommandPacketType commandType, final int packetLength, final Charset charset) {
        if (FirebirdCommandPacketType.BATCH_CREATE == commandType) {
            createBatch(new FirebirdPacketPayload(buffer.slice(buffer.readerIndex(), packetLength), charset));
        } else if (FirebirdCommandPacketType.BATCH_SET_BPB == commandType) {
            FirebirdBatchSetBpbCommandPacket packet = new FirebirdBatchSetBpbCommandPacket(new FirebirdPacketPayload(buffer.slice(buffer.readerIndex(), packetLength), charset));
            batchBlobStreams.computeIfAbsent(packet.getStatementHandle(), key -> new FirebirdBatchBlobStream()).setDefaultBpb(packet.getBpb());
        } else if (FirebirdCommandPacketType.BATCH_RLS == commandType) {
            openBatchStatements.remove(buffer.getInt(buffer.readerIndex() + MESSAGE_TYPE_LENGTH));
        } else if (FirebirdCommandPacketType.FREE_STATEMENT == commandType) {
            freeStatement(buffer);
        }
    }
    
    /**
     * Track {@code op_batch_create} in the order of checks of Firebird {@code batch_create}.
     *
     * <p>Firebird rejects a batch of a statement with an open batch before it changes anything, and a missing or invalid message format,
     * a message length different from it and a batch parameters block of another version before it resets the BLOB stream state;
     * otherwise it resets the BLOB stream state, and the batch is open if the engine creates it, which requires message parameters.
     * Batch messages are decoded with the message format of the statement, so the message format parsed from the BLR is used for the messages of the same read
     * even if the batch is rejected for its message length or batch parameters block.
     * Statement handle and the other items of the batch parameters block are not checked here: a batch rejected only for them is taken as open.</p>
     *
     * @param payload payload of {@code op_batch_create}
     */
    private void createBatch(final FirebirdPacketPayload payload) {
        payload.skipReserved(MESSAGE_TYPE_LENGTH);
        int statementHandle = payload.readInt4();
        if (openBatchStatements.contains(statementHandle)) {
            return;
        }
        ByteBuf batchBlr = payload.readBuffer();
        long messageLength = payload.readInt4Unsigned();
        ByteBuf batchParametersBuffer = payload.readBuffer();
        Optional<FirebirdParseBatchBlr> messageFormat = findMessageFormat(batchBlr);
        if (!messageFormat.isPresent()) {
            return;
        }
        deferredBatchFormats.put(statementHandle, messageFormat.get().getFields());
        if (messageLength != messageFormat.get().getMessageLength() || !isBatchParametersVersionAccepted(batchParametersBuffer)) {
            return;
        }
        batchBlobStreams.put(statementHandle, new FirebirdBatchBlobStream());
        if (!messageFormat.get().getFields().isEmpty()) {
            openBatchStatements.add(statementHandle);
        }
    }
    
    private Optional<FirebirdParseBatchBlr> findMessageFormat(final ByteBuf batchBlr) {
        try {
            return Optional.of(FirebirdParseBatchBlr.parse(batchBlr, batchBlr.readableBytes()));
        } catch (final IllegalArgumentException | IndexOutOfBoundsException ex) {
            return Optional.empty();
        }
    }
    
    private boolean isBatchParametersVersionAccepted(final ByteBuf batchParametersBuffer) {
        return !batchParametersBuffer.isReadable() || BatchItems.BATCH_VERSION_1 == batchParametersBuffer.getByte(batchParametersBuffer.readerIndex());
    }
    
    /**
     * Track {@code op_free_statement}: Firebird releases the batch of the statement on close, drop and unprepare, and drops the statement with its BLOB stream state.
     *
     * @param buffer buffer starting at {@code op_free_statement}
     */
    private void freeStatement(final ByteBuf buffer) {
        int statementHandle = buffer.getInt(buffer.readerIndex() + MESSAGE_TYPE_LENGTH);
        int option = buffer.getInt(buffer.readerIndex() + MESSAGE_TYPE_LENGTH + 4);
        if (0 != (option & RELEASE_BATCH_FREE_STATEMENT_OPTIONS)) {
            openBatchStatements.remove(statementHandle);
        }
        if (0 != (option & FirebirdFreeStatementPacket.DROP)) {
            batchBlobStreams.remove(statementHandle);
        }
    }
    
    @Override
    public void encode(final ChannelHandlerContext context, final DatabasePacket message, final ByteBuf out) {
        FirebirdPacketPayload payload = new FirebirdPacketPayload(out, context.channel().attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).get());
        Integer connectionId = context.channel().attr(FirebirdConstant.CURRENT_CONNECTION).get();
        if (null != connectionId) {
            payload.setConnectionId(connectionId);
        }
        try {
            message.write(payload);
            // CHECKSTYLE:OFF
        } catch (final RuntimeException ex) {
            // CHECKSTYLE:ON
            payload.getByteBuf().resetWriterIndex();
            FirebirdErrorPacketFactory.newInstance(ex).write(payload);
        }
    }
    
    @Override
    public FirebirdPacketPayload createPacketPayload(final ByteBuf message, final Charset charset) {
        return new FirebirdPacketPayload(message, charset);
    }
    
    /**
     * Invalid stream portion of {@code op_batch_blob_stream}: Firebird fails to decode the packet in {@code xdr_blob_stream} and closes the connection without response.
     */
    private static final class InvalidBlobStreamException extends RuntimeException {
        
        private static final long serialVersionUID = 3197564018632745511L;
        
        InvalidBlobStreamException(final SQLDialectException cause) {
            super(cause);
        }
    }
}
