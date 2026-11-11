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
import io.netty.util.ReferenceCountUtil;
import org.apache.shardingsphere.database.exception.core.exception.SQLDialectException;
import org.apache.shardingsphere.database.exception.core.exception.protocol.DatabaseProtocolException;
import org.apache.shardingsphere.database.protocol.codec.DatabasePacketCodecEngine;
import org.apache.shardingsphere.database.protocol.constant.CommonConstants;
import org.apache.shardingsphere.database.protocol.firebird.constant.FirebirdConstant;
import org.apache.shardingsphere.database.protocol.firebird.err.FirebirdErrorPacketFactory;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketFactory;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchBlobStream;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchBlobStreamCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchColumnDescriptor;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchCreateCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchMessageCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchRegistry;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchSetBPBCommandPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdParseBatchBlr;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.FirebirdAllocateStatementPacket;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.statement.FirebirdFreeStatementPacket;
import org.apache.shardingsphere.database.protocol.firebird.payload.FirebirdPacketPayload;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Database packet codec for Firebird.
 */
public final class FirebirdPacketCodecEngine implements DatabasePacketCodecEngine {
    
    private static final int MESSAGE_TYPE_LENGTH = 4;
    
    private final List<ByteBuf> pendingMessages = new LinkedList<>();
    
    private final Map<Integer, List<FirebirdBatchColumnDescriptor>> deferredBatchFormats = new HashMap<>(1, 1F);
    
    private final Map<Integer, FirebirdBatchBlobStream> batchBlobStreams = new HashMap<>(1, 1F);
    
    private FirebirdCommandPacketType pendingPacketType;
    
    private boolean batchCreateBarrierEnabled;
    
    private boolean awaitingBatchCreate;
    
    private ByteBuf pendingBatchCreate;
    
    @Override
    public boolean isValidHeader(final int readableBytes) {
        return awaitingBatchCreate ? readableBytes > 0 : readableBytes >= MESSAGE_TYPE_LENGTH;
    }
    
    @HighFrequencyInvocation
    @Override
    public void decode(final ChannelHandlerContext context, final ByteBuf in, final List<Object> out) {
        try {
            if (awaitingBatchCreate) {
                pendingMessages.add(in.readRetainedSlice(in.readableBytes()));
                return;
            }
            if (pendingMessages.isEmpty()) {
                pendingPacketType = FirebirdCommandPacketType.valueOf(in.getInt(in.readerIndex()));
                if (FirebirdCommandPacketType.ALLOCATE_STATEMENT == pendingPacketType) {
                    handleMultiPacket(context, in, out, FirebirdAllocateStatementPacket.getLength());
                    return;
                }
                if (FirebirdCommandPacketType.FREE_STATEMENT == pendingPacketType) {
                    discardFreedStatementIfComplete(in);
                    handleMultiPacket(context, in, out, FirebirdFreeStatementPacket.getLength());
                    return;
                }
            }
            addToBuffer(context, in, out);
            // CHECKSTYLE:OFF
        } catch (final RuntimeException ex) {
            // CHECKSTYLE:ON
            out.forEach(ReferenceCountUtil::release);
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
    
    private void handleMultiPacket(final ChannelHandlerContext context, final ByteBuf in, final List<Object> out, final int firstPacketLength) {
        out.add(in.readRetainedSlice(firstPacketLength));
        if (in.readableBytes() > MESSAGE_TYPE_LENGTH) {
            decode(context, in, out);
        }
    }
    
    private void addToBuffer(final ChannelHandlerContext context, final ByteBuf in, final List<Object> out) {
        ByteBuf buffer = mergePendingMessages(context, in);
        try {
            processPackets(context, buffer, out);
        } finally {
            if (buffer != in) {
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
            FirebirdCommandPacketType commandType = null == pendingPacketType ? FirebirdCommandPacketType.valueOf(buffer.getInt(buffer.readerIndex())) : pendingPacketType;
            if (FirebirdCommandPacketType.VOID == commandType) {
                buffer.skipBytes(MESSAGE_TYPE_LENGTH);
                continue;
            }
            int packetLength = findPacketLength(context, buffer, commandType, charset);
            if (packetLength < 0) {
                pendingPacketType = commandType;
                pendingMessages.add(buffer.readRetainedSlice(buffer.readableBytes()));
                if (!batchCreateBarrierEnabled && FirebirdCommandPacketType.BATCH_MSG != commandType) {
                    deferredBatchFormats.clear();
                }
                return;
            }
            if (FirebirdCommandPacketType.BATCH_CREATE == commandType && batchCreateBarrierEnabled) {
                awaitingBatchCreate = true;
                pendingPacketType = null;
                pendingBatchCreate = buffer.retainedSlice(buffer.readerIndex(), packetLength);
                out.add(buffer.readRetainedSlice(packetLength));
                if (buffer.isReadable()) {
                    pendingMessages.add(buffer.readRetainedSlice(buffer.readableBytes()));
                }
                return;
            }
            if (FirebirdCommandPacketType.BATCH_CREATE == commandType && buffer.readableBytes() > packetLength) {
                rememberBatchFormat(buffer, packetLength, charset);
            }
            trackBatchBlobStream(context, buffer, commandType, packetLength, charset);
            pendingPacketType = null;
            out.add(buffer.readRetainedSlice(packetLength));
        }
        if (!batchCreateBarrierEnabled) {
            deferredBatchFormats.clear();
        }
    }
    
    private int findPacketLength(final ChannelHandlerContext context, final ByteBuf buffer, final FirebirdCommandPacketType commandType, final Charset charset) {
        int readableBytes = buffer.readableBytes();
        ByteBuf slice = buffer.retainedSlice();
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
            return getBatchBlobStreamLength(context, buffer, payload);
        }
        List<FirebirdBatchColumnDescriptor> columnDescriptors = getDeferredBatchFormat(buffer, commandType);
        return null == columnDescriptors
                ? FirebirdCommandPacketFactory.getExpectedLength(commandType, payload,
                        context.channel().attr(FirebirdConstant.CONNECTION_PROTOCOL_VERSION).get(), context.channel().attr(FirebirdConstant.CURRENT_CONNECTION).get())
                : FirebirdBatchMessageCommandPacket.getLength(payload, columnDescriptors);
    }
    
    private int getBatchBlobStreamLength(final ChannelHandlerContext context, final ByteBuf buffer, final FirebirdPacketPayload payload) {
        int statementHandle = buffer.getInt(buffer.readerIndex() + MESSAGE_TYPE_LENGTH);
        FirebirdBatchBlobStream existing = batchBlobStreams.get(statementHandle);
        FirebirdBatchBlobStream blobStream = null == existing ? new FirebirdBatchBlobStream() : existing.copy();
        int result;
        try {
            result = FirebirdBatchBlobStreamCommandPacket.getLength(payload, blobStream);
        } catch (final SQLDialectException ex) {
            throw new InvalidBlobStreamException(ex);
        }
        if (result >= 0 && (null != existing || shouldRetainBatchState(context, statementHandle))) {
            batchBlobStreams.put(statementHandle, blobStream);
        }
        return result;
    }
    
    private List<FirebirdBatchColumnDescriptor> getDeferredBatchFormat(final ByteBuf buffer, final FirebirdCommandPacketType commandType) {
        return FirebirdCommandPacketType.BATCH_MSG == commandType && buffer.readableBytes() >= 8
                ? deferredBatchFormats.get(buffer.getInt(buffer.readerIndex() + MESSAGE_TYPE_LENGTH))
                : null;
    }
    
    private void rememberBatchFormat(final ByteBuf buffer, final int packetLength, final Charset charset) {
        FirebirdBatchCreateCommandPacket packet = new FirebirdBatchCreateCommandPacket(new FirebirdPacketPayload(buffer.slice(buffer.readerIndex(), packetLength), charset));
        ByteBuf batchBlr = packet.getBatchBlr();
        deferredBatchFormats.put(packet.getStatementHandle(), FirebirdParseBatchBlr.parse(batchBlr, batchBlr.readableBytes()).getFields());
    }
    
    private void trackBatchBlobStream(final ChannelHandlerContext context, final ByteBuf buffer, final FirebirdCommandPacketType commandType, final int packetLength, final Charset charset) {
        if (FirebirdCommandPacketType.BATCH_CREATE == commandType) {
            batchBlobStreams.put(buffer.getInt(buffer.readerIndex() + MESSAGE_TYPE_LENGTH), new FirebirdBatchBlobStream());
        } else if (FirebirdCommandPacketType.BATCH_SET_BPB == commandType) {
            trackDefaultBPB(context, buffer, packetLength, charset);
        } else if (FirebirdCommandPacketType.FREE_STATEMENT == commandType) {
            discardFreedStatement(buffer);
        }
    }
    
    private void trackDefaultBPB(final ChannelHandlerContext context, final ByteBuf buffer, final int packetLength, final Charset charset) {
        FirebirdBatchSetBPBCommandPacket packet = new FirebirdBatchSetBPBCommandPacket(new FirebirdPacketPayload(buffer.slice(buffer.readerIndex(), packetLength), charset));
        FirebirdBatchBlobStream blobStream = batchBlobStreams.get(packet.getStatementHandle());
        if (null == blobStream && shouldRetainBatchState(context, packet.getStatementHandle())) {
            blobStream = new FirebirdBatchBlobStream();
            batchBlobStreams.put(packet.getStatementHandle(), blobStream);
        }
        if (null == blobStream) {
            blobStream = new FirebirdBatchBlobStream();
        }
        blobStream.setDefaultBPB(packet.getBpb());
    }
    
    private boolean shouldRetainBatchState(final ChannelHandlerContext context, final int statementHandle) {
        return !batchCreateBarrierEnabled || hasBatchStatement(context, statementHandle);
    }
    
    private boolean hasBatchStatement(final ChannelHandlerContext context, final int statementHandle) {
        Integer connectionId = context.channel().attr(FirebirdConstant.CURRENT_CONNECTION).get();
        return null != connectionId && null != FirebirdBatchRegistry.getInstance().getBatchStatement(connectionId, statementHandle);
    }
    
    private void discardFreedStatementIfComplete(final ByteBuf buffer) {
        if (buffer.readableBytes() >= FirebirdFreeStatementPacket.getLength()) {
            discardFreedStatement(buffer);
        }
    }
    
    private void discardFreedStatement(final ByteBuf buffer) {
        int option = buffer.getInt(buffer.readerIndex() + MESSAGE_TYPE_LENGTH * 2);
        if (FirebirdFreeStatementPacket.DROP == option) {
            int statementHandle = buffer.getInt(buffer.readerIndex() + MESSAGE_TYPE_LENGTH);
            batchBlobStreams.remove(statementHandle);
            deferredBatchFormats.remove(statementHandle);
        }
    }
    
    private void resetState() {
        pendingMessages.forEach(ByteBuf::release);
        pendingMessages.clear();
        pendingPacketType = null;
        if (null != pendingBatchCreate) {
            pendingBatchCreate.release();
            pendingBatchCreate = null;
        }
    }
    
    /**
     * Coordinate batch creation with the frontend command executor.
     */
    public void enableBatchCreateBarrier() {
        batchCreateBarrierEnabled = true;
    }
    
    /**
     * Resume decoding after a batch creation command has completed.
     *
     * @param context channel handler context
     * @param statementHandle statement handle
     * @param created whether a new batch was created
     * @param statementExists whether the statement handle was valid
     */
    public void completeBatchCreate(final ChannelHandlerContext context, final int statementHandle, final boolean created, final boolean statementExists) {
        if (created) {
            batchBlobStreams.put(statementHandle, new FirebirdBatchBlobStream());
        }
        Integer connectionId = context.channel().attr(FirebirdConstant.CURRENT_CONNECTION).get();
        if (created || !statementExists || null != FirebirdBatchRegistry.getInstance().getBatchStatement(connectionId, statementHandle)) {
            deferredBatchFormats.remove(statementHandle);
        } else if (null != pendingBatchCreate) {
            try {
                rememberBatchFormat(pendingBatchCreate, pendingBatchCreate.readableBytes(), context.channel().attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).get());
            } catch (final DatabaseProtocolException | IllegalArgumentException | IndexOutOfBoundsException ignored) {
                deferredBatchFormats.remove(statementHandle);
            }
        }
        if (null != pendingBatchCreate) {
            pendingBatchCreate.release();
            pendingBatchCreate = null;
        }
        awaitingBatchCreate = false;
        if (!pendingMessages.isEmpty()) {
            CompositeByteBuf buffer = context.alloc().compositeBuffer(pendingMessages.size());
            buffer.addComponents(true, pendingMessages);
            pendingMessages.clear();
            context.pipeline().fireChannelRead(buffer);
        }
    }
    
    /**
     * Discard undecoded data when the frontend channel closes.
     */
    public void discardPendingMessages() {
        awaitingBatchCreate = false;
        resetState();
        deferredBatchFormats.clear();
        batchBlobStreams.clear();
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
            out.resetWriterIndex();
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
