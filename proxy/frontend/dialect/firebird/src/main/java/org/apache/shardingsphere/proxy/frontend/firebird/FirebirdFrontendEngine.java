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

package org.apache.shardingsphere.proxy.frontend.firebird;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import lombok.Getter;
import org.apache.shardingsphere.database.exception.core.exception.SQLDialectException;
import org.apache.shardingsphere.database.exception.core.exception.transaction.InTransactionException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchAlreadyOpenedException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBlobBufferFormatException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBlobContinuationBpbException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchBpbTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchDefaultBpbChangeException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchParametersRequiredException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSegmentExceedsBlobException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSegmentTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchSmallDataException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchTooBigException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BatchWithoutBlobsException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BlobFilterNotFoundException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchBlobPolicyException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchHandleException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchMessageFormatException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBatchParameterVersionException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidBpbVersionException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.InvalidClumpletStructureException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.ParameterConversionException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.RepeatedBatchBlobIdException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.TransliterationFailedException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.UnknownBatchBlobIdException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.UnsupportedBlobFilterException;
import org.apache.shardingsphere.database.protocol.codec.DatabasePacketCodecEngine;
import org.apache.shardingsphere.database.protocol.firebird.codec.FirebirdPacketCodecEngine;
import org.apache.shardingsphere.database.protocol.firebird.constant.FirebirdConstant;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.FirebirdCommandPacketType;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchRegistry;
import org.apache.shardingsphere.database.protocol.firebird.packet.command.query.batch.FirebirdBatchStatement;
import org.apache.shardingsphere.database.protocol.firebird.packet.generic.FirebirdGenericResponsePacket;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.authentication.AuthenticationEngine;
import org.apache.shardingsphere.proxy.frontend.event.WriteCompleteEvent;
import org.apache.shardingsphere.proxy.frontend.firebird.authentication.FirebirdAuthenticationEngine;
import org.apache.shardingsphere.proxy.frontend.firebird.command.FirebirdCommandExecuteEngine;
import org.apache.shardingsphere.proxy.frontend.firebird.resource.FirebirdConnectionResourceManager;
import org.apache.shardingsphere.proxy.frontend.netty.ProxyFlowControlHandler;
import org.apache.shardingsphere.proxy.frontend.spi.DatabaseProtocolFrontendEngine;
import org.firebirdsql.gds.ISCConstants;

import java.util.Arrays;
import java.util.Collection;

/**
 * Frontend engine for Firebird.
 */
@Getter
public final class FirebirdFrontendEngine implements DatabaseProtocolFrontendEngine {
    
    private static final Collection<Class<? extends SQLDialectException>> BATCH_EXCEPTION_TYPES = Arrays.asList(
            BatchAlreadyOpenedException.class,
            BatchBlobBufferFormatException.class,
            BatchBlobContinuationBpbException.class,
            BatchBpbTooBigException.class,
            BatchDefaultBpbChangeException.class,
            BatchParametersRequiredException.class,
            BatchSegmentExceedsBlobException.class,
            BatchSegmentTooBigException.class,
            BatchSmallDataException.class,
            BatchTooBigException.class,
            BatchWithoutBlobsException.class,
            BlobFilterNotFoundException.class,
            InvalidBatchBlobPolicyException.class,
            InvalidBatchHandleException.class,
            InvalidBatchMessageFormatException.class,
            InvalidBatchParameterVersionException.class,
            InvalidBpbVersionException.class,
            InvalidClumpletStructureException.class,
            ParameterConversionException.class,
            RepeatedBatchBlobIdException.class,
            TransliterationFailedException.class,
            UnknownBatchBlobIdException.class,
            UnsupportedBlobFilterException.class);
    
    private final AuthenticationEngine authenticationEngine = new FirebirdAuthenticationEngine();
    
    private final FirebirdCommandExecuteEngine commandExecuteEngine = new FirebirdCommandExecuteEngine();
    
    private final DatabasePacketCodecEngine codecEngine = new FirebirdPacketCodecEngine();
    
    @Override
    public void initChannel(final Channel channel) {
        FirebirdPacketCodecEngine firebirdCodec = (FirebirdPacketCodecEngine) codecEngine;
        firebirdCodec.enableBatchCreateBarrier();
        BatchCreateBarrier barrier = new BatchCreateBarrier(firebirdCodec);
        String flowControlHandler = ProxyFlowControlHandler.class.getSimpleName();
        channel.pipeline().addBefore(flowControlHandler, "FirebirdBatchCreateCompletion", barrier.completionHandler());
        channel.pipeline().addAfter(flowControlHandler, "FirebirdBatchCreateDelivery", barrier.deliveryHandler());
    }
    
    @Override
    public void release(final ConnectionSession connectionSession) {
        FirebirdConnectionResourceManager.getInstance().unregisterConnection(connectionSession.getConnectionId());
    }
    
    @Override
    public void handleException(final ConnectionSession connectionSession, final Exception exception) {
        if (connectionSession.getTransactionStatus().isInTransaction() && !connectionSession.getConnectionContext().getTransactionContext().isExceptionOccur()
                && !(exception instanceof InTransactionException) && !isBatchException(exception)) {
            connectionSession.getConnectionContext().getTransactionContext().setExceptionOccur(true);
        }
    }
    
    private boolean isBatchException(final Exception exception) {
        return BATCH_EXCEPTION_TYPES.stream().anyMatch(each -> each.isInstance(exception));
    }
    
    @Override
    public String getDatabaseType() {
        return "Firebird";
    }
    
    private static final class BatchCreateBarrier {
        
        private final FirebirdPacketCodecEngine codec;
        
        private boolean active;
        
        private boolean delivering;
        
        private boolean synchronousResponse;
        
        private boolean invalidStatement;
        
        private boolean closed;
        
        private int connectionId;
        
        private int statementHandle;
        
        private FirebirdBatchStatement previousBatch;
        
        BatchCreateBarrier(final FirebirdPacketCodecEngine codec) {
            this.codec = codec;
        }
        
        private ChannelInboundHandlerAdapter completionHandler() {
            return new ChannelInboundHandlerAdapter() {
                
                @Override
                public void userEventTriggered(final ChannelHandlerContext context, final Object event) {
                    if (event instanceof WriteCompleteEvent && active) {
                        complete(context);
                    }
                    context.fireUserEventTriggered(event);
                }
                
                @Override
                public void channelInactive(final ChannelHandlerContext context) {
                    close();
                    context.fireChannelInactive();
                }
                
                @Override
                public void handlerRemoved(final ChannelHandlerContext context) {
                    remove(context);
                }
            };
        }
        
        private ChannelDuplexHandler deliveryHandler() {
            return new ChannelDuplexHandler() {
                
                @Override
                public void channelRead(final ChannelHandlerContext context, final Object message) {
                    deliver(context, message);
                }
                
                @Override
                public void write(final ChannelHandlerContext context, final Object message, final ChannelPromise promise) {
                    if (active && message instanceof FirebirdGenericResponsePacket && ISCConstants.isc_bad_stmt_handle == ((FirebirdGenericResponsePacket) message).getErrorCode()) {
                        invalidStatement = true;
                    }
                    if (delivering) {
                        synchronousResponse = true;
                    }
                    context.write(message, promise);
                }
                
                @Override
                public void handlerRemoved(final ChannelHandlerContext context) {
                    remove(context);
                }
            };
        }
        
        private void deliver(final ChannelHandlerContext context, final Object message) {
            if (!(message instanceof ByteBuf) || FirebirdCommandPacketType.BATCH_CREATE.getValue() != ((ByteBuf) message).getInt(((ByteBuf) message).readerIndex())) {
                context.fireChannelRead(message);
                return;
            }
            ByteBuf packet = (ByteBuf) message;
            connectionId = context.channel().attr(FirebirdConstant.CURRENT_CONNECTION).get();
            statementHandle = packet.getInt(packet.readerIndex() + Integer.BYTES);
            previousBatch = FirebirdBatchRegistry.getInstance().getBatchStatement(connectionId, statementHandle);
            active = true;
            delivering = true;
            synchronousResponse = false;
            invalidStatement = false;
            try {
                context.fireChannelRead(message);
            } finally {
                delivering = false;
            }
            if (synchronousResponse && active) {
                complete(context);
            }
        }
        
        private void complete(final ChannelHandlerContext context) {
            boolean created = null == previousBatch && null != FirebirdBatchRegistry.getInstance().getBatchStatement(connectionId, statementHandle);
            int completedHandle = statementHandle;
            boolean statementExists = !invalidStatement;
            active = false;
            context.executor().execute(() -> {
                if (!closed && context.channel().isActive()) {
                    codec.completeBatchCreate(context, completedHandle, created, statementExists);
                }
            });
        }
        
        private void close() {
            if (!closed) {
                closed = true;
                active = false;
                codec.discardPendingMessages();
            }
        }
        
        private void remove(final ChannelHandlerContext context) {
            close();
            if (context.channel().isActive()) {
                context.close();
            }
        }
    }
}
