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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.database.exception.core.exception.SQLDialectException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BlobFilterNotFoundException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.BlobFilterWriteNotSupportedException;
import org.apache.shardingsphere.database.exception.firebird.exception.protocol.UnsupportedBlobFilterException;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.exception.ShardingSpherePreconditions;
import org.firebirdsql.encodings.EncodingDefinition;
import org.firebirdsql.encodings.EncodingFactory;
import org.firebirdsql.gds.ISCConstants;

import java.nio.charset.Charset;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * BLOB parameter buffer utility class for Firebird.
 */
@HighFrequencyInvocation
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FirebirdBlobParameterBufferUtils {
    
    private static final int INTERNAL_FILTER_COUNT = 10;
    
    private static final int BLOB_SUB_TYPE_RANGES = 4;
    
    private static final int BLOB_SUB_TYPE_RUNTIME = 5;
    
    private static final byte[] NO_DATA = new byte[0];
    
    /**
     * Find the BLOB filter through which Firebird writes a BLOB created with the BLOB parameter buffer.
     *
     * <p>Mirrors {@code gds__parse_bpb2} and the filter decision of {@code blb::create2}, which replaces {@code CS_dynamic} with the character set of the connection.
     * Firebird transliterates text between two different character sets other than {@code NONE} and {@code BINARY}; other BLOB filters are not supported.
     * Firebird has internal BLOB filters from its sub types other than ranges to text: the filter of sub type 5 ({@code filter_runtime}) accepts the data without writing it,
     * and the other internal filters answer the creation of the BLOB with {@code isc_uns_ext}; other sub type conversions fail as in a database without declared BLOB filters.
     * If the character set of the connection is unknown, it is passed as {@code CS_dynamic}, which has no Java character set.
     * A BLOB written through a BLOB filter is created on top of another temporary BLOB.</p>
     *
     * @param bpb BLOB parameter buffer
     * @param connectionCharsetId character set ID of the connection
     * @return BLOB filter that turns written data into stored data, or empty if Firebird stores the written data unchanged
     * @throws BlobFilterNotFoundException if Firebird has no BLOB filter for the sub type conversion
     * @throws BlobFilterWriteNotSupportedException if an internal BLOB filter of Firebird does not support writing
     * @throws UnsupportedBlobFilterException if Firebird converts the data with a character set without Java character set
     */
    public static Optional<UnaryOperator<byte[]>> findBlobFilter(final byte[] bpb, final int connectionCharsetId) {
        BlobParameters parameters = new BlobParameters(bpb);
        if (ISCConstants.BLOB_SUB_TYPE_TEXT == parameters.targetType && BLOB_SUB_TYPE_RUNTIME == parameters.sourceType) {
            return Optional.of(data -> NO_DATA);
        }
        if (0 != parameters.targetType && parameters.sourceType != parameters.targetType) {
            checkSubTypeConversion(parameters.sourceType, parameters.targetType, parameters.targetCharset);
            return Optional.empty();
        }
        if (ISCConstants.BLOB_SUB_TYPE_TEXT != parameters.targetType) {
            return Optional.empty();
        }
        return createTextTransliterator(parameters.sourceCharset, parameters.targetCharset, connectionCharsetId).map(each -> each::put);
    }
    
    private static void checkSubTypeConversion(final int sourceType, final int targetType, final int targetCharset) {
        ShardingSpherePreconditions.checkState(ISCConstants.BLOB_SUB_TYPE_TEXT == targetType && (ISCConstants.BLOB_SUB_TYPE_BINARY == sourceType || ISCConstants.CS_BINARY == targetCharset),
                () -> createFilterException(sourceType, targetType));
    }
    
    private static SQLDialectException createFilterException(final int sourceType, final int targetType) {
        return ISCConstants.BLOB_SUB_TYPE_TEXT == targetType && hasInternalTextFilter(sourceType)
                ? new BlobFilterWriteNotSupportedException(sourceType)
                : new BlobFilterNotFoundException(sourceType, targetType);
    }
    
    /**
     * Judge whether Firebird has an internal BLOB filter from the sub type to text.
     *
     * @param subType BLOB sub type
     * @return whether Firebird has an internal BLOB filter from the sub type to text
     */
    static boolean hasInternalTextFilter(final int subType) {
        return subType >= 0 && subType < INTERNAL_FILTER_COUNT && BLOB_SUB_TYPE_RANGES != subType;
    }
    
    /**
     * Create the transliterator of the text BLOB filter of Firebird.
     *
     * <p>Firebird transliterates text between two different character sets other than {@code NONE} and {@code BINARY}.</p>
     *
     * @param sourceCharsetId character set ID of the source text
     * @param targetCharsetId character set ID of the target text
     * @param connectionCharsetId character set ID of the connection
     * @return transliterator, or empty if Firebird keeps the text unchanged
     * @throws UnsupportedBlobFilterException if Firebird converts the text with a character set without Java character set
     */
    static Optional<FirebirdBlobTransliterator> createTextTransliterator(final int sourceCharsetId, final int targetCharsetId, final int connectionCharsetId) {
        int actualSourceCharsetId = getActualCharset(sourceCharsetId, connectionCharsetId);
        int actualTargetCharsetId = getActualCharset(targetCharsetId, connectionCharsetId);
        if (actualSourceCharsetId == actualTargetCharsetId || isPlainCharset(actualSourceCharsetId) || isPlainCharset(actualTargetCharsetId)) {
            return Optional.empty();
        }
        return Optional.of(new FirebirdBlobTransliterator(getJavaCharset(actualSourceCharsetId), getJavaCharset(actualTargetCharsetId)));
    }
    
    private static boolean isPlainCharset(final int charsetId) {
        return ISCConstants.CS_NONE == charsetId || ISCConstants.CS_BINARY == charsetId;
    }
    
    private static Charset getJavaCharset(final int charsetId) {
        Optional<Charset> result = findJavaCharset(charsetId);
        ShardingSpherePreconditions.checkState(result.isPresent(), UnsupportedBlobFilterException::new);
        return result.get();
    }
    
    /**
     * Get the actual character set: Firebird replaces {@code CS_dynamic} with the character set of the connection.
     *
     * @param charsetId character set ID
     * @param connectionCharsetId character set ID of the connection
     * @return actual character set ID
     */
    static int getActualCharset(final int charsetId, final int connectionCharsetId) {
        return ISCConstants.CS_dynamic == charsetId ? connectionCharsetId : charsetId;
    }
    
    /**
     * Find the Java character set of a real Firebird character set.
     *
     * @param charsetId character set ID
     * @return Java character set, or empty for {@code NONE}, {@code BINARY}, {@code CS_dynamic} and character sets without Java character set
     */
    static Optional<Charset> findJavaCharset(final int charsetId) {
        if (isPlainCharset(charsetId) || ISCConstants.CS_dynamic == charsetId) {
            return Optional.empty();
        }
        EncodingDefinition encodingDefinition = EncodingFactory.getPlatformDefault().getEncodingDefinitionByCharacterSetId(charsetId);
        return null == encodingDefinition || encodingDefinition.isInformationOnly() ? Optional.empty() : Optional.of(encodingDefinition.getJavaCharset());
    }
    
    /**
     * Sub types and character sets of a BLOB parameter buffer of version 1.
     */
    @HighFrequencyInvocation
    private static final class BlobParameters {
        
        private final int sourceType;
        
        private final int targetType;
        
        private final int sourceCharset;
        
        private final int targetCharset;
        
        BlobParameters(final byte[] bpb) {
            int[] values = new int[ISCConstants.isc_bpb_target_interp + 1];
            int index = 1;
            while (index + 1 < bpb.length) {
                int tag = bpb[index];
                int valueLength = Math.min(bpb[index + 1] & 0xFF, bpb.length - index - 2);
                if (tag >= ISCConstants.isc_bpb_source_type && tag <= ISCConstants.isc_bpb_target_interp) {
                    values[tag] = readVaxShort(bpb, index + 2, valueLength);
                }
                index += 2 + valueLength;
            }
            sourceType = values[ISCConstants.isc_bpb_source_type];
            targetType = values[ISCConstants.isc_bpb_target_type];
            sourceCharset = values[ISCConstants.isc_bpb_source_interp];
            targetCharset = values[ISCConstants.isc_bpb_target_interp];
        }
        
        private int readVaxShort(final byte[] bpb, final int offset, final int length) {
            if (length <= 0 || length > Integer.BYTES) {
                return 0;
            }
            int result = bpb[offset + length - 1] << 8 * (length - 1);
            for (int i = 0; i < length - 1; i++) {
                result += (bpb[offset + i] & 0xFF) << 8 * i;
            }
            return (short) result;
        }
    }
}
