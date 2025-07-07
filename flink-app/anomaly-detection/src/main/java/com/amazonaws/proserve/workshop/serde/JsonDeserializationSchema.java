/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.amazonaws.proserve.workshop.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * This is a slightly modified copy of existing Flink class made to use
 * AvroSchema with Json format
 */

@Slf4j
public class JsonDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    /**
     * Creates {@link JsonDeserializationSchema} that produces classes that were
     * generated from schema.
     *
     * @param tClass class of record to be produced
     * @return deserializer
     */
    public static <T> JsonDeserializationSchema<T> forSpecific(Class<T> tClass) {
        return new JsonDeserializationSchema<>(tClass);
    }

    /** Class to deserialize to. */
    private final Class<T> recordClazz;

    /** Jackson ObjectMapper */
    private transient ObjectMapper objectMapper;

    /**
     * Creates a deserialization schema.
     *
     * @param recordClazz class to which deserialize.
     */
    JsonDeserializationSchema(Class<T> recordClazz) {
        Preconditions.checkNotNull(recordClazz, "Record class must not be null.");
        this.recordClazz = recordClazz;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        // read record
        checkIfInit();
        log.debug("{}", new String(message));
        return objectMapper.readValue(message, recordClazz);
    }

    private void checkIfInit() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.registerModule(new JavaTimeModule());

        }
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(recordClazz);
    }
}
