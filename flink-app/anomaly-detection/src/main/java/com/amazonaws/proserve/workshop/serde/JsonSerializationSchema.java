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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;

@Slf4j
public class JsonSerializationSchema<T> implements SerializationSchema<T> {
    private static final long serialVersionUID = 1L;

    /**
     * Creates {@link JsonSerializationSchema} that produces json from given class.
     *
     * @param tClass class of record to be serialized
     * @return serializer
     */
    public static <T> JsonSerializationSchema<T> forSpecific(Class<T> tClass) {
        return new JsonSerializationSchema<>(tClass);
    }

    /** Jackson ObjectMapper */
    private transient ObjectMapper objectMapper;

    /**
     * Creates a deserialization schema.
     *
     * @param recordClazz class to which deserialize. Should be one of:
     */
    JsonSerializationSchema(Class<T> recordClazz) {
        Preconditions.checkNotNull(recordClazz, "Record class must not be null.");
    }

    @Override
    public byte[] serialize(T element) {
        checkIfInit();
        log.debug("{}", element);
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
    }

    private void checkIfInit() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.registerModule(new JavaTimeModule());

        }
    }

}
