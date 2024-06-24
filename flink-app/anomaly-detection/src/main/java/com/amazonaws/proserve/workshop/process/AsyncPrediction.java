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

package com.amazonaws.proserve.workshop.process;

import com.amazonaws.proserve.workshop.process.model.Event;
import com.amazonaws.proserve.workshop.process.model.InferenceResult;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeAsync;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeAsyncClientBuilder;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointRequest;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static java.lang.Math.abs;

@Slf4j
public class AsyncPrediction extends RichAsyncFunction<List<Event>, Event> {
    private final double threshold;
    private final String sageMakerEndpoint;
    private transient AmazonSageMakerRuntimeAsync runtimeAsync;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        runtimeAsync = AmazonSageMakerRuntimeAsyncClientBuilder.defaultClient();
    }

    public AsyncPrediction(String sageMakerEndpoint, double threshold) {
        super();
        this.sageMakerEndpoint = sageMakerEndpoint;
        this.threshold = threshold;
    }

    @Override
    public void asyncInvoke(List<Event> events, ResultFuture<Event> resultFuture) {
        final byte[] payload = serialize(events);
        final InvokeEndpointRequest request = getSagemakerRequest(payload);
        final Future<InvokeEndpointResult> result = runtimeAsync.invokeEndpointAsync(request);
        CompletableFuture.supplyAsync(() -> {
            try {
                return deserialize(result.get().getBody().array());
            } catch (InterruptedException | ExecutionException e) {
                // Normally handled explicitly.
                return null;
            }
        }).thenAccept((InferenceResult inferenceResult) -> {
            if (inferenceResult != null) {
                InferenceResult.Prediction[] predictions = inferenceResult.getPredictions();
                for (int i = 0; i < predictions.length; i++) {
                    events.get(i).setFraud(abs(predictions[i].getDotProduct()) > threshold);
                    events.get(i).setDotProduct(predictions[i].getDotProduct());
                }
                resultFuture.complete(events);
            }
        });
    }

    @Override
    public void timeout(List<Event> input, ResultFuture<Event> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
        log.error("Timeout!");
    }

    private InvokeEndpointRequest getSagemakerRequest(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        return new InvokeEndpointRequest().withEndpointName(sageMakerEndpoint).withContentType("text/csv")
                .withAccept("application/json").withBody(buf);
    }

    private byte[] serialize(List<Event> input) {
        StringBuilder sb = new StringBuilder();
        for (Event e : input) {
            // writerId,ipSrc\n
            sb.append(e.getWriterId());
            sb.append(',');
            sb.append(e.getIpSrc());
            sb.append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private InferenceResult deserialize(byte[] output) {
        try {
            ObjectMapper om = new ObjectMapper();
            return om.readValue(output, InferenceResult.class);
        } catch (IOException io) {
            log.error("Error {} during deserializing SageMaker response: {}", io.getMessage(), new String(output));
            return null;
        }
    }
}
