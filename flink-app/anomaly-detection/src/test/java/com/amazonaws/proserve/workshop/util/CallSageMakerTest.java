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

package com.amazonaws.proserve.workshop.util;

import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntime;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeClientBuilder;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointRequest;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointResult;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Disabled
public class CallSageMakerTest {

    @Test
    void callSageMaker() {
        byte[] payload = "eni-095bb4db87156aa49,10.0.3.60\neni-095bb4db87156aa49,10.0.3.60\neni-095bb4db87156aa49,10.0.3.60\neni-095bb4db87156aa49,10.0.3.60"
                .getBytes(StandardCharsets.UTF_8);
        String endpointName = "ipinsights-2024-04-15-15-51-45-169";
        InvokeEndpointRequest invokeEndpointRequest = new InvokeEndpointRequest();
        invokeEndpointRequest.setContentType("text/csv");
        ByteBuffer buf = ByteBuffer.wrap(payload);

        invokeEndpointRequest.setBody(buf);
        invokeEndpointRequest.setEndpointName(endpointName);
        invokeEndpointRequest.setAccept("application/json");

        AmazonSageMakerRuntime amazonSageMaker = AmazonSageMakerRuntimeClientBuilder.defaultClient();
        InvokeEndpointResult invokeEndpointResult = amazonSageMaker.invokeEndpoint(invokeEndpointRequest);
        ByteBuffer body = invokeEndpointResult.getBody();
        System.out.print(StandardCharsets.UTF_8.decode(body));
    }
}
