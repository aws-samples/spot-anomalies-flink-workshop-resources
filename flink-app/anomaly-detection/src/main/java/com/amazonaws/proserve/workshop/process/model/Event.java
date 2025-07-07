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

package com.amazonaws.proserve.workshop.process.model;

import java.math.BigInteger;
import java.time.Instant;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;

@Data
@Builder
@Jacksonized
@ToString(exclude = { "text" })
public class Event {
    @JsonProperty("event_type")
    private String event_type;
    @JsonProperty("ip_src")
    private String ipSrc;
    @JsonProperty("ip_dst")
    private String ipDst;
    @JsonProperty("port_src")
    private String portSrc;
    @JsonProperty("port_dst")
    private String portDst;
    @JsonProperty("ip_proto")
    private String ipProto;
    @JsonProperty("timestamp_start")
    private BigInteger tsStart;
    @JsonProperty("timestamp_end")
    private BigInteger tsEnd;
    @JsonProperty("packets")
    private Integer packets;
    @JsonProperty("bytes")
    private Integer bytes;
    @JsonProperty("writer_id")
    private String writerId;
    @JsonProperty("text")
    private String text;
    @JsonProperty("avg_packets")
    private Double avgPackets;
    
    public Instant getCalculatedEventTime() {
        return tsStart != null ? Instant.ofEpochMilli(tsStart.longValue()) : null;
    }
    
    public boolean isAnomaly() {
        if (avgPackets != null && packets != null && 
               packets < (avgPackets * 0.1)) {
                return true;
               }
        return false;
    }
}
