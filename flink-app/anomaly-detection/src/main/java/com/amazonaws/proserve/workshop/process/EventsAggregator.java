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
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class EventsAggregator extends KeyedProcessFunction<String, Event, List<Event>> {
    private final int maxEvents;
    private final int freqSecs;
    private ListState<Event> events;
    private ValueState<Integer> eventsCount;

    public EventsAggregator(int maxEvents, int freqSecs) {
        this.maxEvents = maxEvents;
        this.freqSecs = freqSecs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        events = getRuntimeContext().getListState(new ListStateDescriptor<>("events", Event.class));
        eventsCount = getRuntimeContext().getState(new ValueStateDescriptor<>("events-count", Integer.class));
    }

    @Override
    public void processElement(Event event, KeyedProcessFunction<String, Event, List<Event>>.Context context,
            Collector<List<Event>> collector) {
        try {
            if (eventsCount.value() == null)
                eventsCount.update(0);
            log.debug("Event:{}", event);
            events.add(event);
            eventsCount.update(+1);
            if (eventsCount.value() >= maxEvents) {
                concatenate(collector);
            } else {
                long currentTime = context.timerService().currentProcessingTime();
                context.timerService().registerProcessingTimeTimer(roundToInterval(currentTime, freqSecs * 1_000L)); // trigger
                                                                                                                     // timer
                                                                                                                     // every
                                                                                                                     // 5
                                                                                                                     // seconds
            }
        } catch (Exception e) {
            log.error("Exception {} during processing event: {}", e, event);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, List<Event>>.OnTimerContext ctx,
            Collector<List<Event>> collector) {
        concatenate(collector);
    }

    private long roundToInterval(long x, long units) {
        return (x / units) * units + units;
    }

    private void concatenate(Collector<List<Event>> collector) {
        try {
            List<Event> bufferedEvents = StreamSupport.stream(events.get().spliterator(), false)
                    .collect(Collectors.toList());
            if (bufferedEvents.isEmpty())
                return;
            collector.collect(bufferedEvents);
            eventsCount.update(eventsCount.value() - 1);
            events.clear();
        } catch (Exception e) {
            log.error("Exception during processing events", e);
        }
    }
}
