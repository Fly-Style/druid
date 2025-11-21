/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.kafka.supervisor;

import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.TaskReportData;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.HybridPartitionAwareAutoScaler;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.HybridPartitionAwareAutoScalerConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HybridPartitionAwareAutoScalerKafkaTest
{
  private static HybridPartitionAwareAutoScalerConfig cfg()
  {
    return HybridPartitionAwareAutoScalerConfig.builderEnabled(1, 16)
                                               .minTriggerScaleActionFrequencyMillis(60_000L)
                                               .waitLow(0.2)
                                               .waitHigh(0.6)
                                               .lagHighWater(1000L)
                                               .partitionAligned(true)
                                               .scaleUpMinIntervalMs(60_000L)
                                               .scaleActionStartDelayMs(0L)
                                               .scaleActionPeriodMs(60_000L)
                                               .scaleInStep(1)
                                               .scaleOutStep(2)
                                               .stepMaxDelta(3)
                                               .build();
  }

  private static int invokeComputeDesiredTaskCount(HybridPartitionAwareAutoScaler scaler) throws Exception
  {
    Method m = HybridPartitionAwareAutoScaler.class.getDeclaredMethod("computeDesiredTaskCount");
    m.setAccessible(true);
    Object result = m.invoke(scaler);
    return (Integer) result;
  }

  @Test
  public void testScaleOutOnLagAndAdvisoryPlan() throws Exception
  {
    SeekableStreamSupervisor supervisor = EasyMock.createMock(SeekableStreamSupervisor.class);
    SupervisorSpec spec = EasyMock.createMock(SupervisorSpec.class);
    ServiceEmitter emitter = new StubServiceEmitter("svc", "host");
    SeekableStreamSupervisorIOConfig ioConfig = EasyMock.createMock(SeekableStreamSupervisorIOConfig.class);

    EasyMock.expect(ioConfig.getStream()).andReturn("topic").anyTimes();
    EasyMock.replay(ioConfig);

    // Supervisor basics
    EasyMock.expect(supervisor.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(supervisor.getActiveTaskGroupsCount()).andReturn(3).anyTimes();
    EasyMock.expect(supervisor.getPartitionCount()).andReturn(8).anyTimes();
    EasyMock.expect(supervisor.computeLagStats()).andReturn(new LagStats(0, 2_000_000L, 0)).anyTimes();

    // Build a Kafka payload with two tasks and lags to drive advisory planning
    KafkaSupervisorReportPayload payload = new KafkaSupervisorReportPayload(
        "id",
        "ds",
        "topic",
        8,
        1,
        60,
        null,
        null,
        null,
        1_000_000L,
        DateTime.now(),
        false,
        true,
        null,
        null,
        Collections.emptyList()
    );

    Map<KafkaTopicPartition, Long> lagA = new HashMap<>();
    lagA.put(new KafkaTopicPartition(true, "topic", 0), 0L);
    lagA.put(new KafkaTopicPartition(true, "topic", 1), 1L);

    Map<KafkaTopicPartition, Long> lagB = new HashMap<>();
    lagB.put(new KafkaTopicPartition(true, "topic", 2), 4000L);
    lagB.put(new KafkaTopicPartition(true, "topic", 3), 3000L);

    TaskReportData<KafkaTopicPartition, Long> tA = new TaskReportData<>(
        "task-A",
        null,
        null,
        DateTime.now(),
        100L,
        TaskReportData.TaskType.ACTIVE,
        lagA,
        null
    );

    TaskReportData<KafkaTopicPartition, Long> tB = new TaskReportData<>(
        "task-B",
        null,
        null,
        DateTime.now(),
        100L,
        TaskReportData.TaskType.ACTIVE,
        lagB,
        null
    );

    payload.addTask(tA);
    payload.addTask(tB);

    SupervisorReport<KafkaSupervisorReportPayload> report = new SupervisorReport<>("id", DateTime.now(), payload);

    EasyMock.expect(supervisor.getStatus()).andReturn(report).anyTimes();
    EasyMock.replay(supervisor);

    HybridPartitionAwareAutoScaler scaler = new HybridPartitionAwareAutoScaler(supervisor, "ds", cfg(), spec, emitter);

    // Decision: should scale from 3 to 4 (aligned to 8 within stepMaxDelta cap)
    int desired = invokeComputeDesiredTaskCount(scaler);
    Assert.assertEquals(4, desired);

    // Simulate recent scale-up to trigger cooldown skip on next tick
    Field f = HybridPartitionAwareAutoScaler.class.getDeclaredField("lastScaleUpTime");
    f.setAccessible(true);
    f.setLong(scaler, System.currentTimeMillis());

    // Next decision: ensure skip (-1)
    int desired2 = invokeComputeDesiredTaskCount(scaler);
    Assert.assertEquals(-1, desired2);

    // Invoke advisory plan method via reflection and assert non-empty
    Method m = HybridPartitionAwareAutoScaler.class.getDeclaredMethod("computeAdvisoryPlan");
    m.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<?> plan = (List<?>) m.invoke(scaler);
    Assert.assertFalse(plan.isEmpty());
  }
}
