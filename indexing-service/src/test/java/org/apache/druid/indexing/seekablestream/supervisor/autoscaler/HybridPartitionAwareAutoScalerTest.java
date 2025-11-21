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

package org.apache.druid.indexing.seekablestream.supervisor.autoscaler;

import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Unit tests for HybridPartitionAwareAutoScaler decision logic.
 * These tests use reflection to invoke the private computeDesiredTaskCount() method directly
 * to avoid threading and supervisor notice plumbing.
 */
public class HybridPartitionAwareAutoScalerTest
{
  private SeekableStreamSupervisor supervisor;
  private SupervisorSpec spec;
  private ServiceEmitter emitter;
  private SeekableStreamSupervisorIOConfig ioConfig;

  @Before
  public void setUp()
  {
    supervisor = EasyMock.createMock(SeekableStreamSupervisor.class);
    spec = EasyMock.createMock(SupervisorSpec.class);
    emitter = new StubServiceEmitter("service", "host");
    ioConfig = EasyMock.createMock(SeekableStreamSupervisorIOConfig.class);

    EasyMock.expect(ioConfig.getStream()).andReturn("stream").anyTimes();
    EasyMock.replay(ioConfig);

    EasyMock.expect(supervisor.getIoConfig()).andReturn(ioConfig).anyTimes();
    // Advisory Phase B is invoked each tick; by default return null report unless overridden in tests
    EasyMock.expect(supervisor.getStatus()).andReturn(null).anyTimes();
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(supervisor);
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception
  {
    Field f = HybridPartitionAwareAutoScaler.class.getDeclaredField(fieldName);
    f.setAccessible(true);
    f.set(target, value);
  }

  private HybridPartitionAwareAutoScalerConfig config(
      int taskCountMin,
      int taskCountMax,
      double waitLow,
      double waitHigh,
      long lagHighWater,
      boolean partitionAligned,
      long scaleUpMinIntervalMillis,
      int scaleInStep,
      int scaleOutStep
  )
  {
    return HybridPartitionAwareAutoScalerConfig.builderEnabled(taskCountMin, taskCountMax)
                                               .minTriggerScaleActionFrequencyMillis(60_000L)
                                               .waitLow(waitLow)
                                               .waitHigh(waitHigh)
                                               .lagHighWater(lagHighWater)
                                               .partitionAligned(partitionAligned)
                                               .scaleUpMinIntervalMs(scaleUpMinIntervalMillis)
                                               .scaleActionStartDelayMs(0L)
                                               .scaleActionPeriodMs(60_000L)
                                               .scaleInStep(scaleInStep)
                                               .scaleOutStep(scaleOutStep)
                                               .stepMaxDelta(100)
                                               .build();
  }

  @Test
  public void testScaleOutOnLagHighWater_partitionAligned() throws Exception
  {
    // Given aggregate lag >= high water => scale out by step and align to partition count
    HybridPartitionAwareAutoScalerConfig cfg = config(2, 16, 0.2, 0.6, 5_000_000L, true, 0L, 1, 2);

    // current=4, partitions=12 => proposed=6 which divides 12 => expect 6
    EasyMock.expect(supervisor.getActiveTaskGroupsCount()).andReturn(4).anyTimes();
    EasyMock.expect(supervisor.getPartitionCount()).andReturn(12).anyTimes();
    EasyMock.expect(supervisor.computeLagStats()).andReturn(new LagStats(0, 5_000_000L, 0)).anyTimes();
    EasyMock.replay(supervisor);

    HybridPartitionAwareAutoScaler scaler = new HybridPartitionAwareAutoScaler(supervisor, "ds", cfg, spec, emitter);
    Assert.assertEquals(6, scaler.computeDesiredTaskCount());
  }

  @Test
  public void testPartitionAlignment_bestRemainderSelected() throws Exception
  {
    // Partition count 13 has no small divisors beyond 1; ensure scaler picks the t with smallest remainder
    HybridPartitionAwareAutoScalerConfig cfg = config(1, 100, 0.2, 0.6, 4_000_000L, true, 0L, 1, 2);

    // Force scale-out via high lag to exercise partition alignment
    EasyMock.expect(supervisor.getActiveTaskGroupsCount()).andReturn(3).anyTimes(); // current
    EasyMock.expect(supervisor.getPartitionCount()).andReturn(13).anyTimes();
    EasyMock.expect(supervisor.computeLagStats()).andReturn(new LagStats(0, 4_000_000L, 0)).anyTimes();
    EasyMock.replay(supervisor);

    HybridPartitionAwareAutoScaler scaler = new HybridPartitionAwareAutoScaler(supervisor, "ds", cfg, spec, emitter);

    // proposed=5 -> implementation searches up to maxAllowed and prefers perfect alignment; 13 divides 13
    Assert.assertEquals(13, scaler.computeDesiredTaskCount());
  }

  @Test
  public void testScaleInOnLowWaitAndLowLag() throws Exception
  {
    HybridPartitionAwareAutoScalerConfig cfg = config(2, 16, 0.2, 0.6, 5_000_000L, true, 0L, 1, 2);

    EasyMock.expect(supervisor.getActiveTaskGroupsCount()).andReturn(4).anyTimes();
    EasyMock.expect(supervisor.getPartitionCount()).andReturn(12).anyTimes();
    EasyMock.expect(supervisor.computeLagStats()).andReturn(new LagStats(0, 10, 0)).anyTimes(); // lag below high-water
    EasyMock.replay(supervisor);

    HybridPartitionAwareAutoScaler scaler = new HybridPartitionAwareAutoScaler(supervisor, "ds", cfg, spec, emitter);

    Assert.assertEquals(3, scaler.computeDesiredTaskCount()); // scale in by 1
  }

  @Test
  public void testRespectScaleUpMinInterval_emitsSkipMetric() throws Exception
  {
    HybridPartitionAwareAutoScalerConfig cfg = config(1, 16, 0.2, 0.6, 5_000_000L, true, 60_000L, 1, 2);

    // Force scale-out from waitHigh path, but with lastScaleUpTime recent => should skip and emit metric
    EasyMock.expect(supervisor.getActiveTaskGroupsCount()).andReturn(2).anyTimes();
    EasyMock.expect(supervisor.getPartitionCount()).andReturn(12).anyTimes();
    EasyMock.expect(supervisor.computeLagStats()).andReturn(new LagStats(0, 0, 0)).anyTimes();
    EasyMock.replay(supervisor);

    StubServiceEmitter stubEmitter = new StubServiceEmitter("svc", "host");
    HybridPartitionAwareAutoScaler scaler = new HybridPartitionAwareAutoScaler(
        supervisor,
        "ds",
        cfg,
        spec,
        stubEmitter
    );

    // Simulate recent scale-up by setting lastScaleUpTime = now
    setField(scaler, "lastScaleUpTime", System.currentTimeMillis());

    Assert.assertEquals(-1, scaler.computeDesiredTaskCount());

    // Verify a metric was emitted for required tasks with skip reason dimension
    Assert.assertFalse(
        stubEmitter.getMetricEvents(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC).isEmpty()
    );
  }

  @Test
  public void testNoDecisionWhenNoPartitions() throws Exception
  {
    HybridPartitionAwareAutoScalerConfig cfg = config(1, 16, 0.2, 0.6, 5_000_000L, true, 0L, 1, 2);

    EasyMock.expect(supervisor.getActiveTaskGroupsCount()).andReturn(2).anyTimes();
    EasyMock.expect(supervisor.getPartitionCount()).andReturn(0).anyTimes();
    EasyMock.replay(supervisor);

    HybridPartitionAwareAutoScaler scaler = new HybridPartitionAwareAutoScaler(supervisor, "ds", cfg, spec, emitter);

    Assert.assertEquals(-1, scaler.computeDesiredTaskCount());
  }
}
