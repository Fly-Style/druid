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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import javax.annotation.Nullable;

/**
 * Proof-of-concept hybrid autoscaler config for streaming ingestion jobs.
 * Minimal configuration surface; reuses shared bounds and cooldown from {@link AutoScalerConfig}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = HybridPartitionAwareAutoScalerConfig.Builder.class)
public class HybridPartitionAwareAutoScalerConfig implements AutoScalerConfig
{
  private static final long MIN_TRIGGER_SCALE_ACTION_FREQ_MS = 600_000L;
  private static final double DEFAULT_WAIT_LOW = 0.20d;
  private static final double DEFAULT_WAIT_HIGH = 0.60d;
  private static final long DEFAULT_HIGH_WATER = 5_000_000L;
  private static final long DEFAULT_MIN_SCALEUP_INTERVAL_MS = 1_800_000L;
  private static final long DEFAULT_SCALE_ACTION_PERIOD_MS = 60_000L;
  private static final long DEFAULT_SCALE_ACTION_DELAY_MS = DEFAULT_SCALE_ACTION_PERIOD_MS * 5;
  private static final int DEFAULT_SCALE_IN_STEP = 1;
  private static final int DEFAULT_SCALE_OUT_STEP = 2;
  private static final int DEFAULT_STEP_MAX_DELTA = 3;
  private static final String DEFAULT_STEP_POLICY = "partitionAlignedSnap";

  // Shared/base
  private final boolean enableTaskAutoScaler;
  private final long minTriggerScaleActionFreqMs;
  private final int taskCountMax;
  private final int taskCountMin;
  @Nullable
  private final Integer taskCountStart;
  @Nullable
  private final Double stopTaskCountRatio;

  // Hybrid-specific knobs (minimal for PoC)
  private final double waitLow;   // 0..1
  private final double waitHigh;  // 0..1
  private final long lagHighWater; // records; override to scale-out when exceeded
  private final boolean partitionAligned; // prefer partition-aligned scale-out
  private final long scaleUpMinIntervalMs; // longer min interval for ups

  // Scheduling knobs (align with lag-based for consistency)
  private final long scaleActionStartDelayMs;
  private final long scaleActionPeriodMs;

  // Scaling steps
  private final int scaleInStep;
  private final int scaleOutStep;
  // Step policy
  private final String stepPolicy;
  private final int stepMaxDelta;

  private HybridPartitionAwareAutoScalerConfig(Builder b)
  {
    this.enableTaskAutoScaler = b.enableTaskAutoScaler != null ? b.enableTaskAutoScaler : false;

    if (this.enableTaskAutoScaler) {
      Preconditions.checkNotNull(b.taskCountMax, "taskCountMax");
      Preconditions.checkNotNull(b.taskCountMin, "taskCountMin");
      Preconditions.checkArgument(b.taskCountMax >= b.taskCountMin, "taskCountMax must be >= taskCountMin");
      if (b.taskCountStart != null) {
        Preconditions.checkArgument(
            b.taskCountStart >= b.taskCountMin && b.taskCountStart <= b.taskCountMax,
            "taskCountMin <= taskCountStart <= taskCountMax"
        );
      }
    }
    this.taskCountMax = b.taskCountMax == null ? 0 : b.taskCountMax;
    this.taskCountMin = b.taskCountMin == null ? 0 : b.taskCountMin;
    this.taskCountStart = b.taskCountStart;

    this.minTriggerScaleActionFreqMs = b.minTriggerScaleActionFreqMs != null
                                       ? b.minTriggerScaleActionFreqMs : MIN_TRIGGER_SCALE_ACTION_FREQ_MS;

    // Hybrid knobs defaults
    this.waitLow = b.waitLow != null ? b.waitLow : DEFAULT_WAIT_LOW;
    this.waitHigh = b.waitHigh != null ? b.waitHigh : DEFAULT_WAIT_HIGH;
    this.lagHighWater = b.lagHighWater != null ? b.lagHighWater : DEFAULT_HIGH_WATER;
    this.partitionAligned = b.partitionAligned != null ? b.partitionAligned : true;
    this.scaleUpMinIntervalMs = b.scaleUpMinIntervalMs != null ? b.scaleUpMinIntervalMs
                                                               : DEFAULT_MIN_SCALEUP_INTERVAL_MS;

    // Scheduling defaults
    this.scaleActionStartDelayMs = b.scaleActionStartDelayMs != null
                                   ? b.scaleActionStartDelayMs
                                   : DEFAULT_SCALE_ACTION_DELAY_MS; // 5m
    this.scaleActionPeriodMs = b.scaleActionPeriodMs != null
                               ? b.scaleActionPeriodMs
                               : DEFAULT_SCALE_ACTION_PERIOD_MS; // 1m

    this.scaleInStep = b.scaleInStep != null ? b.scaleInStep : DEFAULT_SCALE_IN_STEP;
    this.scaleOutStep = b.scaleOutStep != null ? b.scaleOutStep : DEFAULT_SCALE_OUT_STEP;
    this.stepPolicy = b.stepPolicy != null ? b.stepPolicy : DEFAULT_STEP_POLICY;
    this.stepMaxDelta = b.stepMaxDelta != null ? b.stepMaxDelta : DEFAULT_STEP_MAX_DELTA;

    Preconditions.checkArgument(this.waitLow >= 0.0 && this.waitLow <= 1.0, "0.0 <= waitLow <= 1.0");
    Preconditions.checkArgument(this.waitHigh >= 0.0 && this.waitHigh <= 1.0, "0.0 <= waitHigh <= 1.0");
    Preconditions.checkArgument(this.waitLow <= this.waitHigh, "waitLow <= waitHigh");

    Preconditions.checkArgument(
        b.stopTaskCountRatio == null || (b.stopTaskCountRatio > 0.0 && b.stopTaskCountRatio <= 1.0),
        "0.0 < stopTaskCountRatio <= 1.0"
    );
    this.stopTaskCountRatio = b.stopTaskCountRatio;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builderEnabled(int taskCountMin, int taskCountMax)
  {
    return new Builder().enableTaskAutoScaler(true).taskCountMin(taskCountMin).taskCountMax(taskCountMax);
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class Builder
  {
    private Boolean enableTaskAutoScaler;
    private Long minTriggerScaleActionFreqMs;
    private Integer taskCountMax;
    private Integer taskCountMin;
    private Integer taskCountStart;
    private Double stopTaskCountRatio;

    private Double waitLow;
    private Double waitHigh;
    private Long lagHighWater;
    private Boolean partitionAligned;
    private Long scaleUpMinIntervalMs;

    private Long scaleActionStartDelayMs;
    private Long scaleActionPeriodMs;

    private Integer scaleInStep;
    private Integer scaleOutStep;

    // Step sizing policy
    private String stepPolicy;
    private Integer stepMaxDelta;

    public Builder()
    {

    }

    // Optional convenience constructor for minimal required fields when enabled
    public Builder(int taskCountMin, int taskCountMax)
    {
      this.enableTaskAutoScaler = true;
      this.taskCountMin = taskCountMin;
      this.taskCountMax = taskCountMax;
    }

    @JsonProperty("enableTaskAutoScaler")
    public Builder enableTaskAutoScaler(@Nullable Boolean enableTaskAutoScaler)
    {
      this.enableTaskAutoScaler = enableTaskAutoScaler;
      return this;
    }

    @JsonProperty("minTriggerScaleActionFrequencyMillis")
    public Builder minTriggerScaleActionFrequencyMillis(@Nullable Long v)
    {
      this.minTriggerScaleActionFreqMs = v;
      return this;
    }

    @JsonProperty("taskCountMax")
    public Builder taskCountMax(@Nullable Integer v)
    {
      this.taskCountMax = v;
      return this;
    }

    @JsonProperty("taskCountMin")
    public Builder taskCountMin(@Nullable Integer v)
    {
      this.taskCountMin = v;
      return this;
    }

    @JsonProperty("taskCountStart")
    public Builder taskCountStart(@Nullable Integer v)
    {
      this.taskCountStart = v;
      return this;
    }

    @JsonProperty("stopTaskCountRatio")
    public Builder stopTaskCountRatio(@Nullable Double v)
    {
      this.stopTaskCountRatio = v;
      return this;
    }

    @JsonProperty("waitLow")
    public Builder waitLow(@Nullable Double v)
    {
      this.waitLow = v;
      return this;
    }

    @JsonProperty("waitHigh")
    public Builder waitHigh(@Nullable Double v)
    {
      this.waitHigh = v;
      return this;
    }


    @JsonProperty("lagHighWater")
    public Builder lagHighWater(@Nullable Long v)
    {
      this.lagHighWater = v;
      return this;
    }

    @JsonProperty("partitionAligned")
    public Builder partitionAligned(@Nullable Boolean v)
    {
      this.partitionAligned = v;
      return this;
    }

    @JsonProperty("scaleUpMinIntervalMs")
    public Builder scaleUpMinIntervalMs(@Nullable Long v)
    {
      this.scaleUpMinIntervalMs = v;
      return this;
    }

    @JsonProperty("scaleActionStartDelayMs")
    public Builder scaleActionStartDelayMs(@Nullable Long v)
    {
      this.scaleActionStartDelayMs = v;
      return this;
    }

    @JsonProperty("scaleActionPeriodMs")
    public Builder scaleActionPeriodMs(@Nullable Long v)
    {
      this.scaleActionPeriodMs = v;
      return this;
    }

    @JsonProperty("scaleInStep")
    public Builder scaleInStep(@Nullable Integer v)
    {
      this.scaleInStep = v;
      return this;
    }

    @JsonProperty("scaleOutStep")
    public Builder scaleOutStep(@Nullable Integer v)
    {
      this.scaleOutStep = v;
      return this;
    }

    @JsonProperty("stepPolicy")
    public Builder stepPolicy(@Nullable String v)
    {
      this.stepPolicy = v;
      return this;
    }

    @JsonProperty("stepMaxDelta")
    public Builder stepMaxDelta(@Nullable Integer v)
    {
      this.stepMaxDelta = v;
      return this;
    }

    public HybridPartitionAwareAutoScalerConfig build()
    {
      return new HybridPartitionAwareAutoScalerConfig(this);
    }
  }

  @Override
  public boolean getEnableTaskAutoScaler()
  {
    return enableTaskAutoScaler;
  }

  @Override
  public long getMinTriggerScaleActionFreqMs()
  {
    return minTriggerScaleActionFreqMs;
  }

  @Override
  public int getTaskCountMax()
  {
    return taskCountMax;
  }

  @Override
  public int getTaskCountMin()
  {
    return taskCountMin;
  }

  @Nullable
  @Override
  public Integer getTaskCountStart()
  {
    return taskCountStart;
  }

  @Nullable
  @Override
  public Double getStopTaskCountRatio()
  {
    return stopTaskCountRatio;
  }

  public double getWaitLow()
  {
    return waitLow;
  }

  public double getWaitHigh()
  {
    return waitHigh;
  }

  public long getLagHighWater()
  {
    return lagHighWater;
  }

  public boolean isPartitionAligned()
  {
    return partitionAligned;
  }

  public long getScaleUpMinIntervalMs()
  {
    return scaleUpMinIntervalMs;
  }

  public long getScaleActionStartDelayMs()
  {
    return scaleActionStartDelayMs;
  }

  public long getScaleActionPeriodMs()
  {
    return scaleActionPeriodMs;
  }

  public int getScaleInStep()
  {
    return scaleInStep;
  }

  public int getScaleOutStep()
  {
    return scaleOutStep;
  }

  public String getStepPolicy()
  {
    return stepPolicy;
  }

  public int getStepMaxDelta()
  {
    return stepMaxDelta;
  }

  @Override
  public SupervisorTaskAutoScaler createAutoScaler(
      Supervisor supervisor,
      SupervisorSpec spec,
      ServiceEmitter emitter
  )
  {
    if (!(supervisor instanceof SeekableStreamSupervisor)) {
      return new NoopTaskAutoScaler();
    }
    return new HybridPartitionAwareAutoScaler((SeekableStreamSupervisor) supervisor, spec.getId(), this, spec, emitter);
  }
}
