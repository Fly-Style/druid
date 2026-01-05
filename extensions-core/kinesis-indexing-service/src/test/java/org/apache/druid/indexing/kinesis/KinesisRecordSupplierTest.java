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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.kinesis.KinesisRecordEntity;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.druid.indexing.kinesis.KinesisSequenceNumber.END_OF_SHARD_MARKER;
import static org.apache.druid.indexing.kinesis.KinesisSequenceNumber.EXPIRED_MARKER;
import static org.apache.druid.indexing.kinesis.KinesisSequenceNumber.NO_END_SEQUENCE_NUMBER;
import static org.apache.druid.indexing.kinesis.KinesisSequenceNumber.UNREAD_LATEST;
import static org.apache.druid.indexing.kinesis.KinesisSequenceNumber.UNREAD_TRIM_HORIZON;

public class KinesisRecordSupplierTest extends EasyMockSupport
{
  private static final String STREAM = "stream";
  private static final long POLL_TIMEOUT_MILLIS = 2000;
  private static final String SHARD_ID1 = "1";
  private static final String SHARD_ID0 = "0";
  private static final String SHARD1_ITERATOR = "1";
  private static final String SHARD0_ITERATOR = "0";

  private static final Long SHARD0_LAG_MILLIS = 100L;
  private static final Long SHARD1_LAG_MILLIS = 200L;
  private static final Long SHARD1_LAG_MILLIS_EMPTY = 0L;
  private static Map<String, Long> SHARDS_LAG_MILLIS =
      ImmutableMap.of(SHARD_ID0, SHARD0_LAG_MILLIS, SHARD_ID1, SHARD1_LAG_MILLIS);
  private static Map<String, Long> SHARDS_LAG_MILLIS_EMPTY =
          ImmutableMap.of(SHARD_ID0, SHARD0_LAG_MILLIS, SHARD_ID1, SHARD1_LAG_MILLIS_EMPTY);
  private static final List<Record> SHARD0_RECORDS = ImmutableList.of(
      Record.builder().data(SdkBytes.fromByteBuffer(jb("2008", "a", "y", "10", "20.0", "1.0"))).sequenceNumber("0")
      .build(),
      Record.builder().data(SdkBytes.fromByteBuffer(jb("2009", "b", "y", "10", "20.0", "1.0"))).sequenceNumber("1")
      .build()
  );
  private static final List<Record> SHARD1_RECORDS_EMPTY = ImmutableList.of();
  private static final List<Record> SHARD1_RECORDS = ImmutableList.of(
      Record.builder().data(SdkBytes.fromByteBuffer(jb("2011", "d", "y", "10", "20.0", "1.0"))).sequenceNumber("0")
      .build(),
      Record.builder().data(SdkBytes.fromByteBuffer(jb("2011", "e", "y", "10", "20.0", "1.0"))).sequenceNumber("1")
      .build(),
      Record.builder().data(SdkBytes.fromByteBuffer(jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0"))).sequenceNumber("2")
      .build(),
      Record.builder().data(SdkBytes.fromByteBuffer(ByteBuffer.wrap(StringUtils.toUtf8("unparseable")))).sequenceNumber("3")
      .build(),
      Record.builder().data(SdkBytes.fromByteBuffer(ByteBuffer.wrap(StringUtils.toUtf8("unparseable2")))).sequenceNumber("4")
      .build(),
      Record.builder().data(SdkBytes.fromByteBuffer(ByteBuffer.wrap(StringUtils.toUtf8("{}")))).sequenceNumber("5")
      .build(),
      Record.builder().data(SdkBytes.fromByteBuffer(jb("2013", "f", "y", "10", "20.0", "1.0"))).sequenceNumber("6")
      .build(),
      Record.builder().data(SdkBytes.fromByteBuffer(jb("2049", "f", "y", "notanumber", "20.0", "1.0"))).sequenceNumber("7")
      .build(),
      Record.builder().data(SdkBytes.fromByteBuffer(jb("2012", "g", "y", "10", "20.0", "1.0"))).sequenceNumber("8")
      .build(),
      Record.builder().data(SdkBytes.fromByteBuffer(jb("2011", "h", "y", "10", "20.0", "1.0"))).sequenceNumber("9")
      .build()
  );
  private static final List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> ALL_RECORDS = ImmutableList.<OrderedPartitionableRecord<String, String, KinesisRecordEntity>>builder()
      .addAll(SHARD0_RECORDS.stream()
          .map(x -> new OrderedPartitionableRecord<>(
              STREAM,
              SHARD_ID0,
              x.sequenceNumber(),
              Collections.singletonList(new KinesisRecordEntity(Record.builder().data(SdkBytes.fromByteBuffer(new ByteEntity(x.data().asByteBuffer()).getBuffer()))
                  .build()))
          ))
          .collect(
              Collectors
                  .toList()))
      .addAll(SHARD1_RECORDS.stream()
          .map(x -> new OrderedPartitionableRecord<>(
              STREAM,
              SHARD_ID1,
              x.sequenceNumber(),
              Collections.singletonList(new KinesisRecordEntity(Record.builder().data(SdkBytes.fromByteBuffer(new ByteEntity(x.data().asByteBuffer()).getBuffer()))
                  .build()))
          ))
          .collect(Collectors.toList()))
      .build();


  private static ByteBuffer jb(String timestamp, String dim1, String dim2, String dimLong, String dimFloat, String met1)
  {
    try {
      return ByteBuffer.wrap(new ObjectMapper().writeValueAsBytes(
          ImmutableMap.builder()
                      .put("timestamp", timestamp)
                      .put("dim1", dim1)
                      .put("dim2", dim2)
                      .put("dimLong", dimLong)
                      .put("dimFloat", dimFloat)
                      .put("met1", met1)
                      .build()
      ));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  private static KinesisClient kinesis;
  private static ListShardsResponse listShardsResult0;
  private static ListShardsResponse listShardsResult1;
  private static GetShardIteratorResponse getShardIteratorResult0;
  private static GetShardIteratorResponse getShardIteratorResult1;
  private static DescribeStreamResponse describeStreamResult0;
  private static DescribeStreamResponse describeStreamResult1;
  private static StreamDescription streamDescription0;
  private static StreamDescription streamDescription1;
  private static GetRecordsResponse getRecordsResult0;
  private static GetRecordsResponse getRecordsResult1;
  private static Shard shard0;
  private static Shard shard1;
  private static KinesisRecordSupplier recordSupplier;

  @Before
  public void setupTest()
  {
    kinesis = createMock(KinesisClient.class);
    listShardsResult0 = createMock(ListShardsResponse.class);
    listShardsResult1 = createMock(ListShardsResponse.class);
    describeStreamResult0 = createMock(DescribeStreamResponse.class);
    describeStreamResult1 = createMock(DescribeStreamResponse.class);
    streamDescription0 = createMock(StreamDescription.class);
    streamDescription1 = createMock(StreamDescription.class);
    getShardIteratorResult0 = createMock(GetShardIteratorResponse.class);
    getShardIteratorResult1 = createMock(GetShardIteratorResponse.class);
    getRecordsResult0 = createMock(GetRecordsResponse.class);
    getRecordsResult1 = createMock(GetRecordsResponse.class);
    shard0 = createMock(Shard.class);
    shard1 = createMock(Shard.class);
  }

  @After
  public void tearDownTest()
  {
    if (null != recordSupplier) {
      recordSupplier.close();
    }
    recordSupplier = null;
  }

  @Test
  public void testSupplierSetup_withoutListShards()
  {
    final Capture<DescribeStreamRequest> capturedRequest0 = Capture.newInstance();
    final Capture<DescribeStreamRequest> capturedRequest1 = Capture.newInstance();

    EasyMock.expect(kinesis.describeStream(EasyMock.capture(capturedRequest0))).andReturn(describeStreamResult0).once();
    EasyMock.expect(describeStreamResult0.streamDescription()).andReturn(streamDescription0).once();
    EasyMock.expect(streamDescription0.shards()).andReturn(ImmutableList.of(shard0, shard1)).once();
    EasyMock.expect(shard0.shardId()).andReturn(SHARD_ID0).once();
    EasyMock.expect(shard1.shardId()).andReturn(SHARD_ID1).times(2);
    EasyMock.expect(streamDescription0.isHasMoreShards()).andReturn(true).once();

    EasyMock.expect(kinesis.describeStream(EasyMock.capture(capturedRequest1))).andReturn(describeStreamResult1).once();
    EasyMock.expect(describeStreamResult1.streamDescription()).andReturn(streamDescription1).once();
    EasyMock.expect(streamDescription1.shards()).andReturn(ImmutableList.of()).once();
    EasyMock.expect(streamDescription1.isHasMoreShards()).andReturn(false).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0),
        StreamPartition.of(STREAM, SHARD_ID1)
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        100,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(SHARD_ID0, SHARD_ID1), recordSupplier.getPartitionIds(STREAM));

    // calling poll would start background fetch if seek was called, but will instead be skipped and the results
    // empty
    Assert.assertEquals(Collections.emptyList(), recordSupplier.poll(100));

    verifyAll();

    // Since the same request is modified, every captured argument will be the same at the end
    Assert.assertEquals(capturedRequest0.getValues(), capturedRequest1.getValues());

    final DescribeStreamRequest expectedRequest = DescribeStreamRequest.builder()
        .build();
    expectedRequest = expectedRequest.toBuilder().streamName(STREAM).build();
    expectedRequest = expectedRequest.toBuilder().exclusiveStartShardId(SHARD_ID1).build();
    Assert.assertEquals(expectedRequest, capturedRequest1.getValue());
  }

  @Test
  public void testSupplierSetup_withListShards()
  {
    final Capture<ListShardsRequest> capturedRequest0 = Capture.newInstance();
    final Capture<ListShardsRequest> capturedRequest1 = Capture.newInstance();

    EasyMock.expect(kinesis.listShards(EasyMock.capture(capturedRequest0))).andReturn(listShardsResult0).once();
    EasyMock.expect(listShardsResult0.shards()).andReturn(ImmutableList.of(shard0)).once();
    String nextToken = "nextToken";
    EasyMock.expect(listShardsResult0.nextToken()).andReturn(nextToken).once();
    EasyMock.expect(shard0.shardId()).andReturn(SHARD_ID0).once();
    EasyMock.expect(kinesis.listShards(EasyMock.capture(capturedRequest1))).andReturn(listShardsResult1).once();
    EasyMock.expect(listShardsResult1.shards()).andReturn(ImmutableList.of(shard1)).once();
    EasyMock.expect(listShardsResult1.nextToken()).andReturn(null).once();
    EasyMock.expect(shard1.shardId()).andReturn(SHARD_ID1).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0),
        StreamPartition.of(STREAM, SHARD_ID1)
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        100,
        5000,
        5000,
        1_000_000,
        true,
        true
    );

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(SHARD_ID1, SHARD_ID0), recordSupplier.getPartitionIds(STREAM));

    // calling poll would start background fetch if seek was called, but will instead be skipped and the results
    // empty
    Assert.assertEquals(Collections.emptyList(), recordSupplier.poll(100));

    verifyAll();

    final ListShardsRequest expectedRequest0 = ListShardsRequest.builder()
        .build();
    expectedRequest0 = expectedRequest0.toBuilder().streamName(STREAM).build();
    Assert.assertEquals(expectedRequest0, capturedRequest0.getValue());

    final ListShardsRequest expectedRequest1 = ListShardsRequest.builder()
        .build();
    expectedRequest1 = expectedRequest1.toBuilder().nextToken(nextToken).build();
    Assert.assertEquals(expectedRequest1, capturedRequest1.getValue());
  }

  private static GetRecordsRequest generateGetRecordsReq(String shardIterator)
  {
    return GetRecordsRequest.builder().shardIterator(shardIterator)
        .build();
  }

  private static GetRecordsRequest generateGetRecordsWithLimitReq(String shardIterator, int limit)
  {
    return GetRecordsRequest.builder().shardIterator(shardIterator).limit(limit)
        .build();
  }

  // filter out EOS markers
  private static List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> cleanRecords(List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> records)
  {
    return records.stream()
                  .filter(x -> !x.getSequenceNumber()
                                 .equals(END_OF_SHARD_MARKER))
                  .collect(Collectors.toList());
  }

  @Test
  public void testPollWithKinesisInternalFailure() throws InterruptedException
  {
    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID0),
            EasyMock.anyString(),
            EasyMock.anyString()
    )).andReturn(
            getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID1),
            EasyMock.anyString(),
            EasyMock.anyString()
    )).andReturn(
            getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.shardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(getShardIteratorResult1.shardIterator()).andReturn(SHARD1_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR)))
            .andReturn(getRecordsResult1)
            .anyTimes();
    AwsServiceException getException = new AwsServiceException("InternalFailure");
    getException.setErrorCode("InternalFailure");
    getException.setStatusCode(500);
    getException.setServiceName("AmazonKinesis");
    EasyMock.expect(getRecordsResult0.records()).andThrow(getException).once();
    EasyMock.expect(getRecordsResult0.records()).andReturn(SHARD0_RECORDS).once();
    AwsServiceException getException2 = new AwsServiceException("InternalFailure");
    getException2.setErrorCode("InternalFailure");
    getException2.setStatusCode(503);
    getException2.setServiceName("AmazonKinesis");
    EasyMock.expect(getRecordsResult1.records()).andThrow(getException2).once();
    EasyMock.expect(getRecordsResult1.records()).andReturn(SHARD1_RECORDS).once();
    EasyMock.expect(getRecordsResult0.nextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult1.nextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.millisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult0.millisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult1.millisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult1.millisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
            StreamPartition.of(STREAM, SHARD_ID0),
            StreamPartition.of(STREAM, SHARD_ID1)
    );


    recordSupplier = new KinesisRecordSupplier(
            kinesis,
            0,
            2,
            10_000,
            5000,
            5000,
        1_000_000,
            true,
            false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    while (recordSupplier.bufferSize() < 14) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> polledRecords = cleanRecords(recordSupplier.poll(
            POLL_TIMEOUT_MILLIS));

    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS));
    Assert.assertEquals(SHARDS_LAG_MILLIS, recordSupplier.getPartitionResourcesTimeLag());
  }

  @Test
  public void testPollWithKinesisNonRetryableFailure() throws InterruptedException
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    AwsServiceException getException = new AwsServiceException("BadRequest");
    getException.setErrorCode("BadRequest");
    getException.setStatusCode(400);
    getException.setServiceName("AmazonKinesis");
    EasyMock.expect(getShardIteratorResult0.shardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
            .andThrow(getException)
            .once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0)
    );


    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        1,
        100,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    int count = 0;
    while (recordSupplier.isAnyFetchActive() && count++ < 10) {
      Thread.sleep(100);
    }
    Assert.assertFalse(recordSupplier.isAnyFetchActive());

    List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> polledRecords = cleanRecords(recordSupplier.poll(
        POLL_TIMEOUT_MILLIS));

    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(0, polledRecords.size());
  }

  @Test
  public void testSeek()
      throws InterruptedException
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.shardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(getShardIteratorResult1.shardIterator()).andReturn(SHARD1_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR)))
            .andReturn(getRecordsResult1)
            .anyTimes();
    EasyMock.expect(getRecordsResult0.records()).andReturn(SHARD0_RECORDS.subList(1, SHARD0_RECORDS.size())).once();
    EasyMock.expect(getRecordsResult1.records()).andReturn(SHARD1_RECORDS.subList(2, SHARD1_RECORDS.size())).once();
    EasyMock.expect(getRecordsResult0.nextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult1.nextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.millisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult1.millisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).once();

    replayAll();

    StreamPartition<String> shard0Partition = StreamPartition.of(STREAM, SHARD_ID0);
    StreamPartition<String> shard1Partition = StreamPartition.of(STREAM, SHARD_ID1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard0Partition,
        shard1Partition
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seek(shard1Partition, SHARD1_RECORDS.get(2).sequenceNumber());
    recordSupplier.seek(shard0Partition, SHARD0_RECORDS.get(1).sequenceNumber());
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 9; i++) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> polledRecords = cleanRecords(recordSupplier.poll(
        POLL_TIMEOUT_MILLIS));

    verifyAll();
    Assert.assertEquals(9, polledRecords.size());
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS.subList(4, 12)));
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS.subList(1, 2)));
    Assert.assertEquals(SHARDS_LAG_MILLIS, recordSupplier.getPartitionResourcesTimeLag());
  }


  @Test
  public void testSeekToLatest()
      throws InterruptedException
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.shardIterator()).andReturn(null).once();
    EasyMock.expect(getShardIteratorResult1.shardIterator()).andReturn(null).once();

    replayAll();

    StreamPartition<String> shard0 = StreamPartition.of(STREAM, SHARD_ID0);
    StreamPartition<String> shard1 = StreamPartition.of(STREAM, SHARD_ID1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard0,
        shard1
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        100,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToLatest(partitions);
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 2; i++) {
      Thread.sleep(100);
    }
    Assert.assertEquals(Collections.emptyList(), cleanRecords(recordSupplier.poll(POLL_TIMEOUT_MILLIS)));

    verifyAll();
  }

  @Test(expected = ISE.class)
  public void testSeekUnassigned() throws InterruptedException
  {
    StreamPartition<String> shard0 = StreamPartition.of(STREAM, SHARD_ID0);
    StreamPartition<String> shard1 = StreamPartition.of(STREAM, SHARD_ID1);
    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        shard1
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        100,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(Collections.singleton(shard0));
  }


  @Test
  public void testPollAfterSeek()
      throws InterruptedException
  {
    // tests that after doing a seek, the now invalid records in buffer is cleaned up properly

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.eq("5")
    )).andReturn(
        getShardIteratorResult1).once();

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.eq("7")
    )).andReturn(getShardIteratorResult0)
            .once();

    EasyMock.expect(getShardIteratorResult1.shardIterator()).andReturn(SHARD1_ITERATOR).once();
    EasyMock.expect(getShardIteratorResult0.shardIterator()).andReturn(SHARD0_ITERATOR).once();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR)))
            .andReturn(getRecordsResult1)
            .once();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
            .andReturn(getRecordsResult0)
            .once();
    EasyMock.expect(getRecordsResult1.records()).andReturn(SHARD1_RECORDS.subList(5, SHARD1_RECORDS.size())).once();
    EasyMock.expect(getRecordsResult0.records()).andReturn(SHARD1_RECORDS.subList(7, SHARD1_RECORDS.size())).once();
    EasyMock.expect(getRecordsResult1.nextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.nextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.millisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult1.millisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID1)
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seek(StreamPartition.of(STREAM, SHARD_ID1), "5");
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 6; i++) {
      Thread.sleep(100);
    }

    OrderedPartitionableRecord<String, String, KinesisRecordEntity> firstRecord = recordSupplier.poll(POLL_TIMEOUT_MILLIS).get(0);

    Assert.assertEquals(
        ALL_RECORDS.get(7),
        firstRecord
    );

    // only one partition in this test. first results come from getRecordsResult1, which has SHARD1_LAG_MILLIS
    Assert.assertEquals(ImmutableMap.of(SHARD_ID1, SHARD1_LAG_MILLIS), recordSupplier.getPartitionResourcesTimeLag());

    recordSupplier.seek(StreamPartition.of(STREAM, SHARD_ID1), "7");
    recordSupplier.start();

    while (recordSupplier.bufferSize() < 4) {
      Thread.sleep(100);
    }


    OrderedPartitionableRecord<String, String, KinesisRecordEntity> record2 = recordSupplier.poll(POLL_TIMEOUT_MILLIS).get(0);

    Assert.assertEquals(ALL_RECORDS.get(9), record2);
    // only one partition in this test. second results come from getRecordsResult0, which has SHARD0_LAG_MILLIS
    Assert.assertEquals(ImmutableMap.of(SHARD_ID1, SHARD0_LAG_MILLIS), recordSupplier.getPartitionResourcesTimeLag());
    verifyAll();
  }


  @Test
  public void testPollDeaggregate() throws InterruptedException
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID1),
        EasyMock.anyString(),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.shardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(getShardIteratorResult1.shardIterator()).andReturn(SHARD1_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR)))
            .andReturn(getRecordsResult1)
            .anyTimes();
    EasyMock.expect(getRecordsResult0.records()).andReturn(SHARD0_RECORDS).once();
    EasyMock.expect(getRecordsResult1.records()).andReturn(SHARD1_RECORDS).once();
    EasyMock.expect(getRecordsResult0.nextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult1.nextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.millisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).once();
    EasyMock.expect(getRecordsResult1.millisBehindLatest()).andReturn(SHARD1_LAG_MILLIS).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0),
        StreamPartition.of(STREAM, SHARD_ID1)
    );


    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 12; i++) {
      Thread.sleep(100);
    }

    List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> polledRecords = cleanRecords(recordSupplier.poll(
        POLL_TIMEOUT_MILLIS));

    verifyAll();

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertTrue(polledRecords.containsAll(ALL_RECORDS));
    Assert.assertEquals(SHARDS_LAG_MILLIS, recordSupplier.getPartitionResourcesTimeLag());
  }

  @Test
  public void getLatestSequenceNumberWhenShardIsEmptyShouldReturnUnreadToken()
  {

    KinesisRecordSupplier recordSupplier = getSequenceNumberWhenNoRecordsHelperForOpenShard();
    Assert.assertEquals(KinesisSequenceNumber.UNREAD_LATEST,
                        recordSupplier.getLatestSequenceNumber(StreamPartition.of(STREAM, SHARD_ID0)));
    verifyAll();
  }

  @Test
  public void getEarliestSequenceNumberWhenShardIsEmptyShouldReturnUnreadToken()
  {

    KinesisRecordSupplier recordSupplier = getSequenceNumberWhenNoRecordsHelperForOpenShard();
    Assert.assertEquals(KinesisSequenceNumber.UNREAD_TRIM_HORIZON,
                        recordSupplier.getEarliestSequenceNumber(StreamPartition.of(STREAM, SHARD_ID0)));
    verifyAll();
  }

  @Test
  public void getLatestSequenceNumberWhenKinesisRetryableException()
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.eq(STREAM),
        EasyMock.eq(SHARD_ID0),
        EasyMock.eq(ShardIteratorType.LATEST.toString())
    )).andReturn(
        getShardIteratorResult0).once();

    EasyMock.expect(getShardIteratorResult0.shardIterator()).andReturn(SHARD0_ITERATOR).once();

    SdkException ex = new SdkException(new IOException());
    EasyMock.expect(kinesis.getRecords(generateGetRecordsWithLimitReq(SHARD0_ITERATOR, 1000)))
            .andThrow(ex)
            .andReturn(getRecordsResult0)
            .once();

    EasyMock.expect(getRecordsResult0.records()).andReturn(SHARD0_RECORDS).once();
    EasyMock.expect(getRecordsResult0.nextShardIterator()).andReturn(null).once();
    EasyMock.expect(getRecordsResult0.millisBehindLatest()).andReturn(0L).once();

    replayAll();

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    Assert.assertEquals("0", recordSupplier.getLatestSequenceNumber(StreamPartition.of(STREAM, SHARD_ID0)));
  }

  private KinesisRecordSupplier getSequenceNumberWhenNoRecordsHelperForOpenShard()
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.eq(STREAM),
        EasyMock.eq(SHARD_ID0),
        EasyMock.anyString()
    )).andReturn(
        getShardIteratorResult0).times(1);

    EasyMock.expect(getShardIteratorResult0.shardIterator()).andReturn(SHARD0_ITERATOR).times(1);

    EasyMock.expect(kinesis.getRecords(generateGetRecordsWithLimitReq(SHARD0_ITERATOR, 1000)))
            .andReturn(getRecordsResult0)
            .times(1);

    EasyMock.expect(getRecordsResult0.records()).andReturn(Collections.emptyList()).times(1);
    EasyMock.expect(getRecordsResult0.nextShardIterator()).andReturn(SHARD0_ITERATOR).times(1);

    replayAll();

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );
    return recordSupplier;
  }

  @Test
  public void getPartitionTimeLag() throws InterruptedException
  {
    EasyMock.expect(kinesis.getShardIterator(
        EasyMock.anyObject(),
        EasyMock.eq(SHARD_ID0),
            EasyMock.eq(ShardIteratorType.TRIM_HORIZON.toString()),
            EasyMock.or(EasyMock.matches("\\d+"), EasyMock.isNull())
    )).andReturn(getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID0),
            EasyMock.eq(ShardIteratorType.AT_SEQUENCE_NUMBER.toString()),
            EasyMock.matches("\\d+")
    )).andReturn(getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID0),
            EasyMock.eq(ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString()),
            EasyMock.matches("\\d+")
    )).andReturn(getShardIteratorResult0).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID1),
            EasyMock.eq(ShardIteratorType.TRIM_HORIZON.toString()),
            EasyMock.or(EasyMock.matches("\\d+"), EasyMock.isNull())
    )).andReturn(getShardIteratorResult1).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID1),
            EasyMock.eq(ShardIteratorType.AT_SEQUENCE_NUMBER.toString()),
            EasyMock.matches("\\d+")
    )).andReturn(getShardIteratorResult1).anyTimes();

    EasyMock.expect(kinesis.getShardIterator(
            EasyMock.anyObject(),
            EasyMock.eq(SHARD_ID1),
            EasyMock.eq(ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString()),
            EasyMock.matches("\\d+")
    )).andReturn(getShardIteratorResult1).anyTimes();

    EasyMock.expect(getShardIteratorResult0.shardIterator()).andReturn(SHARD0_ITERATOR).anyTimes();
    EasyMock.expect(getShardIteratorResult1.shardIterator()).andReturn(SHARD1_ITERATOR).anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD0_ITERATOR)))
            .andReturn(getRecordsResult0)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsReq(SHARD1_ITERATOR)))
            .andReturn(getRecordsResult1)
            .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsWithLimitReq(SHARD0_ITERATOR, 1)))
        .andReturn(getRecordsResult0)
        .anyTimes();
    EasyMock.expect(kinesis.getRecords(generateGetRecordsWithLimitReq(SHARD1_ITERATOR, 1)))
        .andReturn(getRecordsResult1)
        .anyTimes();
    EasyMock.expect(getRecordsResult0.records()).andReturn(SHARD0_RECORDS).times(2);
    EasyMock.expect(getRecordsResult1.records()).andReturn(SHARD1_RECORDS_EMPTY).times(2);
    EasyMock.expect(getRecordsResult0.nextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult1.nextShardIterator()).andReturn(null).anyTimes();
    EasyMock.expect(getRecordsResult0.millisBehindLatest()).andReturn(SHARD0_LAG_MILLIS).times(2);
    EasyMock.expect(getRecordsResult1.millisBehindLatest()).andReturn(SHARD1_LAG_MILLIS_EMPTY).once();

    replayAll();

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(STREAM, SHARD_ID0),
        StreamPartition.of(STREAM, SHARD_ID1)
    );

    recordSupplier = new KinesisRecordSupplier(
        kinesis,
        0,
        2,
        10_000,
        5000,
        5000,
        1_000_000,
        true,
        false
    );

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);
    recordSupplier.start();

    for (int i = 0; i < 10 && recordSupplier.bufferSize() < 12; i++) {
      Thread.sleep(100);
    }

    Map<String, Long> timeLag = recordSupplier.getPartitionResourcesTimeLag();


    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(SHARDS_LAG_MILLIS_EMPTY, timeLag);

    Map<String, String> offsets = ImmutableMap.of(
        SHARD_ID1, SHARD1_RECORDS.get(0).sequenceNumber(),
        SHARD_ID0, SHARD0_RECORDS.get(0).sequenceNumber()
    );
    Map<String, Long> independentTimeLag = recordSupplier.getPartitionsTimeLag(STREAM, offsets);
    Assert.assertEquals(SHARDS_LAG_MILLIS_EMPTY, independentTimeLag);

    // Verify that kinesis apis are not called for custom sequence numbers
    for (String sequenceNum : Arrays.asList(NO_END_SEQUENCE_NUMBER, END_OF_SHARD_MARKER, EXPIRED_MARKER,
                                            UNREAD_LATEST, UNREAD_TRIM_HORIZON)) {
      offsets = ImmutableMap.of(
          SHARD_ID1, sequenceNum,
          SHARD_ID0, sequenceNum
      );

      Map<String, Long> zeroOffsets = ImmutableMap.of(
          SHARD_ID1, 0L,
          SHARD_ID0, 0L
      );

      Assert.assertEquals(zeroOffsets, recordSupplier.getPartitionsTimeLag(STREAM, offsets));
    }
    verifyAll();
  }

  @Test
  public void testIsOffsetAvailable()
  {
    KinesisClient mockKinesis = EasyMock.mock(KinesisClient.class);
    KinesisRecordSupplier target = new KinesisRecordSupplier(
        mockKinesis,
        0,
        2,
        100,
        5000,
        5000,
        1_000_000,
        true,
        false
    );
    StreamPartition<String> partition = new StreamPartition<>(STREAM, SHARD_ID0);

    setupMockKinesisForShardId(mockKinesis, SHARD_ID0, null,
                               ShardIteratorType.AT_SEQUENCE_NUMBER, "-1",
                               Collections.emptyList(), "whatever");

    Record record0 = Record.builder().sequenceNumber("5")
        .build();
    setupMockKinesisForShardId(mockKinesis, SHARD_ID0, null,
                               ShardIteratorType.AT_SEQUENCE_NUMBER, "0",
                               Collections.singletonList(record0), "whatever");

    Record record10 = Record.builder().sequenceNumber("10")
        .build();
    setupMockKinesisForShardId(mockKinesis, SHARD_ID0, null,
                               ShardIteratorType.AT_SEQUENCE_NUMBER, "10",
                               Collections.singletonList(record10), "whatever");

    EasyMock.replay(mockKinesis);

    Assert.assertTrue(target.isOffsetAvailable(partition, KinesisSequenceNumber.of(UNREAD_TRIM_HORIZON)));

    Assert.assertFalse(target.isOffsetAvailable(partition, KinesisSequenceNumber.of(END_OF_SHARD_MARKER)));

    Assert.assertFalse(target.isOffsetAvailable(partition, KinesisSequenceNumber.of("-1")));

    Assert.assertFalse(target.isOffsetAvailable(partition, KinesisSequenceNumber.of("0")));

    Assert.assertTrue(target.isOffsetAvailable(partition, KinesisSequenceNumber.of("10")));
  }

  private void setupMockKinesisForShardId(KinesisClient kinesis, String shardId,
                                          List<Record> records, String nextIterator)
  {
    setupMockKinesisForShardId(kinesis, shardId, 1, ShardIteratorType.TRIM_HORIZON, null, records, nextIterator);
  }

  private void setupMockKinesisForShardId(KinesisClient kinesis, String shardId, Integer limit,
                                          ShardIteratorType iteratorType, String sequenceNumber,
                                          List<Record> records, String nextIterator)
  {
    String shardIteratorType = iteratorType.toString();
    String shardIterator = "shardIterator" + shardId;
    if (sequenceNumber != null) {
      shardIterator += sequenceNumber;
    }
    GetShardIteratorResponse shardIteratorResult = GetShardIteratorResponse.builder().shardIterator(shardIterator)
        .build();
    if (sequenceNumber == null) {
      EasyMock.expect(kinesis.getShardIterator(STREAM, shardId, shardIteratorType))
              .andReturn(shardIteratorResult)
              .once();
    } else {
      EasyMock.expect(kinesis.getShardIterator(STREAM, shardId, shardIteratorType, sequenceNumber))
              .andReturn(shardIteratorResult)
              .once();
    }
    GetRecordsRequest request = GetRecordsRequest.builder().shardIterator(shardIterator)
        .limit(limit)
        .build();
    GetRecordsResponse result = GetRecordsResponse.builder().records(records)
        .nextShardIterator(nextIterator)
        .build();
    EasyMock.expect(kinesis.getRecords(request)).andReturn(result);
  }
}
