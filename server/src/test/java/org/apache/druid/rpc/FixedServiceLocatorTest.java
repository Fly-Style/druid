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

package org.apache.druid.rpc;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

public class FixedServiceLocatorTest
{
  public static final DruidServerMetadata DATA_SERVER_1 = new DruidServerMetadata(
      "TestDataServer",
      "hostName:9092",
      null,
      2,
      null,
      ServerType.REALTIME,
      "tier1",
      2
  );

  public static final DruidServerMetadata DATA_SERVER_2 = new DruidServerMetadata(
      "TestDataServer",
      "hostName:8083",
      null,
      2,
      null,
      ServerType.REALTIME,
      "tier1",
      2
  );

  private static final ServiceLocation PLAINTEXT_LOCATION = new ServiceLocation("worker1", 8100, -1, "/base");

  @Test
  public void test_constructor_rejectsNull()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new FixedServiceLocator((ServiceLocation) null)
    );

    Assertions.assertThrows(
        NullPointerException.class,
        () -> new FixedServiceLocator((ServiceLocations) null)
    );
  }

  @Test
  public void test_locate_singleServer() throws ExecutionException, InterruptedException
  {
    FixedServiceLocator serviceLocator =
        new FixedServiceLocator(ServiceLocation.fromDruidServerMetadata(DATA_SERVER_1));

    Assertions.assertEquals(
        ServiceLocations.forLocation(ServiceLocation.fromDruidServerMetadata(DATA_SERVER_1)),
        serviceLocator.locate().get()
    );
  }

  @Test
  public void test_locate_afterClose() throws ExecutionException, InterruptedException
  {
    FixedServiceLocator serviceLocator =
        new FixedServiceLocator(ServiceLocation.fromDruidServerMetadata(DATA_SERVER_1));

    serviceLocator.close();

    Assertions.assertEquals(
        ServiceLocations.closed(),
        serviceLocator.locate().get()
    );
  }

  @Test
  public void test_locate_multipleServers() throws ExecutionException, InterruptedException
  {
    final ServiceLocations locations = ServiceLocations.forLocations(
        ImmutableSet.of(
            ServiceLocation.fromDruidServerMetadata(DATA_SERVER_1),
            ServiceLocation.fromDruidServerMetadata(DATA_SERVER_2)
        )
    );

    FixedServiceLocator serviceLocator = new FixedServiceLocator(locations);
    Assertions.assertEquals(locations, serviceLocator.locate().get());
  }

  @Test
  public void test_constructorWithDiscovery_rejectsNull()
  {
    final TestDiscovery discovery = new TestDiscovery();

    Assertions.assertThrows(
        NullPointerException.class,
        () -> new FixedServiceLocator(null, discovery, Duration.ZERO)
    );

    Assertions.assertThrows(
        NullPointerException.class,
        () -> new FixedServiceLocator(PLAINTEXT_LOCATION, null, Duration.ZERO)
    );

    Assertions.assertThrows(
        NullPointerException.class,
        () -> new FixedServiceLocator(PLAINTEXT_LOCATION, discovery, null)
    );
  }

  @Test
  public void test_locate_withDiscovery_announcedService() throws ExecutionException, InterruptedException
  {
    final TestDiscovery discovery = new TestDiscovery();
    discovery.announce(plaintextNode("worker1", 8100));
    final FixedServiceLocator serviceLocator = new FixedServiceLocator(PLAINTEXT_LOCATION, discovery, Duration.ZERO);

    Assertions.assertEquals(ServiceLocations.forLocation(PLAINTEXT_LOCATION), serviceLocator.locate().get());
  }

  @Test
  public void test_locate_withDiscovery_announcedTlsService() throws ExecutionException, InterruptedException
  {
    final ServiceLocation tlsLocation = new ServiceLocation("worker1", -1, 8101, "/base");
    final TestDiscovery discovery = new TestDiscovery();
    discovery.announce(tlsNode("worker1", 8100, 8101));
    final FixedServiceLocator serviceLocator = new FixedServiceLocator(tlsLocation, discovery, Duration.ZERO);

    Assertions.assertEquals(ServiceLocations.forLocation(tlsLocation), serviceLocator.locate().get());
  }

  @Test
  public void test_locate_withDiscovery_unannouncedServiceDuringGracePeriod()
      throws ExecutionException, InterruptedException
  {
    final FixedServiceLocator serviceLocator = new FixedServiceLocator(
        PLAINTEXT_LOCATION,
        new TestDiscovery(),
        FixedServiceLocator.DISCOVERY_GRACE_PERIOD
    );

    Assertions.assertEquals(ServiceLocations.forLocation(PLAINTEXT_LOCATION), serviceLocator.locate().get());
  }

  @Test
  public void test_locate_withDiscovery_unannouncedServiceAfterGracePeriod()
      throws ExecutionException, InterruptedException
  {
    final TestDiscovery discovery = new TestDiscovery();

    // A different service on the same host does not count.
    discovery.announce(plaintextNode("worker1", 8101));
    final FixedServiceLocator serviceLocator = new FixedServiceLocator(PLAINTEXT_LOCATION, discovery, Duration.ZERO);

    Assertions.assertEquals(ServiceLocations.closed(), serviceLocator.locate().get());
  }

  @Test
  public void test_locate_withDiscovery_serviceGoesAway() throws ExecutionException, InterruptedException
  {
    final TestDiscovery discovery = new TestDiscovery();
    final DiscoveryDruidNode node = plaintextNode("worker1", 8100);
    discovery.announce(node);
    final FixedServiceLocator serviceLocator = new FixedServiceLocator(PLAINTEXT_LOCATION, discovery, Duration.ZERO);

    Assertions.assertEquals(ServiceLocations.forLocation(PLAINTEXT_LOCATION), serviceLocator.locate().get());

    discovery.unannounce(node);
    Assertions.assertEquals(ServiceLocations.closed(), serviceLocator.locate().get());
  }

  @Test
  public void test_locate_withDiscovery_staysClosedWhenServiceComesBack()
      throws ExecutionException, InterruptedException
  {
    final TestDiscovery discovery = new TestDiscovery();
    final FixedServiceLocator serviceLocator = new FixedServiceLocator(PLAINTEXT_LOCATION, discovery, Duration.ZERO);

    Assertions.assertEquals(ServiceLocations.closed(), serviceLocator.locate().get());

    discovery.announce(plaintextNode("worker1", 8100));
    Assertions.assertEquals(ServiceLocations.closed(), serviceLocator.locate().get());
  }

  @Test
  public void test_locate_withDiscovery_afterClose() throws ExecutionException, InterruptedException
  {
    final TestDiscovery discovery = new TestDiscovery();
    discovery.announce(plaintextNode("worker1", 8100));
    final FixedServiceLocator serviceLocator = new FixedServiceLocator(PLAINTEXT_LOCATION, discovery, Duration.ZERO);

    serviceLocator.close();

    Assertions.assertEquals(ServiceLocations.closed(), serviceLocator.locate().get());
  }

  private static DiscoveryDruidNode plaintextNode(final String host, final int plaintextPort)
  {
    return new DiscoveryDruidNode(
        new DruidNode("worker", host, false, plaintextPort, null, true, false),
        NodeRole.HISTORICAL,
        Map.of()
    );
  }

  private static DiscoveryDruidNode tlsNode(final String host, final int plaintextPort, final int tlsPort)
  {
    return new DiscoveryDruidNode(
        new DruidNode("worker", host, false, plaintextPort, tlsPort, true, true),
        NodeRole.HISTORICAL,
        Map.of()
    );
  }

  private static class TestDiscovery implements DruidNodeDiscovery
  {
    private final List<DiscoveryDruidNode> nodes = new CopyOnWriteArrayList<>();

    void announce(final DiscoveryDruidNode node)
    {
      nodes.add(node);
    }

    void unannounce(final DiscoveryDruidNode node)
    {
      nodes.remove(node);
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      return nodes;
    }

    @Override
    public void registerListener(final Listener listener)
    {
      throw new UnsupportedOperationException();
    }
  }
}
