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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;

/**
 * Locator for a fixed set of {@link ServiceLocations}.
 * <p>
 * Optionally, the locator can also check a {@link DruidNodeDiscovery} on each {@link #locate()} call.
 * In this mode, the locator closes itself when the service is no longer announced.
 * This lets clients stop retrying a server that has gone away, if nothing calls {@link #close()}.
 */
public class FixedServiceLocator implements ServiceLocator
{
  public static final Duration DISCOVERY_GRACE_PERIOD = Duration.standardMinutes(5);

  private static final Logger log = new Logger(FixedServiceLocator.class);

  private final ServiceLocations locations;

  /**
   * Discovery to check on each call to {@link #locate()}. Null if this locator does not check discovery.
   */
  @Nullable
  private final DruidNodeDiscovery discovery;

  @Nullable
  private final String hostAndPort;

  @Nullable
  private final DateTime gracePeriodEnd;

  private volatile boolean closed = false;

  public FixedServiceLocator(final ServiceLocations locations)
  {
    this.locations = Preconditions.checkNotNull(locations);
    this.discovery = null;
    this.hostAndPort = null;
    this.gracePeriodEnd = null;
  }

  public FixedServiceLocator(final ServiceLocation location)
  {
    this(ServiceLocations.forLocation(location));
  }

  /**
   * Create a locator that closes itself when the service at {@code location} is no longer announced in the discovery.
   */
  public FixedServiceLocator(
      final ServiceLocation location,
      final DruidNodeDiscovery discovery,
      final Duration gracePeriod
  )
  {
    this.locations = ServiceLocations.forLocation(Preconditions.checkNotNull(location, "location"));
    this.discovery = Preconditions.checkNotNull(discovery, "discovery");
    this.hostAndPort = location.getHostAndPort();
    this.gracePeriodEnd = DateTimes.nowUtc().plus(Preconditions.checkNotNull(gracePeriod, "gracePeriod"));
  }

  @Override
  public ListenableFuture<ServiceLocations> locate()
  {
    if (!closed && discovery != null && isGracePeriodOver() && !isAnnounced()) {
      log.warn("Service[%s] is no longer announced in discovery. Closing its locator.", hostAndPort);
      close();
    }

    if (closed) {
      return Futures.immediateFuture(ServiceLocations.closed());
    } else {
      return Futures.immediateFuture(locations);
    }
  }

  @Override
  public void close()
  {
    closed = true;
  }

  private boolean isGracePeriodOver()
  {
    return !DateTimes.nowUtc().isBefore(gracePeriodEnd);
  }

  private boolean isAnnounced()
  {
    return discovery.getAllNodes()
                    .stream()
                    .anyMatch(node -> hostAndPort.equals(node.getDruidNode().getHostAndPortToUse()));
  }
}
