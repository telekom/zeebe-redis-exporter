/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.zeebe.redis.testcontainers.camunda;

import io.camunda.zeebe.client.ZeebeClient;
import io.lettuce.core.RedisClient;

public class RedisTestContext extends TestContext<RedisTestContext> {

  private boolean tenantStreamsEnabled;
  private String enabledTenants;

  private RedisClient redisClient;
  private ZeebeClient zeebeClient;

  public boolean isTenantStreamsEnabled() {
    return tenantStreamsEnabled;
  }

  public RedisTestContext setTenantStreamsEnabled(final boolean tenantStreamsEnabled) {
    this.tenantStreamsEnabled = tenantStreamsEnabled;
    return this;
  }

  public String getEnabledTenants() {
    return enabledTenants;
  }

  public RedisTestContext setEnabledTenants(final String enabledTenants) {
    this.enabledTenants = enabledTenants;
    return this;
  }

  public RedisClient getRedisClient() {
    return redisClient;
  }

  public RedisTestContext setRedisClient(final RedisClient redisClient) {
    this.redisClient = redisClient;
    return this;
  }

  public ZeebeClient getZeebeClient() {
    return zeebeClient;
  }

  public RedisTestContext setZeebeClient(final ZeebeClient zeebeClient) {
    this.zeebeClient = zeebeClient;
    return this;
  }
}
