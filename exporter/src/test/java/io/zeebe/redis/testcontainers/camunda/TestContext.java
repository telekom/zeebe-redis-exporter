/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.zeebe.redis.testcontainers.camunda;

import io.camunda.zeebe.client.ZeebeClientBuilder;
import java.io.File;
import org.testcontainers.containers.Network;

public class TestContext<T extends TestContext<T>> {

  private File zeebeDataFolder;
  private Network network;

  private String externalPostgresHost;
  private int externalPostgresPort;
  private String internalPostgresHost;
  private int internalPostgresPort;

  private String externalIdentityHost;
  private int externalIdentityPort;
  private String internalIdentityHost;
  private int internalIdentityPort;

  private String externalRedisHost;
  private Integer externalRedisPort;
  private String internalRedisHost;
  private Integer internalRedisPort;

  private String externalKeycloakHost;
  private int externalKeycloakPort;
  private String internalKeycloakHost;
  private int internalKeycloakPort;

  private String externalZeebeContactPoint;
  private String internalZeebeContactPoint;

  private Integer partitionCount;
  private Boolean multitenancyEnabled;

  private ZeebeClientBuilder zeebeClientBuilder;

  public File getZeebeDataFolder() {
    return zeebeDataFolder;
  }

  public T setZeebeDataFolder(final File zeebeDataFolder) {
    this.zeebeDataFolder = zeebeDataFolder;
    return (T) this;
  }

  public Network getNetwork() {
    return network;
  }

  public T setNetwork(final Network network) {
    this.network = network;
    return (T) this;
  }

  public String getExternalPostgresHost() {
    return externalPostgresHost;
  }

  public void setExternalPostgresHost(String externalPostgresHost) {
    this.externalPostgresHost = externalPostgresHost;
  }

  public int getExternalPostgresPort() {
    return externalPostgresPort;
  }

  public void setExternalPostgresPort(int externalPostgresPort) {
    this.externalPostgresPort = externalPostgresPort;
  }

  public String getInternalPostgresHost() {
    return internalPostgresHost;
  }

  public void setInternalPostgresHost(String internalPostgresHost) {
    this.internalPostgresHost = internalPostgresHost;
  }

  public int getInternalPostgresPort() {
    return internalPostgresPort;
  }

  public void setInternalPostgresPort(int internalPostgresPort) {
    this.internalPostgresPort = internalPostgresPort;
  }

  public String getExternalIdentityHost() {
    return externalIdentityHost;
  }

  public void setExternalIdentityHost(String externalIdentityHost) {
    this.externalIdentityHost = externalIdentityHost;
  }

  public int getExternalIdentityPort() {
    return externalIdentityPort;
  }

  public void setExternalIdentityPort(int externalIdentityPort) {
    this.externalIdentityPort = externalIdentityPort;
  }

  public String getInternalIdentityHost() {
    return internalIdentityHost;
  }

  public void setInternalIdentityHost(String internalIdentityHost) {
    this.internalIdentityHost = internalIdentityHost;
  }

  public int getInternalIdentityPort() {
    return internalIdentityPort;
  }

  public void setInternalIdentityPort(int internalIdentityPort) {
    this.internalIdentityPort = internalIdentityPort;
  }

  public String getExternalRedisHost() {
    return externalRedisHost;
  }

  public T setExternalRedisHost(final String externalRedisHost) {
    this.externalRedisHost = externalRedisHost;
    return (T) this;
  }

  public Integer getExternalRedisPort() {
    return externalRedisPort;
  }

  public T setExternalRedisPort(final Integer externalRedisPort) {
    this.externalRedisPort = externalRedisPort;
    return (T) this;
  }

  public String getInternalRedisHost() {
    return internalRedisHost;
  }

  public T setInternalRedisHost(final String internalRedisHost) {
    this.internalRedisHost = internalRedisHost;
    return (T) this;
  }

  public Integer getInternalRedisPort() {
    return internalRedisPort;
  }

  public T setInternalRedisPort(final Integer internalRedisPort) {
    this.internalRedisPort = internalRedisPort;
    return (T) this;
  }

  public String getExternalKeycloakHost() {
    return externalKeycloakHost;
  }

  public void setExternalKeycloakHost(String externalKeycloakHost) {
    this.externalKeycloakHost = externalKeycloakHost;
  }

  public int getExternalKeycloakPort() {
    return externalKeycloakPort;
  }

  public void setExternalKeycloakPort(int externalKeycloakPort) {
    this.externalKeycloakPort = externalKeycloakPort;
  }

  public String getInternalKeycloakHost() {
    return internalKeycloakHost;
  }

  public void setInternalKeycloakHost(String internalKeycloakHost) {
    this.internalKeycloakHost = internalKeycloakHost;
  }

  public int getInternalKeycloakPort() {
    return internalKeycloakPort;
  }

  public void setInternalKeycloakPort(int internalKeycloakPort) {
    this.internalKeycloakPort = internalKeycloakPort;
  }

  public String getExternalZeebeContactPoint() {
    return externalZeebeContactPoint;
  }

  public T setExternalZeebeContactPoint(final String externalZeebeContactPoint) {
    this.externalZeebeContactPoint = externalZeebeContactPoint;
    return (T) this;
  }

  public String getInternalZeebeContactPoint() {
    return internalZeebeContactPoint;
  }

  public T setInternalZeebeContactPoint(final String internalZeebeContactPoint) {
    this.internalZeebeContactPoint = internalZeebeContactPoint;
    return (T) this;
  }

  public Integer getPartitionCount() {
    return partitionCount;
  }

  public TestContext<T> setPartitionCount(final Integer partitionCount) {
    this.partitionCount = partitionCount;
    return this;
  }

  public Boolean isMultitenancyEnabled() {
    return multitenancyEnabled;
  }

  public TestContext<T> setMultitenancyEnabled(final Boolean multitenancyEnabled) {
    this.multitenancyEnabled = multitenancyEnabled;
    return this;
  }

  public String getInternalKeycloakBaseUrl() {
    return "http://" + getInternalKeycloakHost() + ":" + getInternalKeycloakPort();
  }

  public String getInternalIdentityBaseUrl() {
    return String.format("http://%s:%d", internalIdentityHost, internalIdentityPort);
  }

  public String getExternalKeycloakBaseUrl() {
    return "http://" + getExternalKeycloakHost() + ":" + getExternalKeycloakPort();
  }

  public ZeebeClientBuilder getZeebeClientBuilder() {
    return zeebeClientBuilder;
  }

  public TestContext<T> setZeebeClientBuilder(final ZeebeClientBuilder zeebeClientBuilder) {
    this.zeebeClientBuilder = zeebeClientBuilder;
    return this;
  }
}
