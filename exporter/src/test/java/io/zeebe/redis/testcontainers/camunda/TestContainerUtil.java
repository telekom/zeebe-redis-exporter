/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.zeebe.redis.testcontainers.camunda;

import static org.testcontainers.images.PullPolicy.alwaysPull;

import io.camunda.zeebe.client.ZeebeClientBuilder;
import io.camunda.zeebe.client.impl.ZeebeClientBuilderImpl;
import io.camunda.zeebe.client.impl.oauth.OAuthCredentialsProviderBuilder;
import io.zeebe.containers.ZeebeContainer;
import io.zeebe.containers.ZeebePort;
import io.zeebe.containers.ZeebeTopologyWaitStrategy;
import io.zeebe.redis.testcontainers.RedisContainer;
import java.io.File;
import java.time.Duration;
import java.util.Map;
import org.keycloak.admin.client.Keycloak;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class TestContainerUtil {

  public static final String PROPERTIES_PREFIX = "camunda.operate.";
  public static final String ELS_NETWORK_ALIAS = "elasticsearch";
  // public static final int ELS_PORT = 9200;
  // public static final String ELS_HOST = "localhost";
  // public static final String ELS_SCHEME = "http";
  public static final int POSTGRES_PORT = 5432;
  public static final Integer KEYCLOAK_PORT = 8080;
  public static final Integer IDENTITY_PORT = 8082;
  public static final String IDENTITY_NETWORK_ALIAS = "identity";
  public static final String POSTGRES_NETWORK_ALIAS = "postgres";
  public static final String KEYCLOAK_NETWORK_ALIAS = "keycloak";
  public static final String KEYCLOAK_INIT_OPERATE_SECRET = "the-cake-is-alive";
  public static final String KEYCLOAK_INIT_TASKLIST_SECRET = "the-cake-is-alive";
  public static final String KEYCLOAK_USERNAME = "demo";
  public static final String KEYCLOAK_PASSWORD = "demo";
  public static final String KEYCLOAK_USERNAME_2 = "user2";
  public static final String KEYCLOAK_PASSWORD_2 = "user2";
  public static final String KEYCLOAK_USERS_0_ROLES_0 = "Identity";
  public static final String KEYCLOAK_USERS_0_ROLES_1 = "Tasklist";
  public static final String KEYCLOAK_USERS_0_ROLES_2 = "Operate";
  public static final String IDENTITY_DATABASE_HOST = "postgres";
  public static final String IDENTITY_DATABASE_NAME = "identity";
  public static final String IDENTITY_DATABASE_USERNAME = "identity";
  public static final String IDENTITY_DATABASE_PASSWORD = "iecret";
  public static final String TENANT_1 = "tenant_1";
  public static final String TENANT_2 = "tenant_2";
  private static final Logger LOGGER = LoggerFactory.getLogger(TestContainerUtil.class);
  private static final String ZEEBE = "zeebe";
  private static final String KEYCLOAK_ZEEBE_SECRET = "zecret";
  private static final String USER_MEMBER_TYPE = "USER";
  private static final String APPLICATION_MEMBER_TYPE = "APPLICATION";
  private static final String OPERATE = "operate";

  private Network network;

  private ZeebeContainer broker;
  private RedisContainer redisContainer;

  private GenericContainer<?> identityContainer;
  private GenericContainer<?> keycloakContainer;
  private PostgreSQLContainer<?> postgreSQLContainer;

  private Keycloak keycloakClient;

  public void startIdentity(
      final TestContext testContext, final String version, final boolean multiTenancyEnabled) {
    if (identityContainer != null && identityContainer.isRunning()) {
      LOGGER.info("Identity container already running; skipping start.");
      return;
    }
    LOGGER.info("Starting Identity container with image: camunda/identity:{} ", version);

    startPostgres(testContext);
    startKeyCloak(testContext);

    LOGGER.info("************ Starting Identity ************");
    identityContainer =
        new GenericContainer<>(String.format("%s:%s", "camunda/identity", version))
            .withExposedPorts(IDENTITY_PORT)
            .withNetwork(Network.SHARED)
            .withNetworkAliases(IDENTITY_NETWORK_ALIAS)
            .withEnv("CAMUNDA_IDENTITY_LICENSE.ENFORCEMENT.ENABLED", "false")
            .waitingFor(
                new HttpWaitStrategy()
                    .forPort(IDENTITY_PORT)
                    .forPath("/actuator/health")
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(5)));

    identityContainer.withEnv("SERVER_PORT", String.valueOf(IDENTITY_PORT));

    // Connect to Keycloak (using internal hostname)
    identityContainer.withEnv("KEYCLOAK_URL", testContext.getInternalKeycloakBaseUrl() + "/auth");

    // Configure Identity OAuth provider URLs
    identityContainer.withEnv(
        "IDENTITY_AUTH_PROVIDER_ISSUER_URL",
        testContext.getExternalKeycloakBaseUrl() + "/auth/realms/camunda-platform");
    identityContainer.withEnv(
        "IDENTITY_AUTH_PROVIDER_BACKEND_URL",
        testContext.getInternalKeycloakBaseUrl() + "/auth/realms/camunda-platform");

    identityContainer.withEnv("SPRING_JPA_HIBERNATE_DDL_AUTO", "update");

    // Connect Identity to Postgres DB "identity"
    identityContainer.withEnv("IDENTITY_DATABASE_HOST", IDENTITY_DATABASE_HOST);
    identityContainer.withEnv("IDENTITY_DATABASE_PORT", String.valueOf(POSTGRES_PORT));
    identityContainer.withEnv("IDENTITY_DATABASE_NAME", IDENTITY_DATABASE_NAME);
    identityContainer.withEnv("IDENTITY_DATABASE_USERNAME", IDENTITY_DATABASE_USERNAME);
    identityContainer.withEnv("IDENTITY_DATABASE_PASSWORD", IDENTITY_DATABASE_PASSWORD);

    // Keycloak related secrets and clients
    identityContainer.withEnv("KEYCLOAK_INIT_OPERATE_SECRET", KEYCLOAK_INIT_OPERATE_SECRET);
    identityContainer.withEnv("KEYCLOAK_INIT_OPERATE_ROOT_URL", "http://operate:8080");
    identityContainer.withEnv("KEYCLOAK_INIT_TASKLIST_SECRET", KEYCLOAK_INIT_TASKLIST_SECRET);
    identityContainer.withEnv("KEYCLOAK_INIT_TASKLIST_ROOT_URL", "http://tasklist:8080");
    identityContainer.withEnv("KEYCLOAK_INIT_ZEEBE_SECRET", KEYCLOAK_ZEEBE_SECRET);
    identityContainer.withEnv("KEYCLOAK_CLIENTS_0_NAME", ZEEBE);
    identityContainer.withEnv("KEYCLOAK_CLIENTS_0_ID", ZEEBE);
    identityContainer.withEnv("KEYCLOAK_CLIENTS_0_SECRET", KEYCLOAK_ZEEBE_SECRET);
    identityContainer.withEnv("KEYCLOAK_CLIENTS_0_TYPE", "M2M");
    identityContainer.withEnv("KEYCLOAK_CLIENTS_0_PERMISSIONS_0_RESOURCE_SERVER_ID", "zeebe-api");
    identityContainer.withEnv("KEYCLOAK_CLIENTS_0_PERMISSIONS_0_DEFINITION", "write:*");

    // Keycloak users and roles
    identityContainer.withEnv("KEYCLOAK_USERS_0_USERNAME", KEYCLOAK_USERNAME);
    identityContainer.withEnv("KEYCLOAK_USERS_0_PASSWORD", KEYCLOAK_PASSWORD);
    identityContainer.withEnv("KEYCLOAK_USERS_0_ROLES_0", KEYCLOAK_USERS_0_ROLES_0);
    identityContainer.withEnv("KEYCLOAK_USERS_0_ROLES_1", KEYCLOAK_USERS_0_ROLES_1);
    identityContainer.withEnv("KEYCLOAK_USERS_0_ROLES_2", KEYCLOAK_USERS_0_ROLES_2);
    identityContainer.withEnv("KEYCLOAK_USERS_1_USERNAME", KEYCLOAK_USERNAME_2);
    identityContainer.withEnv("KEYCLOAK_USERS_1_PASSWORD", KEYCLOAK_PASSWORD_2);
    identityContainer.withEnv("KEYCLOAK_USERS_1_ROLES_0", KEYCLOAK_USERS_0_ROLES_0);
    identityContainer.withEnv("KEYCLOAK_USERS_1_ROLES_1", KEYCLOAK_USERS_0_ROLES_1);
    identityContainer.withEnv("KEYCLOAK_USERS_1_ROLES_2", KEYCLOAK_USERS_0_ROLES_2);

    identityContainer.withEnv("RESOURCE_PERMISSIONS_ENABLED", "true");
    identityContainer.withEnv("MULTITENANCY_ENABLED", String.valueOf(multiTenancyEnabled));

    if (multiTenancyEnabled) {
      identityContainer.withEnv("IDENTITY_TENANTS_0_NAME", TENANT_1);
      identityContainer.withEnv("IDENTITY_TENANTS_0_TENANT_ID", TENANT_1);
      identityContainer.withEnv("IDENTITY_TENANTS_0_MEMBERS_0_TYPE", USER_MEMBER_TYPE);
      identityContainer.withEnv("IDENTITY_TENANTS_0_MEMBERS_0_USERNAME", KEYCLOAK_USERNAME);
      identityContainer.withEnv("IDENTITY_TENANTS_0_MEMBERS_1_TYPE", APPLICATION_MEMBER_TYPE);
      identityContainer.withEnv("IDENTITY_TENANTS_0_MEMBERS_1_APPLICATION_ID", ZEEBE);
      identityContainer.withEnv("IDENTITY_TENANTS_0_MEMBERS_2_TYPE", APPLICATION_MEMBER_TYPE);
      identityContainer.withEnv("IDENTITY_TENANTS_0_MEMBERS_2_APPLICATION_ID", OPERATE);

      identityContainer.withEnv("IDENTITY_TENANTS_1_NAME", TENANT_2);
      identityContainer.withEnv("IDENTITY_TENANTS_1_TENANT_ID", TENANT_2);
      identityContainer.withEnv("IDENTITY_TENANTS_1_MEMBERS_0_TYPE", USER_MEMBER_TYPE);
      identityContainer.withEnv("IDENTITY_TENANTS_1_MEMBERS_0_USERNAME", KEYCLOAK_USERNAME_2);
      identityContainer.withEnv("IDENTITY_TENANTS_1_MEMBERS_1_TYPE", APPLICATION_MEMBER_TYPE);
      identityContainer.withEnv("IDENTITY_TENANTS_1_MEMBERS_1_APPLICATION_ID", ZEEBE);
      identityContainer.withEnv("IDENTITY_TENANTS_1_MEMBERS_2_TYPE", APPLICATION_MEMBER_TYPE);
      identityContainer.withEnv("IDENTITY_TENANTS_1_MEMBERS_2_APPLICATION_ID", OPERATE);
      identityContainer.withEnv("IDENTITY_TENANTS_1_MEMBERS_3_TYPE", USER_MEMBER_TYPE);
      identityContainer.withEnv("IDENTITY_TENANTS_1_MEMBERS_3_USERNAME", KEYCLOAK_USERNAME);
    }
    identityContainer.withEnv("LOGGING_LEVEL_ROOT", "DEBUG");
    identityContainer.withEnv("LOGGING_LEVEL_IO_CAMUNDA_IDENTITY", "DEBUG");
    identityContainer.start();
    identityContainer.followOutput(new Slf4jLogConsumer(LOGGER));

    testContext.setExternalIdentityHost(identityContainer.getHost());
    testContext.setExternalIdentityPort(identityContainer.getMappedPort(IDENTITY_PORT));
    testContext.setInternalIdentityHost(IDENTITY_NETWORK_ALIAS);
    testContext.setInternalIdentityPort(IDENTITY_PORT);

    LOGGER.info(
        "************ Identity started on {}:{} ************",
        testContext.getExternalIdentityHost(),
        testContext.getExternalIdentityPort());
  }

  public void startKeyCloak(final TestContext testContext) {
    if (keycloakContainer != null && keycloakContainer.isRunning()) {
      LOGGER.info("Keycloak container already running; skipping start.");
      return;
    }
    LOGGER.info("************ Starting Keycloak ************");

    keycloakContainer =
        new GenericContainer<>(DockerImageName.parse("bitnami/keycloak:22.0.1"))
            .withExposedPorts(KEYCLOAK_PORT)
            .withEnv(
                Map.of(
                    "KEYCLOAK_HTTP_RELATIVE_PATH",
                    "/auth",
                    "KEYCLOAK_DATABASE_USER",
                    IDENTITY_DATABASE_USERNAME,
                    "KEYCLOAK_DATABASE_PASSWORD",
                    IDENTITY_DATABASE_PASSWORD,
                    "KEYCLOAK_DATABASE_NAME",
                    IDENTITY_DATABASE_NAME,
                    "KEYCLOAK_ADMIN_USER",
                    "admin",
                    "KEYCLOAK_ADMIN_PASSWORD",
                    "admin",
                    "KEYCLOAK_DATABASE_HOST",
                    POSTGRES_NETWORK_ALIAS))
            .dependsOn(postgreSQLContainer)
            .withNetwork(Network.SHARED)
            .withNetworkAliases(KEYCLOAK_NETWORK_ALIAS)
            .waitingFor(
                new HttpWaitStrategy()
                    .forPort(KEYCLOAK_PORT)
                    .forPath("/auth/")
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofSeconds(90)));
    keycloakContainer.start();

    testContext.setExternalKeycloakHost(keycloakContainer.getHost());
    testContext.setExternalKeycloakPort(keycloakContainer.getFirstMappedPort());
    testContext.setInternalKeycloakHost(KEYCLOAK_NETWORK_ALIAS);
    testContext.setInternalKeycloakPort(KEYCLOAK_PORT);

    LOGGER.info(
        "************ Keycloak started on {}:{} ************",
        testContext.getExternalKeycloakHost(),
        testContext.getExternalKeycloakPort());

    keycloakClient =
        Keycloak.getInstance(
            testContext.getExternalKeycloakBaseUrl() + "/auth",
            "master",
            "admin",
            "admin",
            "admin-cli");
  }

  public void startPostgres(final TestContext testContext) {
    if (postgreSQLContainer != null && postgreSQLContainer.isRunning()) {
      LOGGER.info("Postgres container already running; skipping start.");
      return;
    }
    LOGGER.info("************ Starting Postgres ************");

    postgreSQLContainer =
        new PostgreSQLContainer<>("postgres:14.2")
            .withNetwork(Network.SHARED)
            .withNetworkAliases(POSTGRES_NETWORK_ALIAS)
            .withDatabaseName(IDENTITY_DATABASE_NAME)
            .withUsername(IDENTITY_DATABASE_USERNAME)
            .withPassword(IDENTITY_DATABASE_PASSWORD)
            .withReuse(true)
            .withExposedPorts(POSTGRES_PORT)
            .waitingFor(new HostPortWaitStrategy());
    postgreSQLContainer.start();

    testContext.setExternalPostgresHost(postgreSQLContainer.getHost());
    testContext.setExternalPostgresPort(postgreSQLContainer.getMappedPort(POSTGRES_PORT));
    testContext.setInternalPostgresHost(POSTGRES_NETWORK_ALIAS);
    testContext.setInternalPostgresPort(POSTGRES_PORT);

    LOGGER.info(
        "************ Postgres started on {}:{} ************",
        testContext.getExternalPostgresHost(),
        testContext.getExternalPostgresPort());
  }

  public void startRedis(final TestContext testContext) {
    LOGGER.info("************ Starting Redis ************");
    redisContainer =
        new RedisContainer()
            .withNetwork(Network.SHARED)
            .withNetworkAliases(RedisContainer.ALIAS)
            .withExposedPorts(RedisContainer.PORT)
            // .withEnv("ZEEBE_REDIS_ENABLE_TENANT_STREAMS", "true") // FIXME: configurable
            .waitingFor(new HostPortWaitStrategy());
    redisContainer.start();

    testContext.setExternalRedisHost(redisContainer.getHost());
    testContext.setExternalRedisPort(redisContainer.getMappedPort(RedisContainer.PORT));
    testContext.setInternalRedisHost(RedisContainer.ALIAS);
    testContext.setInternalRedisPort(RedisContainer.PORT);
  }

  public ZeebeContainer startZeebe(
      final String version,
      final String prefix,
      final Integer partitionCount,
      final boolean multitenancyEnabled,
      final String connectionType) {
    final TestContext testContext =
        new TestContext()
            .setPartitionCount(partitionCount)
            .setMultitenancyEnabled(multitenancyEnabled);
    return startZeebe(version, testContext);
  }

  public ZeebeContainer startZeebe(final String version, final TestContext testContext) {
    if (broker == null) {
      // final String dockerRepo = ContainerVersionsUtil
      // .readProperty(ZEEBE_CURRENTVERSION_DOCKER_REPO_PROPERTY_NAME);
      // FIXME: use better method to get the docker repo
      final String dockerRepo = "ghcr.io/camunda-community-hub/zeebe-with-redis-exporter";

      LOGGER.info("************ Starting Zeebe {}:{} ************", dockerRepo, version);
      final long startTime = System.currentTimeMillis();
      broker =
          new ZeebeContainer(DockerImageName.parse(String.format("%s:%s", dockerRepo, version)));

      broker.withLogConsumer(new Slf4jLogConsumer(LOGGER));
      if (testContext.getNetwork() != null) {
        broker.withNetwork(testContext.getNetwork());
      }
      if (testContext.getZeebeDataFolder() != null) {
        broker.withFileSystemBind(
            testContext.getZeebeDataFolder().getPath(), "/usr/local/zeebe/data");
      }
      if ("SNAPSHOT".equals(version)) {
        broker.withImagePullPolicy(alwaysPull());
      }

      // from 8.3.0 onwards, Zeebe is run with a non-root user in the container;
      // this user cannot access a mounted volume that is owned by root
      broker.withCreateContainerCmdModifier(cmd -> cmd.withUser("root"));

      configureRedisExporter((RedisTestContext) testContext);

      broker
          .withEnv("JAVA_OPTS", "-Xss256k -XX:+TieredCompilation -XX:TieredStopAtLevel=1")
          .withEnv("ZEEBE_LOG_LEVEL", "DEBUG")
          .withEnv("ATOMIX_LOG_LEVEL", "ERROR")
          .withEnv("ZEEBE_CLOCK_CONTROLLED", "true")
          .withEnv("ZEEBE_BROKER_DATA_DISKUSAGEREPLICATIONWATERMARK", "0.99")
          .withEnv("ZEEBE_BROKER_DATA_DISKUSAGECOMMANDWATERMARK", "0.98")
          .withEnv("ZEEBE_BROKER_DATA_SNAPSHOTPERIOD", "1m");

      if (testContext.getPartitionCount() != null) {
        broker.withEnv(
            "ZEEBE_BROKER_CLUSTER_PARTITIONSCOUNT",
            String.valueOf(testContext.getPartitionCount()));
      }
      if (testContext.isMultitenancyEnabled() != null) {
        broker.withEnv(
            "ZEEBE_BROKER_GATEWAY_MULTITENANCY_ENABLED",
            String.valueOf(testContext.isMultitenancyEnabled()));
        if (testContext.isMultitenancyEnabled()) {
          broker
              .withEnv("ZEEBE_BROKER_GATEWAY_SECURITY_AUTHENTICATION_MODE", "identity")
              .withEnv("ZEEBE_BROKER_GATEWAY_SECURITY_AUTHENTICATION_IDENTITY_TYPE", "keycloak")
              .withEnv(
                  "ZEEBE_BROKER_GATEWAY_SECURITY_AUTHENTICATION_IDENTITY_ISSUERBACKENDURL",
                  testContext.getInternalKeycloakBaseUrl() + "/auth/realms/camunda-platform")
              .withEnv(
                  "ZEEBE_BROKER_GATEWAY_SECURITY_AUTHENTICATION_IDENTITY_AUDIENCE", "zeebe-api")
              .withEnv(
                  "ZEEBE_BROKER_GATEWAY_SECURITY_AUTHENTICATION_IDENTITY_BASEURL",
                  testContext.getInternalIdentityBaseUrl());
          OAuthCredentialsProviderBuilder credentialsProviderBuilder =
              new OAuthCredentialsProviderBuilder()
                  .authorizationServerUrl(
                      testContext.getExternalKeycloakBaseUrl()
                          + "/auth/realms/camunda-platform/protocol/openid-connect/token")
                  .clientId(ZEEBE)
                  .clientSecret(KEYCLOAK_ZEEBE_SECRET)
                  .audience("zeebe-api");
          ZeebeClientBuilder zeebeClientBuilder =
              new ZeebeClientBuilderImpl()
                  .usePlaintext()
                  .credentialsProvider(credentialsProviderBuilder.build());
          testContext.setZeebeClientBuilder(zeebeClientBuilder);
          broker.withTopologyCheck(
              new ZeebeTopologyWaitStrategy().forBuilder(() -> zeebeClientBuilder));
        }
      }
      broker.start();

      LOGGER.info(
          "\n====\nBroker startup time: {}\n====\n", (System.currentTimeMillis() - startTime));

      testContext.setInternalZeebeContactPoint(
          broker.getInternalAddress(ZeebePort.GATEWAY_GRPC.getPort()));
      testContext.setExternalZeebeContactPoint(
          broker.getExternalAddress(ZeebePort.GATEWAY_GRPC.getPort()));
    } else {
      throw new IllegalStateException("Broker is already started. Call stopZeebe first.");
    }
    return broker;
  }

  private void configureRedisExporter(final RedisTestContext testContext) {
    broker.withEnv(
        "ZEEBE_REDIS_REMOTE_ADDRESS",
        "redis://" + testContext.getInternalRedisHost() + ":" + testContext.getInternalRedisPort());
    broker.withEnv(
        "ZEEBE_REDIS_ENABLE_TENANT_STREAMS", String.valueOf(testContext.isTenantStreamsEnabled()));
    if (testContext.getEnabledTenants() != null) {
      broker.withEnv("ZEEBE_REDIS_ENABLED_TENANTS", testContext.getEnabledTenants());
    }
  }

  public void stopZeebeAndRedis(final TestContext testContext) {
    stopZeebe(testContext);
    stopRedis(testContext);
  }

  protected void stopZeebe(final TestContext testContext) {
    stopZeebe(testContext, null);
  }

  public void stopZeebe(final TestContext testContext, final File tmpFolder) {
    // stopZeebe(tmpFolder);
    testContext.setInternalZeebeContactPoint(null);
    testContext.setExternalZeebeContactPoint(null);

    try {
      broker.stop();
    } catch (final Exception ex) {
      LOGGER.error("broker.stop failed", ex);
      // ignore
    }
    broker = null;
  }

  // @SuppressWarnings("checkstyle:NestedIfDepth")
  // public void stopZeebe(final File tmpFolder) {
  // if (broker != null) {
  // try {
  // if (tmpFolder != null && tmpFolder.listFiles().length > 0) {
  // boolean found = false;
  // int attempts = 0;
  // while (!found && attempts < 10) {
  // // check for snapshot existence
  // final List<Path> files = Files.walk(Paths.get(tmpFolder.toURI()))
  // .filter(p -> p.getFileName().endsWith("snapshots"))
  // .collect(Collectors.toList());
  // if (files.size() == 1 && Files.isDirectory(files.get(0))) {
  // if (Files.walk(files.get(0)).count() > 1) {
  // found = true;
  // LOGGER.debug(
  // "Zeebe snapshot was found in "
  // + Files.walk(files.get(0)).findFirst().toString());
  // }
  // }
  // if (!found) {
  // sleepFor(10000L);
  // }
  // attempts++;
  // }
  // if (!found) {
  // throw new AssertionError("Zeebe snapshot was never taken");
  // }
  // }
  // } catch (final IOException e) {
  // throw new RuntimeException(e);
  // } finally {
  // try {
  // broker.shutdownGracefully(Duration.ofSeconds(3));
  // } catch (final Exception ex) {
  // LOGGER.error("broker.shutdownGracefully failed", ex);
  // // ignore
  // }
  // try {
  // broker.stop();
  // } catch (final Exception ex) {
  // LOGGER.error("broker.stop failed", ex);
  // // ignore
  // }
  // broker = null;
  // }
  // }
  // }

  protected void stopRedis(final TestContext testContext) {
    testContext.setExternalRedisHost(null);
    testContext.setExternalRedisPort(null);
    stopRedis();
  }

  public Network getNetwork() {
    if (network == null) {
      network = Network.newNetwork();
    }
    return network;
  }

  // @PreDestroy
  // public void stopRedis() {
  //   stopRed();
  //   closeNetwork();
  // }

  private void stopRedis() {
    if (redisContainer != null) {
      redisContainer.stop();
    }
  }

  private void closeNetwork() {
    if (network != null) {
      network.close();
      network = null;
    }
  }
}
