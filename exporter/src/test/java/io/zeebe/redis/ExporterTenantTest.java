package io.zeebe.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.zeebe.exporter.proto.Schema;
import io.zeebe.redis.exporter.ProtobufCodec;
import io.zeebe.redis.testcontainers.*;
import io.zeebe.redis.testcontainers.camunda.RedisTestContext;
import io.zeebe.redis.testcontainers.camunda.TestContainerUtil;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(OnFailureExtension.class)
@Execution(ExecutionMode.SAME_THREAD)
public class ExporterTenantTest {

  public static final String TENANT_1 = "tenant_1";
  public static final String TENANT_2 = "tenant_2";

  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process")
          .startEvent("start")
          .sequenceFlowId("to-task")
          .serviceTask("task", s -> s.zeebeJobType("test"))
          .sequenceFlowId("to-end")
          .endEvent("end")
          .done();

  private static final BpmnModelInstance USER_TASK_WORKFLOW =
      Bpmn.createExecutableProcess("user_task_process")
          .startEvent("start")
          .sequenceFlowId("to-task")
          .userTask("userTask", u -> u.zeebeUserTask().zeebeCandidateGroups("testGroup"))
          .sequenceFlowId("to-end")
          .endEvent("end")
          .done();

  private static final TestContainerUtil testContainerUtil = new TestContainerUtil();
  private static RedisTestContext testContext;

  private StatefulRedisConnection<String, byte[]> redisConnection;

  @BeforeAll
  public static void setup() throws IOException {
    testContext = new RedisTestContext();
    testContext.setMultitenancyEnabled(true);
    testContext.setTenantStreamsEnabled(true);

    testContainerUtil.startIdentity(testContext, "8.7.4", testContext.isMultitenancyEnabled());
  }

  @BeforeEach
  public void startUp() throws IOException {
    testContainerUtil.startRedis(testContext);
    testContext.setRedisClient(
        RedisClient.create(
            "redis://"
                + testContext.getExternalRedisHost()
                + ":"
                + testContext.getExternalRedisPort()));
  }

  private void startUpZeebeWithRedisExporter() {
    // final String zeebeVersion =
    // ContainerVersionsUtil.readProperty(ZEEBE_CURRENTVERSION_DOCKER_PROPERTY_NAME);
    testContainerUtil.startZeebe("8.7.8-2.0.1-SNAPSHOT", testContext);
    final ZeebeClient client = testContext.getZeebeClientBuilder().build();
    testContext.setZeebeClient(client);

    // Initialize the Redis connection for each test
    redisConnection = testContext.getRedisClient().connect(new ProtobufCodec());
    redisConnection.sync().xtrim("zeebe:DEPLOYMENT:<default>", 0);
  }

  @AfterEach
  public void cleanUp() throws IOException {
    if (redisConnection != null) {
      redisConnection.close();
    }
    testContainerUtil.stopZeebeAndRedis(testContext);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    // testContainerUtil.stopZeebeAndRedis(testContext);
  }

  @Test
  public void shouldExportUserTaskEventsWithTenant() throws Exception {
    testContext.setTenantStreamsEnabled(true);
    testContext.setEnabledTenants(TENANT_1); // any Tenant
    startUpZeebeWithRedisExporter();
    // given
    testContext
        .getZeebeClient()
        .newDeployResourceCommand()
        .addProcessModel(USER_TASK_WORKFLOW, "user-task.bpmn")
        .tenantId(TENANT_1)
        .send()
        .join();
    testContext
        .getZeebeClient()
        .newCreateInstanceCommand()
        .bpmnProcessId("user_task_process")
        .latestVersion()
        .tenantId(TENANT_1)
        .send()
        .join();
    Thread.sleep(1000);

    // when
    final var message =
        redisConnection.sync().xrange("zeebe:USER_TASK:" + TENANT_1, Range.create("-", "+")).get(0);

    // then
    assertThat(message).isNotNull();
    final var messageValue = message.getBody().values().iterator().next();

    final var record = Schema.Record.parseFrom(messageValue);
    assertThat(record.getRecord().is(Schema.UserTaskRecord.class)).isTrue();

    final var userTaskRecord = record.getRecord().unpack(Schema.UserTaskRecord.class);
    assertThat(userTaskRecord.getElementId()).isEqualTo("userTask");
    assertThat(userTaskRecord.getCandidateGroupsCount()).isEqualTo(1);
    assertThat(userTaskRecord.getCandidateGroups(0)).isEqualTo("testGroup");
  }

  @Test
  public void shouldExportUserTaskEventsOnlyTenant1() throws Exception {
    testContext.setEnabledTenants(TENANT_1); // Only enable TENANT_1 for this test
    startUpZeebeWithRedisExporter();
    // given
    testContext
        .getZeebeClient()
        .newDeployResourceCommand()
        .addProcessModel(USER_TASK_WORKFLOW, "user-task.bpmn")
        .tenantId(TENANT_1)
        .send()
        .join();
    testContext
        .getZeebeClient()
        .newCreateInstanceCommand()
        .bpmnProcessId("user_task_process")
        .latestVersion()
        .tenantId(TENANT_1)
        .send()
        .join();

    testContext
        .getZeebeClient()
        .newDeployResourceCommand()
        .addProcessModel(USER_TASK_WORKFLOW, "user-task.bpmn")
        .tenantId(TENANT_2)
        .send()
        .join();
    testContext
        .getZeebeClient()
        .newCreateInstanceCommand()
        .bpmnProcessId("user_task_process")
        .latestVersion()
        .tenantId(TENANT_2)
        .send()
        .join();
    Thread.sleep(1000);

    // when
    final var message =
        redisConnection.sync().xrange("zeebe:USER_TASK:" + TENANT_1, Range.create("-", "+")).get(0);

    // then
    assertThat(message).isNotNull();
    final var messageValue = message.getBody().values().iterator().next();

    final var record = Schema.Record.parseFrom(messageValue);
    assertThat(record.getRecord().is(Schema.UserTaskRecord.class)).isTrue();

    final var userTaskRecord = record.getRecord().unpack(Schema.UserTaskRecord.class);
    assertThat(userTaskRecord.getElementId()).isEqualTo("userTask");
    assertThat(userTaskRecord.getCandidateGroupsCount()).isEqualTo(1);
    assertThat(userTaskRecord.getCandidateGroups(0)).isEqualTo("testGroup");

    // Verify that TENANT_2 stream does not exist or is empty (since only enabled
    // tenants are
    // configured)
    final var tenant2Messages =
        redisConnection.sync().xrange("zeebe:USER_TASK:" + TENANT_2, Range.create("-", "+"));
    assertThat(tenant2Messages).isEmpty();
  }

  @Test
  public void shouldExportUserTaskEventsForBothTenants() throws Exception {
    // Temporarily enable both tenants for this test
    testContext.setEnabledTenants(TENANT_1 + "," + TENANT_2);
    startUpZeebeWithRedisExporter();

    // given - deploy and start process for TENANT_1
    testContext
        .getZeebeClient()
        .newDeployResourceCommand()
        .addProcessModel(USER_TASK_WORKFLOW, "user-task-tenant1.bpmn")
        .tenantId(TENANT_1)
        .send()
        .join();
    testContext
        .getZeebeClient()
        .newCreateInstanceCommand()
        .bpmnProcessId("user_task_process")
        .latestVersion()
        .tenantId(TENANT_1)
        .send()
        .join();

    // given - deploy and start process for TENANT_2
    testContext
        .getZeebeClient()
        .newDeployResourceCommand()
        .addProcessModel(USER_TASK_WORKFLOW, "user-task-tenant2.bpmn")
        .tenantId(TENANT_2)
        .send()
        .join();
    testContext
        .getZeebeClient()
        .newCreateInstanceCommand()
        .bpmnProcessId("user_task_process")
        .latestVersion()
        .tenantId(TENANT_2)
        .send()
        .join();

    Thread.sleep(1000);

    // when - get messages from both tenant streams
    final var tenant1Messages =
        redisConnection.sync().xrange("zeebe:USER_TASK:" + TENANT_1, Range.create("-", "+"));
    final var tenant2Messages =
        redisConnection.sync().xrange("zeebe:USER_TASK:" + TENANT_2, Range.create("-", "+"));

    // then - verify both tenants have messages
    assertThat(tenant1Messages).isNotEmpty();
    assertThat(tenant2Messages).isNotEmpty();

    // Verify TENANT_1 message
    final var tenant1Message = tenant1Messages.get(0);
    assertThat(tenant1Message).isNotNull();
    final var tenant1MessageValue = tenant1Message.getBody().values().iterator().next();
    final var tenant1Record = Schema.Record.parseFrom(tenant1MessageValue);
    assertThat(tenant1Record.getRecord().is(Schema.UserTaskRecord.class)).isTrue();
    final var tenant1UserTaskRecord = tenant1Record.getRecord().unpack(Schema.UserTaskRecord.class);
    assertThat(tenant1UserTaskRecord.getElementId()).isEqualTo("userTask");

    // Verify TENANT_2 message
    final var tenant2Message = tenant2Messages.get(0);
    assertThat(tenant2Message).isNotNull();
    final var tenant2MessageValue = tenant2Message.getBody().values().iterator().next();
    final var tenant2Record = Schema.Record.parseFrom(tenant2MessageValue);
    assertThat(tenant2Record.getRecord().is(Schema.UserTaskRecord.class)).isTrue();
    final var tenant2UserTaskRecord = tenant2Record.getRecord().unpack(Schema.UserTaskRecord.class);
    assertThat(tenant2UserTaskRecord.getElementId()).isEqualTo("userTask");
  }
}
