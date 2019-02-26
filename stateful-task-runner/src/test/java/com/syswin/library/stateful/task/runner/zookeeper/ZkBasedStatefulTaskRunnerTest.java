package com.syswin.library.stateful.task.runner.zookeeper;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.seanyinx.github.unit.scaffolding.Randomness;
import com.syswin.library.stateful.task.runner.StatefulTask;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZkBasedStatefulTaskRunnerTest {

  private static TestingServer zookeeper;
  private static CuratorFramework curator;

  private int counter = 0;
  private final String clusterName = Randomness.uniquify("clusterName");
  private final Queue<Integer> values = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean isBroken = new AtomicBoolean(false);
  private final StatefulTask task = new StatefulTask() {

    private final AtomicBoolean isStopped = new AtomicBoolean(false);

    @Override
    public void start(Consumer<Throwable> errorHandler) {
      isStopped.set(false);
      isBroken.set(false);
      while (!isStopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          values.add(counter++);
          Thread.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        if (isBroken.get()) {
          errorHandler.accept(new IllegalStateException("oops"));
        }
      }
    }

    @Override
    public void stop() {
      isStopped.set(true);
    }
  };

  private ZkBasedStatefulTaskRunner taskRunner1;
  private ZkBasedStatefulTaskRunner taskRunner2;

  @BeforeClass
  public static void beforeClass() throws Exception {
    zookeeper = new TestingServer(2181, true);

    curator = CuratorFrameworkFactory.newClient(zookeeper.getConnectString(), new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
    curator.start();
    curator.blockUntilConnected();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    curator.close();
    zookeeper.close();
  }

  @Before
  public void setUp() throws Exception {
    taskRunner1 = new ZkBasedStatefulTaskRunner(clusterName, UUID.randomUUID().toString(), task, curator);
    taskRunner2 = new ZkBasedStatefulTaskRunner(clusterName, UUID.randomUUID().toString(), task, curator);

    taskRunner1.start();
    taskRunner2.start();
  }

  @After
  public void tearDown() {
    taskRunner1.shutdown();
    taskRunner2.shutdown();
  }

  @Test
  public void runTaskOnLeaderOnly() throws Exception {
    Thread.sleep(500);

    ZkBasedStatefulTaskRunner leader = taskRunner1.isLeader() ? taskRunner1 : taskRunner2;
    leader.shutdown();
    int countProducedByLastLeader = values.size();
    Thread.sleep(500);

    // task continues on leader alive
    assertThat(values.size()).isGreaterThan(countProducedByLastLeader);

    // task executed on leader only
    Integer previous = values.poll();
    Integer current;
    while (!values.isEmpty()) {
      current = values.poll();
      assertThat(current - previous).isOne();
      previous = current;
    }

    assertThat(taskRunner1.participantCount()).isLessThanOrEqualTo(1);
  }

  @Test
  public void resumeTaskOnLeaderOnly() throws Exception {
    Thread.sleep(200);

    zookeeper.stop();
    int countProducedByLastLeader = values.size();
    Thread.sleep(200);
    zookeeper.restart();
    await().atMost(1, SECONDS).untilAsserted(() -> assertThat(curator.getZookeeperClient().isConnected()).isTrue());
    Thread.sleep(200);

    // task resumes on reconnect
    assertThat(values.size()).isGreaterThan(countProducedByLastLeader);

    // task executed on leader only
    Integer previous = values.poll();
    Integer current;
    while (!values.isEmpty()) {
      current = values.poll();
      assertThat(current - previous).isOne();
      previous = current;
    }

    assertThat(taskRunner1.participantCount()).isLessThanOrEqualTo(2);
  }

  @Test
  public void resumeBrokenTaskWhenReconnected() throws Exception {
    Thread.sleep(200);

    isBroken.set(true);
    zookeeper.stop();
    int countProducedByLastLeader = values.size();
    Thread.sleep(200);
    zookeeper.restart();
    await().atMost(1, SECONDS).untilAsserted(() -> assertThat(curator.getZookeeperClient().isConnected()).isTrue());
    Thread.sleep(200);

    // task resumes on reconnect
    assertThat(values.size()).isGreaterThan(countProducedByLastLeader);

    // task executed on leader only
    Integer previous = values.poll();
    Integer current;
    while (!values.isEmpty()) {
      current = values.poll();
      assertThat(current - previous).isOne();
      previous = current;
    }

    assertThat(taskRunner1.participantCount()).isLessThanOrEqualTo(2);
  }

  @Test
  public void releaseLeadershipOnException() throws Exception {
    Thread.sleep(200);

    // 1st leader broke with error
    isBroken.set(true);
    int countProducedByLastLeader = values.size();
    Thread.sleep(200);

    // task continues on leader alive
    assertThat(values.size()).isGreaterThan(countProducedByLastLeader);

    // 2nd leader broke with error
    isBroken.set(true);
    countProducedByLastLeader = values.size();
    Thread.sleep(200);

    // task continues on leader alive
    assertThat(values.size()).isGreaterThan(countProducedByLastLeader);

    // task executed on leader only
    Integer previous = values.poll();
    Integer current;
    while (!values.isEmpty()) {
      current = values.poll();
      assertThat(current - previous).isOne();
      previous = current;
    }

    assertThat(taskRunner1.participantCount()).isLessThanOrEqualTo(2);
  }
}
