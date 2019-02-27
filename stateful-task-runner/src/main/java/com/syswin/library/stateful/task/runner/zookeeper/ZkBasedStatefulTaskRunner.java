package com.syswin.library.stateful.task.runner.zookeeper;

import static com.syswin.library.stateful.task.runner.zookeeper.ZookeeperPaths.ZK_ROOT_PATH;
import static org.apache.curator.framework.recipes.leader.LeaderLatch.State.CLOSED;

import com.syswin.library.stateful.task.runner.StatefulTask;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkBasedStatefulTaskRunner {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String LEADER_LATCH_PATH_TEMPLATE = ZK_ROOT_PATH + "/%s/leader";
  private final String leaderLatchPath;
  private final CuratorFramework curator;
  private final Consumer<Throwable> errorHandler = errorHandler();
  private final Map<String, LeaderLatch> leaderLatches = new ConcurrentHashMap<>();
  private volatile LeaderLatch leaderLatch;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  private final String participantId;
  private final StatefulTask task;

  public ZkBasedStatefulTaskRunner(String clusterName, String participantId, StatefulTask task, CuratorFramework curator) {
    this.participantId = participantId;
    this.task = task;

    this.curator = curator;
    leaderLatchPath = String.format(LEADER_LATCH_PATH_TEMPLATE, clusterName);
    leaderLatch = createLeaderLatch(curator, participantId);
  }

  public void start() throws Exception {
    curator.create().orSetData().creatingParentsIfNeeded().forPath(leaderLatchPath);

    leaderLatch.start();
    curator.getConnectionStateListenable().addListener((client, newState) -> {
      if (newState.isConnected()) {
        log.info("Participant {} is connected with leader latch {} to zookeeper {}",
            leaderLatch.getId(),
            leaderLatchPath,
            curator.getZookeeperClient().getCurrentConnectionString());
      } else {
        log.error("Participant {} is disconnected with leader latch {} from zookeeper {}",
            leaderLatch.getId(),
            leaderLatchPath,
            curator.getZookeeperClient().getCurrentConnectionString());

        this.task.stop();
      }
    });
    run();
  }

  private void run() {
    executor.execute(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          log.info("Participant {} is waiting for leadership with leader latch {}", leaderLatch.getId(), leaderLatchPath);
          leaderLatch.await();
          log.info("Participant {} is running with leadership with leader latch {}", leaderLatch.getId(), leaderLatchPath);
          task.start(errorHandler);
          log.info("Participant {} completed task with leadership with leader latch {}", leaderLatch.getId(), leaderLatchPath);
        }
      } catch (Exception e) {
        log.warn("Failed to acquire leadership with leader latch {} due to interruption", leaderLatchPath, e);
      }
    });
  }

  public void shutdown() {
    log.info("Shutting down stateful task runner of participant {}", leaderLatch.getId());
    executor.shutdownNow();
    task.stop();
    leaderLatches.forEach((id, leaderLatch) -> releaseLeadership(leaderLatch));
    log.info("Shut down stateful task runner of participant {} successfully", leaderLatch.getId());
  }

  private void releaseLeadership(LeaderLatch leaderLatch) {
    try {
      if (!CLOSED.equals(leaderLatch.getState())) {
        leaderLatch.close();
      }
      leaderLatches.remove(leaderLatch.getId());
      log.info("Participant {} released leadership with leader latch {}", leaderLatch.getId(), leaderLatchPath);
    } catch (IOException e) {
      log.warn("Failed to close leader latch {} of participant {}", leaderLatchPath, leaderLatch.getId(), e);
    }
  }

  private Consumer<Throwable> errorHandler() {
    return ex -> {
      log.error("Unexpected exception when running task on participant {} with leader latch {}", leaderLatch.getId(), leaderLatchPath, ex);
      LeaderLatch latchToClose = replaceLeaderLatch();
      task.stop();
      releaseLeadership(latchToClose);
    };
  }

  private LeaderLatch replaceLeaderLatch() {
    LeaderLatch latchToClose = leaderLatch;
    leaderLatch = createLeaderLatch(curator, participantId);
    try {
      leaderLatch.start();
    } catch (Exception e) {
      // this shall not happen
      log.error("Failed to start leader latch {} of participant {}", leaderLatchPath, leaderLatch.getId());
    }
    return latchToClose;
  }

  private LeaderLatch createLeaderLatch(CuratorFramework curator, String participantId) {
    LeaderLatch leaderLatch = new LeaderLatch(curator, leaderLatchPath, participantId + "-" + System.currentTimeMillis());
    leaderLatches.put(leaderLatch.getId(), leaderLatch);
    return leaderLatch;
  }

  boolean isLeader() {
    return leaderLatch.hasLeadership();
  }

  int participantCount() throws Exception {
    return leaderLatch.getParticipants().size();
  }
}
