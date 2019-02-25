package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.mysql.stream.ZookeeperPaths.ZK_ROOT_PATH;
import static org.apache.curator.framework.recipes.leader.LeaderLatch.State.CLOSED;

import java.io.EOFException;
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

class ZkBasedStatefulTaskRunner {
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

  ZkBasedStatefulTaskRunner(String clusterName, String participantId, StatefulTask task, CuratorFramework curator) {
    this.participantId = participantId;
    this.task = task;

    this.curator = curator;
    leaderLatchPath = String.format(LEADER_LATCH_PATH_TEMPLATE, clusterName);
    leaderLatch = createLeaderLatch(curator, participantId);
  }

  void start() throws Exception {
    curator.create().orSetData().creatingParentsIfNeeded().forPath(leaderLatchPath);

    leaderLatch.start();
    curator.getConnectionStateListenable().addListener((client, newState) -> {
      if (newState.isConnected()) {
        log.info("Participant {} is connected to zookeeper {}",
            leaderLatch.getId(),
            curator.getZookeeperClient().getCurrentConnectionString());
      } else {
        log.error("Participant {} is disconnected from zookeeper {}",
            leaderLatch.getId(),
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
          log.info("Participant {} is waiting for leadership", leaderLatch.getId());
          leaderLatch.await();
          log.info("Participant {} is running with leadership", leaderLatch.getId());
          task.start(errorHandler);
          log.info("Participant {} completed task with leadership", leaderLatch.getId());
        }
      } catch (InterruptedException | EOFException e) {
        log.warn("Failed to acquire leadership due to interruption", e);
      }
    });
  }

  void shutdown() {
    executor.shutdownNow();
    task.stop();
    leaderLatches.forEach((id, leaderLatch) -> releaseLeadership(leaderLatch));
  }

  private void releaseLeadership(LeaderLatch leaderLatch) {
    try {
      if (!CLOSED.equals(leaderLatch.getState())) {
        leaderLatch.close();
        log.info("Participant {} released leadership", leaderLatch.getId());
      }
      leaderLatches.remove(leaderLatch.getId());
    } catch (IOException e) {
      log.warn("Failed to close leader latch of participant {}", leaderLatch.getId(), e);
    }
  }

  private Consumer<Throwable> errorHandler() {
    return ex -> {
      log.error("Unexpected exception when running task on participant {}", leaderLatch.getId(), ex);
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
      log.error("Failed to start leader latch of participant {}", leaderLatch.getId());
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
