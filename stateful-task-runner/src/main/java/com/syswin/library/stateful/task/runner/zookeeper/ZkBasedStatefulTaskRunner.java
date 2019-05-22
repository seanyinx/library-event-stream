package com.syswin.library.stateful.task.runner.zookeeper;

import static com.syswin.library.stateful.task.runner.zookeeper.ZookeeperPaths.ZK_ROOT_PATH;

import com.syswin.library.stateful.task.runner.StatefulTask;
import java.lang.invoke.MethodHandles;
import java.util.function.Consumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkBasedStatefulTaskRunner {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String leaderLatchPath;
  private final CuratorFramework curator;
  private final Consumer<Throwable> errorHandler = errorHandler();
  private final LeaderSelector leaderSelector;

  private final String participantId;
  private final StatefulTask task;

  public ZkBasedStatefulTaskRunner(String clusterName, String participantId, StatefulTask task, CuratorFramework curator) {
    this(ZK_ROOT_PATH, clusterName, participantId, task, curator);
  }

  public ZkBasedStatefulTaskRunner(String rootPath, String clusterName, String participantId, StatefulTask task, CuratorFramework curator) {
    this.participantId = participantId;
    this.task = task;

    this.curator = curator;
    leaderLatchPath = String.format("/%s/%s/leader", rootPath, clusterName);

    leaderSelector = createLeaderSelector(curator, task);
  }

  public void start() throws Exception {
    curator.create().orSetData().creatingParentsIfNeeded().forPath(leaderLatchPath);

    leaderSelector.start();
  }

  public void shutdown() {
    log.info("Shutting down stateful task runner of participant {}", participantId);
    task.stop();
    try {
      leaderSelector.close();
    } catch (IllegalStateException e) {
      log.warn("Failed to close leader selector of participant {}", participantId, e);
    }
    log.info("Shut down stateful task runner of participant {} successfully", participantId);
  }

  private Consumer<Throwable> errorHandler() {
    return ex -> {
      log.error("Unexpected exception when running task on participant {} with leader latch {}", participantId, leaderLatchPath, ex);
      task.stop();
    };
  }

  boolean isLeader() {
    return leaderSelector.hasLeadership();
  }

  int participantCount() throws Exception {
    return leaderSelector.getParticipants().size();
  }

  private LeaderSelector createLeaderSelector(CuratorFramework curator, StatefulTask task) {
    LeaderSelector leaderSelector = new LeaderSelector(curator, leaderLatchPath, new LeaderSelectorListenerAdapter() {
      @Override
      public void takeLeadership(CuratorFramework client) {
        log.info("Participant {} acquired leadership with leader latch {} to zookeeper {}",
            participantId,
            leaderLatchPath,
            client.getZookeeperClient().getCurrentConnectionString());

        task.start(errorHandler);

        log.info("Participant {} released leadership with leader latch {} to zookeeper {}",
            participantId,
            leaderLatchPath,
            client.getZookeeperClient().getCurrentConnectionString());
      }
    });

    leaderSelector.setId(participantId);
    leaderSelector.autoRequeue();
    return leaderSelector;
  }
}
