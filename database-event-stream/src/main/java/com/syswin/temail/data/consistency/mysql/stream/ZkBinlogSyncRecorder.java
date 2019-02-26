package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.mysql.stream.ZookeeperPaths.ZK_ROOT_PATH;

import java.lang.invoke.MethodHandles;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkBinlogSyncRecorder implements BinlogSyncRecorder {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String BINLOG_POSITION_PATH_TEMPLATE = ZK_ROOT_PATH + "/%s/position";
  private final String recordPath;
  private final CuratorFramework curator;

  ZkBinlogSyncRecorder(String clusterName, CuratorFramework curator) {
    this.curator = curator;
    this.recordPath = String.format(BINLOG_POSITION_PATH_TEMPLATE, clusterName);
  }

  void updatePositionToZk(String position) {
    try {
      log.debug("Updating binlog position [{}] to {} on zookeeper", position, recordPath);
      curator.create().orSetData()
          .creatingParentsIfNeeded()
          .forPath(recordPath, position.getBytes());
      log.debug("Updated binlog position [{}] to {} on zookeeper", position, recordPath);
    } catch (Exception e) {
      log.error("Failed to record binlog position {} to {} on zookeeper {}",
          position,
          recordPath,
          curator.getZookeeperClient().getCurrentConnectionString(),
          e);
    }
  }

  @Override
  public String position() {
    try {
      if (curator.checkExists().forPath(recordPath) == null) {
        return "";
      }

      return binlogPositionString();
    } catch (Exception e) {
      log.error("Failed to retrieve binlog position on zookeeper with path {}", recordPath, e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String recordPath() {
    return recordPath;
  }

  private String binlogPositionString() throws Exception {
    return new String(curator.getData().forPath(recordPath));
  }
}
