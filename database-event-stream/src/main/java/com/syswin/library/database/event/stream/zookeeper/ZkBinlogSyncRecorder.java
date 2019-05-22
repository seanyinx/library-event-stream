package com.syswin.library.database.event.stream.zookeeper;

import static com.syswin.library.stateful.task.runner.zookeeper.ZookeeperPaths.ZK_ROOT_PATH;

import com.syswin.library.database.event.stream.BinlogSyncRecorder;
import com.syswin.library.database.event.stream.DbEventStreamConnectionException;
import com.syswin.library.database.event.stream.DbEventStreamEndOfLifeException;
import java.lang.invoke.MethodHandles;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkBinlogSyncRecorder implements BinlogSyncRecorder {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String recordPath;
  private final CuratorFramework curator;

  ZkBinlogSyncRecorder(String clusterName, CuratorFramework curator) {
    this(ZK_ROOT_PATH, clusterName, curator);
  }

  ZkBinlogSyncRecorder(String rootPath, String clusterName, CuratorFramework curator) {
    this.curator = curator;
    this.recordPath = String.format("/%s/%s/position", rootPath, clusterName);
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
    } catch (InterruptedException e) {
      throw new DbEventStreamEndOfLifeException(e);
    } catch (Exception e) {
      throw new DbEventStreamConnectionException("Failed to retrieve binlog position on zookeeper with path " + recordPath, e);
    }
  }

  @Override
  public String recordPath() {
    return recordPath;
  }

  @Override
  public void flush() {
    if (curator.getZookeeperClient().isConnected()) {
      flushIfUpdated();
      try {
        log.info("Flushed binlog position [{}] to {} on zookeeper", binlogPositionString(), recordPath);
      } catch (Exception e) {
        log.error("Failed to retrieve binlog position to zookeeper with path {}", recordPath, e);
      }
    }
  }

  abstract void flushIfUpdated();

  private String binlogPositionString() throws Exception {
    return new String(curator.getData().forPath(recordPath));
  }
}
