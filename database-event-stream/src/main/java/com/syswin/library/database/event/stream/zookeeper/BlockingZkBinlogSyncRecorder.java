package com.syswin.library.database.event.stream.zookeeper;

import org.apache.curator.framework.CuratorFramework;

public class BlockingZkBinlogSyncRecorder extends ZkBinlogSyncRecorder {

  public BlockingZkBinlogSyncRecorder(String clusterName, CuratorFramework curator) {
    super(clusterName, curator);
  }

  public BlockingZkBinlogSyncRecorder(String rootPath, String clusterName, CuratorFramework curator) {
    super(rootPath, clusterName, curator);
  }

  @Override
  void flushIfUpdated() {
  }

  @Override
  public void record(String position) {
    updatePositionToZk(position);
  }
}
