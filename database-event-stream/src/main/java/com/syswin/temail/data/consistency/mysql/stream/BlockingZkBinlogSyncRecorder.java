package com.syswin.temail.data.consistency.mysql.stream;

import org.apache.curator.framework.CuratorFramework;

public class BlockingZkBinlogSyncRecorder extends ZkBinlogSyncRecorder {

  public BlockingZkBinlogSyncRecorder(String clusterName, CuratorFramework curator) {
    super(clusterName, curator);
  }

  @Override
  public void record(String position) {
    updatePositionToZk(position);
  }

  @Override
  public void flush() {
  }
}
