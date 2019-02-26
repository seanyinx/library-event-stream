package com.syswin.temail.data.consistency.mysql.stream;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class BlockingZkBinlogSyncRecorderTest extends ZkBinlogSyncRecorderTestBase {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    recorder = new BlockingZkBinlogSyncRecorder(clusterName, curator);
  }

  @Test
  public void recordBinlogPositionToZk() throws Exception {
    recorder.start();
    recorder.record(filename);

    assertThat(recorder.position()).isEqualTo(filename);

    byte[] bytes = curator.getData().forPath(recorder.recordPath());
    assertThat(new String(bytes)).isEqualTo(filename);
  }
}
