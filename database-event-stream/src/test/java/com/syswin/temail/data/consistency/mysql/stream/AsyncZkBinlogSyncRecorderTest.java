package com.syswin.temail.data.consistency.mysql.stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

public class AsyncZkBinlogSyncRecorderTest extends ZkBinlogSyncRecorderTestBase {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    recorder = new AsyncZkBinlogSyncRecorder(clusterName, curator, 300L);
  }

  @Test
  public void recordBinlogPositionToZk() {
    recorder.start();
    recorder.record(filename);

    await().atMost(1, SECONDS).ignoreExceptions().untilAsserted(() -> {
      byte[] bytes = curator.getData().forPath(recorder.recordPath());
      assertThat(new String(bytes)).isEqualTo(filename);
    });

    assertThat(recorder.position()).isEqualTo(filename);
  }

  @Test
  public void forceUpdateBinlogPositionToZk() throws Exception {
    recorder.start();
    recorder.record(filename);
    recorder.flush();

    byte[] bytes = curator.getData().forPath(recorder.recordPath());
    assertThat(new String(bytes)).isEqualTo(filename);

    assertThat(recorder.position()).isEqualTo(filename);
  }

  @Test
  public void skipUpdateIfNoChange() throws InterruptedException {
    AtomicInteger counter = new AtomicInteger();
    AsyncZkBinlogSyncRecorder recorder = new AsyncZkBinlogSyncRecorder(clusterName, curator, 100L) {
      @Override
      public void record(String position) {
        counter.getAndIncrement();
        super.updatePositionToZk(position);
      }
    };

    recorder.start();
    recorder.record(filename);

    await().atMost(1, SECONDS).ignoreExceptions().untilAsserted(() -> {
      byte[] bytes = curator.getData().forPath(recorder.recordPath());
      assertThat(new String(bytes)).isEqualTo(filename);
    });

    Thread.sleep(200);
    assertThat(recorder.position()).isEqualTo(filename);
    assertThat(counter).hasValue(1);
  }
}
