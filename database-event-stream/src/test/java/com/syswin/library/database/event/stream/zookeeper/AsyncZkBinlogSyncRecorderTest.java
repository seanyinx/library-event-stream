/*
 * MIT License
 *
 * Copyright (c) 2019 Syswin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.syswin.library.database.event.stream.zookeeper;

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
