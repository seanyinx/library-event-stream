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

import static com.seanyinx.github.unit.scaffolding.Randomness.uniquify;
import static org.assertj.core.api.Assertions.assertThat;

import com.syswin.library.database.event.stream.DbEventStreamConnectionException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ZkBinlogSyncRecorderTestBase {
  CuratorFramework curator;
  private static TestingServer zookeeper;

  final String clusterName = uniquify("cluster");
  final String filename = uniquify("filename");

  ZkBinlogSyncRecorder recorder;

  @BeforeClass
  public static void beforeClass() throws Exception {
    zookeeper = new TestingServer(2181, true);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    zookeeper.close();
  }

  @Before
  public void setUp() throws Exception {
    curator = CuratorFrameworkFactory.newClient(
        zookeeper.getConnectString(),
        new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
    curator.start();
    curator.blockUntilConnected();
  }

  @After
  public void tearDown() {
    recorder.shutdown();
    curator.close();
  }

  @Test (expected = DbEventStreamConnectionException.class)
  public void blowsUpWhenGettingGTID_IfNotConnectedToZookeeper() {
    curator.close();
    recorder.position();
  }

  @Test
  public void emptyGTID_WhenNoDataOnZookeeper() {
    assertThat(recorder.position()).isEmpty();
  }
}
