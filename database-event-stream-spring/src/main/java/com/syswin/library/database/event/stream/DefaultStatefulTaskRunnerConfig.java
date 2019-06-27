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

package com.syswin.library.database.event.stream;

import static com.syswin.library.stateful.task.runner.zookeeper.ZookeeperPaths.ZK_ROOT_PATH;

import com.syswin.library.database.event.stream.mysql.StatefulTaskComposer;
import com.syswin.library.stateful.task.runner.StatefulTask;
import com.syswin.library.stateful.task.runner.zookeeper.ZkBasedStatefulTaskRunner;
import javax.sql.DataSource;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(value = "library.database.stream.multi.enabled", havingValue = "false", matchIfMissing = true)
@Configuration
class DefaultStatefulTaskRunnerConfig {

  @ConditionalOnMissingBean(ZkBasedStatefulTaskRunner.class)
  @Bean(initMethod = "start", destroyMethod = "shutdown")
  ZkBasedStatefulTaskRunner taskRunner(
      @Value("${library.database.stream.cluster.root:" + ZK_ROOT_PATH + "}") String clusterRoot,
      @Value("${library.database.stream.cluster.name}") String clusterName,
      @Value("${library.database.stream.participant.id}") String participantId,
      DataSource dataSource,
      StatefulTask task,
      StatefulTaskComposer statefulTaskComposer,
      CuratorFramework curator) {
    return new ZkBasedStatefulTaskRunner(clusterRoot,
        clusterName,
        participantId,
        statefulTaskComposer.apply(dataSource, task),
        curator);
  }
}
