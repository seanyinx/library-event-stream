package com.syswin.library.database.event.stream;

import com.syswin.library.stateful.task.runner.StatefulTask;
import com.syswin.library.stateful.task.runner.zookeeper.ZkBasedStatefulTaskRunner;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class DefaultStatefulTaskRunnerConfig {

  @ConditionalOnMissingBean(ZkBasedStatefulTaskRunner.class)
  @Bean(initMethod = "start", destroyMethod = "shutdown")
  ZkBasedStatefulTaskRunner taskRunner(
      @Value("${library.database.stream.cluster.name}") String clusterName,
      @Value("${library.database.stream.participant.id}") String participantId,
      StatefulTask task,
      CuratorFramework curator) {
    return new ZkBasedStatefulTaskRunner(clusterName, participantId, task, curator);
  }
}
