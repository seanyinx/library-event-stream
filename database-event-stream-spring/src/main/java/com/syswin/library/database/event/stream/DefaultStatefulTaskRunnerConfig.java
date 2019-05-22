package com.syswin.library.database.event.stream;

import static com.syswin.library.stateful.task.runner.zookeeper.ZookeeperPaths.ZK_ROOT_PATH;

import com.syswin.library.database.event.stream.mysql.StatefulTaskComposer;
import com.syswin.library.stateful.task.runner.StatefulTask;
import com.syswin.library.stateful.task.runner.zookeeper.ZkBasedStatefulTaskRunner;
import javax.sql.DataSource;
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
