package com.syswin.library.database.event.stream.zookeeper;

import java.lang.invoke.MethodHandles;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class DefaultCuratorConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @ConditionalOnMissingBean
  @Bean(destroyMethod = "close")
  static CuratorFramework curator(@Value("${library.database.stream.zk.address}") String zookeeperAddress) throws InterruptedException {
    CuratorFramework curator = CuratorFrameworkFactory.newClient(
        zookeeperAddress,
        new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));

    curator.start();
    log.info("Connecting to zookeeper at {}", zookeeperAddress);
    curator.blockUntilConnected();
    log.info("Connected to zookeeper at {}", zookeeperAddress);
    return curator;
  }
}
