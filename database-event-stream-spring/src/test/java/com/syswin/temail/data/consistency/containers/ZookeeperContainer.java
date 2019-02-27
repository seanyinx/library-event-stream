package com.syswin.temail.data.consistency.containers;

import org.testcontainers.containers.GenericContainer;

public class ZookeeperContainer extends GenericContainer<ZookeeperContainer> {
  public ZookeeperContainer() {
    super("zookeeper:3.4.13");
  }

  @Override
  protected void configure() {
    super.configure();

    withExposedPorts(2181);
  }
}
