package com.syswin.temail.data.consistency.containers;

import static org.testcontainers.containers.BindMode.READ_ONLY;

import org.testcontainers.containers.GenericContainer;

public class MysqlContainer extends GenericContainer<MysqlContainer> {
  public MysqlContainer() {
    super("mysql:5.7");
  }

  @Override
  protected void configure() {
    super.configure();

    withClasspathResourceMapping("config-file.cnf", "/etc/mysql/conf.d/config-file.cnf", READ_ONLY);
    withExposedPorts(3306);
  }
}
