package com.syswin.library.database.event.stream;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "library.database.stream.multi")
public class MultiDataSourceConfig {

  private final List<DbStreamContext> contexts = new ArrayList<>();

  public static class DbStreamContext {
    private final DbStreamDataSource dataSource = new DbStreamDataSource();
    private final ClusterConfig cluster = new ClusterConfig();

    public DbStreamDataSource getDataSource() {
      return dataSource;
    }

    public ClusterConfig getCluster() {
      return cluster;
    }
  }

  public static class DbStreamDataSource {
    String url;
    String username;
    String password;

    public String getUrl() {
      return url;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public void setPassword(String password) {
      this.password = password;
    }
  }


  public static class ClusterConfig {
    String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  public List<DbStreamContext> getContexts() {
    return contexts;
  }
}
