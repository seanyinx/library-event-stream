package com.syswin.library.database.event.stream.mysql;

import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class DataSourceContainer {

  @ConditionalOnMissingBean
  @Bean
  List<DataSource> dataSources() {
    return new ArrayList<>();
  }
}
