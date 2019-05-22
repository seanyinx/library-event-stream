package com.syswin.library.database.event.stream.mysql;

import com.syswin.library.database.event.stream.StatefulTaskSupplier;
import com.syswin.library.stateful.task.runner.CompositeStatefulTask;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class StatefulTaskComposerConfig {

  @ConditionalOnBean(StatefulTaskSupplier.class)
  @Bean
  StatefulTaskComposer compositeStatefulTaskSupplier(StatefulTaskSupplier statefulTaskSupplier) {
    return (dataSource, statefulTask) -> new CompositeStatefulTask(
        statefulTaskSupplier.apply(dataSource),
        statefulTask
    );
  }

  @ConditionalOnMissingBean(StatefulTaskComposer.class)
  @Bean
  StatefulTaskComposer passThroughStatefulTaskSupplier() {
    return (dataSource, statefulTask) -> statefulTask;
  }
}
