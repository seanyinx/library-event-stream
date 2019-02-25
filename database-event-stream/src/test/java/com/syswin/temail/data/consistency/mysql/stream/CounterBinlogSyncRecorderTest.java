package com.syswin.temail.data.consistency.mysql.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.seanyinx.github.unit.scaffolding.Randomness;
import org.junit.Test;
import org.mockito.Mockito;

public class CounterBinlogSyncRecorderTest {

  private final BinlogSyncRecorder recorder = Mockito.mock(BinlogSyncRecorder.class);
  private final CounterBinlogSyncRecorder counter = new CounterBinlogSyncRecorder(recorder);
  private final String position = Randomness.uniquify("position");

  @Test
  public void recordWithUnderlying() {
    counter.record(position);

    verify(recorder).record(position);
  }

  @Test
  public void flushWithUnderlying() {
    counter.flush();

    verify(recorder).flush();
  }

  @Test
  public void startWithUnderlying() {
    counter.start();

    verify(recorder).start();
  }

  @Test
  public void shutdownWithUnderlying() {
    counter.shutdown();

    verify(recorder).shutdown();
  }

  @Test
  public void getPositionWithUnderlying() {
    when(recorder.position()).thenReturn(position);

    String position = counter.position();

    assertThat(position).isEqualTo(this.position);
  }

  @Test
  public void getPathWithUnderlying() {
    when(recorder.recordPath()).thenReturn(position);

    String position = counter.recordPath();

    assertThat(position).isEqualTo(this.position);
  }
}
