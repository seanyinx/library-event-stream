package com.syswin.library.database.event.stream.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.shyiko.mysql.binlog.GtidSet;
import org.junit.Test;

public class GtidSetUtilsTest {

  private static final String localGtidSet = "036d85a9-64e5-11e6-9b48-42010af0000c:1-1000,"
      + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:5-41";

  private static final String remoteGtidSet = "036d85a9-64e5-11e6-9b48-42010af0000c:1-2000,"
      + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200,"
      + "123e4567-e89b-12d3-a456-426655440000:1-41";

  private static final String purgedGtidSet = "7145bf69-d1ca-11e5-a588-0242ac110004:1-1234,"
      + "036d85a9-64e5-11e6-9b48-42010af0000c:1-500";


  @Test
  public void mergeGtidSets() {
    GtidSet gtidSet = GtidSetUtils.mergeGtidSets(new GtidSet(localGtidSet), new GtidSet(remoteGtidSet), new GtidSet(purgedGtidSet));

    assertThat(gtidSet.toString()).isEqualTo(
        "036d85a9-64e5-11e6-9b48-42010af0000c:1-1000,"
        + "7145bf69-d1ca-11e5-a588-0242ac110004:1-1234,"
        + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:5-41");
  }
}
