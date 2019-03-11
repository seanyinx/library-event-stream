package com.syswin.library.database.event.stream.mysql;

import com.github.shyiko.mysql.binlog.GtidSet;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GtidSetUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static GtidSet mergeGtidSets(GtidSet localGtidSet, GtidSet purgedGtidSet) {
    GtidSet mergedGtidSet = new GtidSet("");

    log.info("Local GTID set is [{}], and purged GTID set on MySQL server is [{}]", localGtidSet, purgedGtidSet);
    purgedGtidSet.getUUIDSets().forEach(mergedGtidSet::putUUIDSet);
    localGtidSet.getUUIDSets()
        .stream()
        .filter(uuidSet -> !uuidSet.isContainedWithin(purgedGtidSet.getUUIDSet(uuidSet.getUUID())))
        .forEach(mergedGtidSet::putUUIDSet);
    log.info("Merged GTID set is [{}]", mergedGtidSet);
    return mergedGtidSet;
  }
}
