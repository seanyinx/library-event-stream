package com.syswin.library.database.event.stream.mysql;

import com.github.shyiko.mysql.binlog.GtidSet;

class GtidSetUtils {

  static GtidSet mergeGtidSets(GtidSet localGtidSet, GtidSet remoteGtidSet, GtidSet purgedGtidSet) {
    GtidSet mergedGtidSet = new GtidSet("");

    remoteGtidSet.getUUIDSets()
        .stream()
        .filter(uuidSet -> localGtidSet.getUUIDSet(uuidSet.getUUID()) != null)
        .forEach(mergedGtidSet::putUUIDSet);

    purgedGtidSet.getUUIDSets().forEach(mergedGtidSet::putUUIDSet);
    localGtidSet.getUUIDSets().forEach(mergedGtidSet::putUUIDSet);
    return mergedGtidSet;
  }
}
