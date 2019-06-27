/*
 * MIT License
 *
 * Copyright (c) 2019 Syswin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.syswin.library.database.event.stream.mysql;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.shyiko.mysql.binlog.GtidSet;
import org.junit.Test;

public class GtidSetUtilsTest {

  private static final String localGtidSet = "036d85a9-64e5-11e6-9b48-42010af0000c:1-1000,"
      + "27cb766f-9986-11e8-a968-5254007d8718:5-500,"
      + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:5-41";

  private static final String purgedGtidSet = "7145bf69-d1ca-11e5-a588-0242ac110004:1-1234,"
      + "27cb766f-9986-11e8-a968-5254007d8718:1-1000,"
      + "036d85a9-64e5-11e6-9b48-42010af0000c:1-500";


  @Test
  public void mergeGtidSets() {
    GtidSet gtidSet = GtidSetUtils.mergeGtidSets(new GtidSet(localGtidSet), new GtidSet(purgedGtidSet));

    assertThat(asList(gtidSet.toString().split(","))).containsExactlyInAnyOrder(
        "036d85a9-64e5-11e6-9b48-42010af0000c:1-1000",
        "7145bf69-d1ca-11e5-a588-0242ac110004:1-1234",
        "27cb766f-9986-11e8-a968-5254007d8718:1-1000",
        "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:5-41");
  }
}
