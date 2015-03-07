/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.util.TableId;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Utility methods for dealing with HBase table name conversions in HBase 0.94.
 */
public class HTable94NameConverter extends HTableNameConverter {
  @Override
  public String getSysConfigTablePrefix(String hTableName) {
    return HBASE_NAMESPACE_PREFIX + Constants.SYSTEM_NAMESPACE + ".";
  }

  @Override
  public TableId from(String hTableName) {
    return fromTableName(hTableName);
  }

  public static String toTableName(CConfiguration cConf, TableId tableId) {
    Preconditions.checkArgument(tableId != null, "Table Id should not be null.");
    // backward compatibility
    if (Constants.DEFAULT_NAMESPACE_ID.equals(tableId.getNamespace())) {
      return getHBaseTableName(cConf, tableId);
    }
    return Joiner.on(".").join(toHBaseNamespace(tableId.getNamespace()),
                               getHBaseTableName(cConf, tableId));
  }

  // Assumptions made:
  // 1) root prefix can not have '.' or '_'.
  // 2) namespace can not have '.'
  public static TableId fromTableName(String hTableName) {
    Preconditions.checkArgument(hTableName != null, "HBase table name should not be null.");
    String[] parts = hTableName.split("\\.", 2);
    String hBaseNamespace;
    String hBaseQualifier;

    if (!parts[0].contains("_")) {
      hBaseNamespace = Constants.DEFAULT_NAMESPACE;
      hBaseQualifier = hTableName;
    } else {
      hBaseNamespace = parts[0];
      hBaseQualifier = parts[1];
    }
    return HTableNameConverter.from(hBaseNamespace, hBaseQualifier);
  }
}
