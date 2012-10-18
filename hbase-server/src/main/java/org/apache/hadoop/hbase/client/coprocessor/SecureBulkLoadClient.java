/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client.coprocessor;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint;
import org.apache.hadoop.hbase.security.access.SecureBulkLoadProtocol;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.List;

/**
 * Client proxy for SecureBulkLoadProtocol
 * used in conjunction with SecureBulkLoadEndpoint
 */
public class SecureBulkLoadClient {
  private SecureBulkLoadProtocol proxy;
  private HTable table;

  public SecureBulkLoadClient(HTable table) throws IOException {
    this(table, HConstants.EMPTY_START_ROW);
  }

  public SecureBulkLoadClient(HTable table, byte[] startRow) throws IOException {
    proxy = table.coprocessorProxy(SecureBulkLoadProtocol.class, startRow);
    this.table = table;
  }

  public String prepareBulkLoad(byte[] tableName) throws IOException {
    try {
      return proxy.prepareBulkLoad(tableName);
    } catch (Exception e) {
      throw new IOException("Failed to prepareBulkLoad", e);
    }
  }

  public void cleanupBulkLoad(String bulkToken) throws IOException {
    proxy.cleanupBulkLoad(bulkToken);
  }

  public boolean bulkLoadHFiles(List<Pair<byte[], String>> familyPaths,
                         Token<?> userToken, String bulkToken) throws IOException {
    return proxy.bulkLoadHFiles(familyPaths, userToken, bulkToken);
  }

  public Path getStagingPath(String bulkToken, byte[] family) throws IOException {
    return SecureBulkLoadEndpoint.getStagingPath(table.getConfiguration(), bulkToken, family);
  }
}
