/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.GroupAdmin;
import org.apache.hadoop.hbase.master.GroupInfo;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Performs authorization checks for common operations, according to different
 * levels of authorized users.
 */
@Category(LargeTests.class)
@SuppressWarnings("rawtypes")
public class TestSecureGroupInfoManagerEndpoint {
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  // user with all permissions
  private static User SUPERUSER;
  // user granted with all global permission
  private static User USER_ADMIN;
  // user with rw permissions
  private static User USER_RW;
  // user with read-only permissions
  private static User USER_RO;
  // user is table owner. will have all permissions on table
  private static User USER_OWNER;
  // user with create table permissions alone
  private static User USER_CREATE;
  // user with no permissions
  private static User USER_NONE;

  private static byte[] TEST_TABLE = Bytes.toBytes("testtable");
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static MasterCoprocessorEnvironment CP_ENV;
  private static RegionCoprocessorEnvironment RCP_ENV;
  private static AccessController ACCESS_CONTROLLER;
  private static SecureGroupInfoManagerEndpoint GROUP_ENDPOINT;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);
    conf.set("hbase.coprocessor.region.classes",
        conf.get("hbase.coprocessor.region.classes")+","+SecureGroupInfoManagerEndpoint.class.getName());

    TEST_UTIL.startMiniCluster();
    MasterCoprocessorHost cpHost = TEST_UTIL.getMiniHBaseCluster().getMaster().getCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    ACCESS_CONTROLLER = (AccessController) cpHost.findCoprocessor(AccessController.class.getName());
    GROUP_ENDPOINT = (SecureGroupInfoManagerEndpoint)
        TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getOnlineRegions(HConstants.ROOT_TABLE_NAME).get(0)
           .getCoprocessorHost().findCoprocessor(SecureGroupInfoManagerEndpoint.class.getName());
    // Wait for the ACL table to become available
    TEST_UTIL.waitTableAvailable(AccessControlLists.ACL_TABLE_NAME, 5000);



    // create a set of test users
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER_ADMIN = User.createUserForTesting(conf, "admin2", new String[0]);
    USER_RW = User.createUserForTesting(conf, "rwuser", new String[0]);
    USER_RO = User.createUserForTesting(conf, "rouser", new String[0]);
    USER_OWNER = User.createUserForTesting(conf, "owner", new String[0]);
    USER_CREATE = User.createUserForTesting(conf, "tbl_create", new String[0]);
    USER_NONE = User.createUserForTesting(conf, "nouser", new String[0]);

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
    htd.setOwner(USER_OWNER);
    admin.createTable(htd);

    // initilize access control
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol = meta.coprocessorProxy(AccessControllerProtocol.class,
      TEST_TABLE);

    HRegion region = TEST_UTIL.getHBaseCluster().getRegions(TEST_TABLE).get(0);
    RegionCoprocessorHost rcpHost = region.getCoprocessorHost();
    RCP_ENV = rcpHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER,
      Coprocessor.PRIORITY_HIGHEST, 1, conf);

    protocol.grant(new UserPermission(Bytes.toBytes(USER_ADMIN.getShortName()),
        Permission.Action.ADMIN, Permission.Action.CREATE, Permission.Action.READ,
        Permission.Action.WRITE));

    protocol.grant(new UserPermission(Bytes.toBytes(USER_RW.getShortName()), TEST_TABLE,
        TEST_FAMILY, Permission.Action.READ, Permission.Action.WRITE));

    protocol.grant(new UserPermission(Bytes.toBytes(USER_RO.getShortName()), TEST_TABLE,
        TEST_FAMILY, Permission.Action.READ));

    protocol.grant(new UserPermission(Bytes.toBytes(USER_CREATE.getShortName()), TEST_TABLE, null,
        Permission.Action.CREATE));


  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void verifyAllowed(User user, PrivilegedExceptionAction... actions) throws Exception {
    for (PrivilegedExceptionAction action : actions) {
      try {
        user.runAs(action);
      } catch (AccessDeniedException ade) {
        fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
      }
    }
  }

  public void verifyAllowed(PrivilegedExceptionAction action, User... users) throws Exception {
    for (User user : users) {
      verifyAllowed(user, action);
    }
  }

  public void verifyDenied(User user, PrivilegedExceptionAction... actions) throws Exception {
    for (PrivilegedExceptionAction action : actions) {
      try {
        user.runAs(action);
        fail("Expected AccessDeniedException for user '" + user.getShortName() + "'");
      } catch (RetriesExhaustedWithDetailsException e) {
        // in case of batch operations, and put, the client assembles a
        // RetriesExhaustedWithDetailsException instead of throwing an
        // AccessDeniedException
        boolean isAccessDeniedException = false;
        for (Throwable ex : e.getCauses()) {
          if (ex instanceof AccessDeniedException) {
            isAccessDeniedException = true;
            break;
          }
        }
        if (!isAccessDeniedException) {
          fail("Not receiving AccessDeniedException for user '" + user.getShortName() + "'");
        }
      } catch (AccessDeniedException ade) {
        // expected result
      }
    }
  }

  public void verifyDenied(PrivilegedExceptionAction action, User... users) throws Exception {
    for (User user : users) {
      verifyDenied(user, action);
    }
  }

  @Test
  public void testGetAddRemove() throws Exception {
    final AtomicLong counter = new AtomicLong(0);

    PrivilegedExceptionAction getGroup = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        GROUP_ENDPOINT.getGroup("default");
        return null;
      }
    };
    verifyAllowed(getGroup, SUPERUSER, USER_ADMIN, USER_CREATE, USER_RW, USER_RO, USER_NONE);

    PrivilegedExceptionAction addGroup = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        GroupInfo group = new GroupInfo("testGetAddRemove"+counter.incrementAndGet(), new TreeSet<String>());
        GROUP_ENDPOINT.addGroup(group);
        return null;
      }
    };
    verifyDenied(addGroup, USER_CREATE, USER_RW, USER_RO, USER_NONE);
    verifyAllowed(addGroup, SUPERUSER, USER_ADMIN);

    PrivilegedExceptionAction removeGroup = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        GROUP_ENDPOINT.removeGroup("testGetAddRemove"+counter.getAndDecrement());
        return null;
      }
    };
    verifyAllowed(removeGroup, SUPERUSER, USER_ADMIN);
    verifyDenied(removeGroup, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testMoveServer() throws Exception {
    final AtomicLong counter = new AtomicLong(0);
    NavigableSet<String> servers = new TreeSet<String>();
    for(int i=1;i<=100;i++) {
      servers.add("foo_"+i+":1");
    }

    GROUP_ENDPOINT.addGroup(new GroupInfo("testMoveServer", servers));
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        GROUP_ENDPOINT.moveServer("foo_"+counter.incrementAndGet()+":1", "default", "testMoveServer");
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testListGroups() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        GROUP_ENDPOINT.listGroups();
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testSetGetGroupPropertyOfTable() throws Exception {
    PrivilegedExceptionAction setGroup = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        GROUP_ENDPOINT.setGroupPropertyOfTable("my_group", new HTableDescriptor("my_table"));
        return null;
      }
    };
    verifyAllowed(setGroup, SUPERUSER, USER_ADMIN, USER_CREATE, USER_RW, USER_RO, USER_NONE);

    PrivilegedExceptionAction getGroup = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        GROUP_ENDPOINT.getGroupPropertyOfTable(new HTableDescriptor("my_table"));
        return null;
      }
    };
    verifyAllowed(getGroup, SUPERUSER, USER_ADMIN, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }
}
