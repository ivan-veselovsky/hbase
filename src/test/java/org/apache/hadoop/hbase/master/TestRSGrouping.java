/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.GroupInfoManager.ServerPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestRSGrouping {
	private static HBaseTestingUtility TEST_UTIL;
	private static HMaster master;

	@BeforeClass
	public static void setUp() throws Exception {
		TEST_UTIL = new HBaseTestingUtility();
		TEST_UTIL.getConfiguration().set(
				HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
				GroupBasedLoadBalancer.class.getName());
		TEST_UTIL.startMiniCluster(4);
		MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
		master = cluster.getMaster();

	}

	@AfterClass
	public static void tearDown() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}

	@Test
	public void testBasicStartUp() throws IOException {
		GroupInfoManager groupManager = new GroupInfoManager(master);
		GroupInfo defaultInfo = groupManager
				.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		defaultInfo = groupManager.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		assertTrue(defaultInfo.getServers().size() == 4);
		// Assignment of root and meta regions.
		assertTrue(groupManager.getRegionsOfGroup(GroupInfo.DEFAULT_GROUP)
				.size() == 2);
		TEST_UTIL.getDFSCluster().getFileSystem().delete(new Path(FSUtils.getRootDir(master
				.getConfiguration()), GroupInfoManager.GROUP_INFO_FILE_NAME), true);
	}

	@Test
	public void testSimpleRegionServerMove() throws IOException, InterruptedException {
		GroupInfoManager groupManager = new GroupInfoManager(master);
		String adminGrp = "admin";
		addGroup(groupManager, adminGrp,1);
		groupManager.refresh();
		GroupInfo dInfo = groupManager.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		master.getAssignmentManager().refreshBalancer();
		addGroup(groupManager, "application1",1);
		//Force the group info manager to read group information from disk.
		groupManager.refresh();
		assertTrue(groupManager.getExistingGroups().size() == 3);
		dInfo = groupManager.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		GroupInfo appInfo = groupManager.getGroupInformation("application1");
		GroupInfo adminInfo = groupManager.getGroupInformation("admin");
		assertTrue(adminInfo.getServers().size() == 1);
		assertTrue(appInfo.getServers().size() == 1);
		assertTrue(dInfo.getServers().size() == 2);
		groupManager.moveServer(new ServerPlan(appInfo.getServers().get(0),
				"application1", GroupInfo.DEFAULT_GROUP));
		groupManager.deleteGroup("application1");
		groupManager.moveServer(new ServerPlan(adminInfo.getServers().get(0),
				"admin", GroupInfo.DEFAULT_GROUP));
		groupManager.deleteGroup("admin");
		groupManager.refresh();
		assertTrue(groupManager.getExistingGroups().size() == 1);
		TEST_UTIL.getDFSCluster().getFileSystem().delete(new Path(FSUtils.getRootDir(master
				.getConfiguration()), GroupInfoManager.GROUP_INFO_FILE_NAME), true);
	}

	@Test
	public void testTableMove() throws IOException, InterruptedException {
		String tableName = "FOO";
		byte[] TABLENAME = Bytes.toBytes(tableName);
		byte[] FAMILYNAME = Bytes.toBytes("fam");
		GroupInfoManager groupManager = new GroupInfoManager(master);
		String newGroupName = "temptables";
		addGroup(groupManager, newGroupName,2);
		groupManager.refresh();

		HTable ht = TEST_UTIL.createTable(TABLENAME, FAMILYNAME);
		assertTrue(TEST_UTIL.createMultiRegions(master.getConfiguration(), ht,
				FAMILYNAME, 4) == 4);
		TEST_UTIL.waitUntilAllRegionsAssigned(4);
		assertTrue(master.getAssignmentManager().getZKTable()
				.isEnabledTable(Bytes.toString(TABLENAME)));
		List<HRegionInfo> regionList = TEST_UTIL.getHBaseAdmin()
				.getTableRegions(TABLENAME);
		assertTrue(regionList.size() > 0);
		GroupInfo tableGrp = groupManager.getGroupInfoOfTable(tableName);
		assertTrue(tableGrp.getName().equals(GroupInfo.DEFAULT_GROUP));

		master.getAssignmentManager().refreshBalancer();
		groupManager.moveTableToGroup(newGroupName, tableName);
		groupManager.refresh();
		GroupInfo newTableGrp = groupManager.getGroupInfoOfTable(tableName);
		assertTrue(newTableGrp.getName().equals(newGroupName));
		Map<String, Map<ServerName, List<HRegionInfo>>> tableRegionAssignMap = master
				.getAssignmentManager().getAssignmentsByTable();
		assertTrue(tableRegionAssignMap.keySet().size() == 1);
		Map<ServerName, List<HRegionInfo>> serverMap = tableRegionAssignMap
				.get(tableName);
		for (ServerName rs : serverMap.keySet()) {
			if (serverMap.get(rs).size() > 0) {
				assertTrue(newTableGrp.contains(rs));
			}
		}

		TEST_UTIL.deleteTable(TABLENAME);
		tableRegionAssignMap = master.getAssignmentManager().getAssignmentsByTable();
		assertTrue(tableRegionAssignMap.size() == 0);
		TEST_UTIL.getDFSCluster().getFileSystem().delete(new Path(FSUtils.getRootDir(master
				.getConfiguration()), GroupInfoManager.GROUP_INFO_FILE_NAME), true);
	}

	@Test
	public void testDefaultGroupOperations() throws IOException, InterruptedException {
		GroupInfoManager groupManager = new GroupInfoManager(master);
		String userTableGroup = "usertables";
		GroupInfo dInfo = groupManager.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		assertFalse(groupManager.deleteGroup(GroupInfo.DEFAULT_GROUP));

		// Now create a new group and try moving all the four region servers
		// to the new group.
		assertTrue(groupManager.addGroup(userTableGroup));
		Iterator<ServerName> itr = dInfo.getServers().iterator();
		master.getAssignmentManager().refreshBalancer();
		groupManager.moveServer(new ServerPlan(itr.next(),
				GroupInfo.DEFAULT_GROUP, userTableGroup));
		master.getAssignmentManager().refreshBalancer();
		groupManager.moveServer(new ServerPlan(itr.next(),
				GroupInfo.DEFAULT_GROUP, userTableGroup));
		master.getAssignmentManager().refreshBalancer();
		groupManager.moveServer(new ServerPlan(itr.next(),
				GroupInfo.DEFAULT_GROUP, userTableGroup));
		master.getAssignmentManager().refreshBalancer();
		boolean exceptionCaught = false;
		try{
		groupManager.moveServer(new ServerPlan(itr.next(),
				GroupInfo.DEFAULT_GROUP, userTableGroup));
		}catch (Exception exp){
			if(exp instanceof IOException){
				exceptionCaught = true;
				assertTrue(exp.getMessage().contains(
						"The default group should contain atleast one server."));
			}
		}
		assertTrue(exceptionCaught);
		TEST_UTIL.getDFSCluster().getFileSystem().delete(new Path(FSUtils.getRootDir(master
				.getConfiguration()), GroupInfoManager.GROUP_INFO_FILE_NAME), true);
	}


	@Test
	public void testRegionMove() throws IOException, InterruptedException{
		GroupInfoManager groupManager = new GroupInfoManager(master);
		String newGroupName = "temptables";
		addGroup(groupManager, newGroupName,1);
		groupManager.refresh();
		String tableNameOne = "TABLE-One";
		byte[] tableOneBytes = Bytes.toBytes(tableNameOne);
		byte[] familyOneBytes = Bytes.toBytes("fam1");
		master.getAssignmentManager().refreshBalancer();
		HTable ht = TEST_UTIL.createTable(tableOneBytes, familyOneBytes);
		// All the regions created below will be assigned to the default group.
		assertTrue(TEST_UTIL.createMultiRegions(master.getConfiguration(), ht,
				familyOneBytes, 10) == 10);
		TEST_UTIL.waitUntilAllRegionsAssigned(10);
		List<HRegionInfo> regions = groupManager.getRegionsOfGroup(GroupInfo.DEFAULT_GROUP);
		HRegionInfo region = regions.get(0);
		//Lets move this region to temptables group.
		master.getAssignmentManager().refreshBalancer();
		ServerName tobeAssigned = groupManager
		.getGroupInformation(newGroupName).getServers().get(0);
		master.move(region.getEncodedNameAsBytes(), Bytes.toBytes(tobeAssigned.toString()));
		groupManager.refresh();

		List<HRegionInfo> updatedRegions = groupManager.getRegionsOfGroup(GroupInfo.DEFAULT_GROUP);
		assertTrue(regions.size() == updatedRegions.size());
		assertFalse(groupManager.getRegionsOfServer(tobeAssigned).contains(region));
		TEST_UTIL.getDFSCluster().getFileSystem().delete(new Path(FSUtils.getRootDir(master
				.getConfiguration()), GroupInfoManager.GROUP_INFO_FILE_NAME), true);
	}

	private void addGroup(GroupInfoManager gManager, String groupName,
			int servers) throws IOException, InterruptedException {
		GroupInfo defaultInfo = gManager
				.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		assertTrue(defaultInfo != null);
		assertTrue(defaultInfo.getServers().size() >= servers);
		assertTrue(gManager.addGroup(groupName));
		Iterator<ServerName> itr = defaultInfo.getServers().iterator();
		for (int i = 0; i < servers; i++) {
			gManager.moveServer(new ServerPlan(itr.next(),
					GroupInfo.DEFAULT_GROUP, groupName));
		}
		gManager.refresh();
		assertTrue(gManager.getGroupInformation(groupName).getServers().size() == servers);
	}

}
