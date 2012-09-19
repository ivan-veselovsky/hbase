package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.GroupInfoManager.ServerPlan;
import org.apache.hadoop.hbase.util.Bytes;
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
		assertTrue(defaultInfo != null);
		assertTrue(defaultInfo.getServers().size() == 0);
		groupManager.addServers(master.getServerManager()
				.getOnlineServersList(), GroupInfo.DEFAULT_GROUP);
		defaultInfo = groupManager.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		assertTrue(defaultInfo.getServers().size() == 4);
		assertTrue(groupManager.getRegionsOfGroup(GroupInfo.DEFAULT_GROUP)
				.size() == 2);
	}

	@Test
	public void testSimpleRegionServerMove() throws IOException, InterruptedException {
		GroupInfoManager groupManager = new GroupInfoManager(master);
		GroupInfo dInfo = groupManager
				.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		if(dInfo.getServers().size() == 0){
			groupManager.addServers(master.getServerManager()
					.getOnlineServersList(), GroupInfo.DEFAULT_GROUP);
		}
		assertTrue(groupManager.addGroup("admin"));
		Iterator<ServerName> itr = dInfo.getServers().iterator();
		master.getAssignmentManager().refreshBalancer();
		groupManager.moveServer(new ServerPlan(itr.next(),
				GroupInfo.DEFAULT_GROUP, "admin"));
		master.getAssignmentManager().refreshBalancer();
		assertTrue(groupManager.addGroup("application1"));
		groupManager.moveServer(new ServerPlan(itr.next(),
				GroupInfo.DEFAULT_GROUP, "application1"));
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
	}

	@Test
	public void testTableMove() throws IOException, InterruptedException {
		String tableName = "FOO";
		String newGroupName = "temptables";
		byte[] TABLENAME = Bytes.toBytes(tableName);
		byte[] FAMILYNAME = Bytes.toBytes("fam");
		GroupInfoManager groupManager = new GroupInfoManager(master);
		GroupInfo dInfo = groupManager.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		if(dInfo.getServers().size() == 0){
			groupManager.addServers(master.getServerManager()
					.getOnlineServersList(), GroupInfo.DEFAULT_GROUP);
		}
		Iterator<ServerName> itr = dInfo.getServers().iterator();
		assertTrue(groupManager.addGroup(newGroupName));
		master.getAssignmentManager().refreshBalancer();
		groupManager.moveServer(new ServerPlan(itr.next(),
				GroupInfo.DEFAULT_GROUP, newGroupName));

		HTable ht = TEST_UTIL.createTable(TABLENAME, FAMILYNAME);
		assertTrue(master.getAssignmentManager().getZKTable()
				.isEnabledTable(Bytes.toString(TABLENAME)));
		TEST_UTIL.loadTable(ht, FAMILYNAME);
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
	}

	@Test
	public void testDeleteDefaultGroup() {
		// create a new table with no group specified.
		// see if the table is assigned to default group's RS.
	}

	/*@Test
	public void testTableMultiRegionAssignment() throws IOException, InterruptedException {
		GroupInfoManager groupManager = new GroupInfoManager(master);
		GroupInfo dInfo = groupManager.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		if(dInfo.getServers().size() == 0){
			groupManager.addServers(master.getServerManager()
					.getOnlineServersList(), GroupInfo.DEFAULT_GROUP);
		}
		String tableName = "TABLE-A";
		String newGroupName = "usertables";
		byte[] TABLENAME = Bytes.toBytes(tableName);
		byte[] FAMILYNAME = Bytes.toBytes("fam1");
		assertTrue(groupManager.addGroup(newGroupName));
		Iterator<ServerName> itr = dInfo.getServers().iterator();
		master.getAssignmentManager().refreshBalancer();
		groupManager.moveServer(new ServerPlan(itr.next(),
				GroupInfo.DEFAULT_GROUP, newGroupName));
		master.getAssignmentManager().refreshBalancer();
		groupManager.moveServer(new ServerPlan(itr.next(),
				GroupInfo.DEFAULT_GROUP, newGroupName));
		assertTrue(groupManager.getGroupInformation(newGroupName).getServers().size() == 2);

		HTable ht = TEST_UTIL.createTable(TABLENAME, FAMILYNAME);
		// All the regions created below will be assigned to the default group.
		assertTrue(TEST_UTIL.createMultiRegions(master.getConfiguration(), ht, FAMILYNAME,10) == 10);
		//Now move the table to the usertables group.
		master.getAssignmentManager().refreshBalancer();
		groupManager.moveTableToGroup(newGroupName, tableName);

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

		assertTrue(master.balance());
		ClusterStatus status = master.getClusterStatus();
		dInfo = groupManager.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		itr = dInfo.getServers().iterator();
		ServerName sn1 = ServerName.findServerWithSameHostnamePort(dInfo.getServers(), itr.next());
		ServerName sn2 = ServerName.findServerWithSameHostnamePort(dInfo.getServers(), itr.next());
		assertTrue(status.getLoad(sn1).getLoad() == status.getLoad(sn2).getLoad());

		GroupInfo newGroupInfo = groupManager.getGroupInformation(newGroupName);
		itr = newGroupInfo.getServers().iterator();
		ServerName sn3 = ServerName.findServerWithSameHostnamePort(newGroupInfo.getServers(), itr.next());
		ServerName sn4 = ServerName.findServerWithSameHostnamePort(newGroupInfo.getServers(), itr.next());
		assertTrue(status.getLoad(sn3).getLoad() == status.getLoad(sn4).getLoad());

		// Now we move one of the region servers from default group to the usertables group.

		TEST_UTIL.deleteTable(TABLENAME);
		tableRegionAssignMap = master.getAssignmentManager().getAssignmentsByTable();
		assertTrue(tableRegionAssignMap.size() == 0);

	}*/

}
