/**
 * Copyright The Apache Software Foundation
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.zookeeper.RegionServerTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This class is responsible for managing region server group information.
 */
public class GroupAdmin {
	private static final Log LOG = LogFactory.getLog(GroupAdmin.class);

  private final GroupInfoManager groupManager;
  private final HConnection connection;
  private final HBaseAdmin admin;
  private final RegionServerTracker rsTracker;

	public GroupAdmin(Configuration conf) throws IOException {
    this.groupManager = new GroupInfoManagerProxy(conf);
    this.admin = new HBaseAdmin(conf);
    this.connection = admin.getConnection();
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "test_watcher", null);
    this.rsTracker = new RegionServerTracker(zkw, null, null);
    try {
      rsTracker.start();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private List<HRegionInfo> getOnlineRegions(String hostPort) throws IOException {
    String split[] = hostPort.split(":",2);
    HRegionInterface server = connection.getHRegionConnection(split[0], Integer.parseInt(split[1]));
    return server.getOnlineRegions();
  }

  private void unassignRegions(List<HRegionInfo> regions) throws IOException {
    for(HRegionInfo region: regions) {
      admin.unassign(region.getRegionName(), false);
    }
  }

	/**
	 * Get regions of a region server group.
	 *
	 * @param groupName
	 *            the name of the group
	 * @return list of regions this group contains
	 */
  public List<HRegionInfo> listRegionsOfGroup(String groupName) throws IOException {
		List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
		if (groupName == null) {
      throw new NullPointerException("groupName can't be null");
    }

    GroupInfo groupInfo = groupManager.getGroup(groupName);
    if (groupInfo == null) {
			return null;
		} else {
			NavigableSet<String> servers = groupInfo.getServers();
      for(ServerName serverName: rsTracker.getOnlineServers()) {
        String hostPort = serverName.getHostAndPort();
        if(servers.contains(hostPort)) {
          List<HRegionInfo> temp = getOnlineRegions(hostPort);
          regions.addAll(temp);
        }
			}
		}
		return regions;
	}

	/**
	 * Get tables of a group.
	 *
	 * @param groupName
	 *            the name of the group
	 * @return List of HTableDescriptor
	 */
  public Collection<String> listTablesOfGroup(String groupName) throws IOException {
		Set<String> set = new HashSet<String>();
		if (groupName == null) {
      throw new NullPointerException("groupName can't be null");
    }

    GroupInfo groupInfo = groupManager.getGroup(groupName);
    if (groupInfo == null) {
			return null;
		} else {
      HTableDescriptor[] tables = admin.listTables();
      for (HTableDescriptor table : tables) {
        if(GroupInfo.getGroupString(table).equals(groupName))
          set.add(table.getNameAsString());
      }
    }
		return set;
	}


	/**
	 * Gets the group information.
	 *
	 * @param groupName the group name
	 * @return An instance of GroupInfo
	 */
  public GroupInfo getGroupInfo(String groupName) throws IOException {
			return groupManager.getGroup(groupName);
	}


	/**
	 * Gets the group info of table.
	 *
	 * @param tableName the table name
	 * @return An instance of GroupInfo.
	 */
  public GroupInfo getGroupInfoOfTable(byte[] tableName) throws IOException {
		HTableDescriptor des;
		GroupInfo tableRSGroup;
    des =  connection.getHTableDescriptor(tableName);
		String group = GroupInfo.getGroupString(des);
		tableRSGroup = groupManager.getGroup(group);
		return tableRSGroup;
	}

	/**
	 * Gets the existing groups.
	 *
	 * @return Collection of GroupInfo.
	 */
  public Collection<GroupInfo> getExistingGroups() throws IOException {
		return groupManager.listGroups();
	}

	/**
	 * Carry out the server movement from one group to another.
	 *
	 * @param server the server
	 * @param sourceGroup the source group
	 * @param targetGroup the target group
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException the interrupted exception
	 */
  //TODO create bulk approach
  public synchronized void moveServer(String server, String sourceGroup, String targetGroup)
			throws IOException, InterruptedException {
		if ((server == null) || (StringUtils.isEmpty(targetGroup))) {
			throw new IOException(
					"The region server or the target to move found to be null.");
		}

    long period = 10000;
    long tries = 30*60*1000/period;
    boolean isTrans = false;

    String transName = sourceGroup;
    if(!sourceGroup.startsWith(GroupInfo.TRANSITION_GROUP_PREFIX)) {
      transName = GroupInfo.TRANSITION_GROUP_PREFIX+sourceGroup+"_TO_"+targetGroup;
      groupManager.addGroup(new GroupInfo(transName, new TreeSet<String>()));
      isTrans = false;
    }


    groupManager.moveServer(server, sourceGroup, transName);
    int size = 0;
    do {
      unassignRegions(getOnlineRegions(server));
      Thread.sleep(period);
    } while(getOnlineRegions(server).size() > 0 && --tries > 0);

    if(tries == 0) {
      throw new DoNotRetryIOException("Waiting too long for regions to be unassigned.");
    }
    groupManager.moveServer(server, transName, targetGroup);
    if(!isTrans) {
      groupManager.removeGroup(transName);
    }
	}

  public void addGroup(GroupInfo groupInfo) throws IOException {
    groupManager.addGroup(groupInfo);
  }

  public void removeGroup(String name) throws IOException {
    groupManager.removeGroup(name);
  }

  public List<GroupInfo> listGroups() throws IOException {
    return groupManager.listGroups();
  }

  public String getGroupPropertyOfTable(HTableDescriptor desc) throws IOException {
    return groupManager.getGroupPropertyOfTable(desc);
  }

  public void setGroupPropertyOfTable(String groupName, HTableDescriptor desc) throws IOException {
    groupManager.setGroupPropertyOfTable(groupName, desc);
  }
}
