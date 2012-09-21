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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.Lists;

/**
 * This class is responsible for managing region server group information.
 */
public class GroupInfoManager extends Writables {
	private static final Log LOG = LogFactory.getLog(GroupInfoManager.class);
	/** The file name used to store group information in HDFS */
	public static final String GROUP_INFO_FILE_NAME = ".rsgroupinfo";
	public final static String GROUP_ASSIGN_MGR_IMPL = "hbase.assignment.manager.class";
	private final FileSystem fs;
	private MasterServices master;
	private Path path;

	// Store the region server group information,
	// key is group name and value is a GroupInfo instance
	private ConcurrentHashMap<String, GroupInfo> groupMap;

	public GroupInfoManager(MasterServices master) throws IOException {
		this.master = master;
		groupMap = new ConcurrentHashMap<String, GroupInfo>();
		Configuration conf = master.getConfiguration();
		this.path = new Path(FSUtils.getRootDir(conf), GROUP_INFO_FILE_NAME);
		this.fs = FSUtils.getRootDir(conf).getFileSystem(conf);
		this.readConfig();
		if (groupMap.size() == 0) {
			groupMap.put(GroupInfo.DEFAULT_GROUP, new GroupInfo(
					GroupInfo.DEFAULT_GROUP));
		}
	}

	/**
	 * Removes the region server from its existing group.
	 *
	 * @param server
	 *            The region server to be removed.
	 * @throws InterruptedException
	 *             the interrupted exception
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	synchronized void removeServer(ServerName server)
			throws InterruptedException, IOException {
		GroupInfo group = getGroupInfoOfServer(server);
		if (group != null) {
			if (group.getName().equalsIgnoreCase(GroupInfo.DEFAULT_GROUP)
					&& (group.getServers().size() == 1)) {
				LOG.warn("Trying to delete the last server in the default group.");
				throw new IOException(
						"The default group should contain atleast one server.");
			} else {
				// First we unassign all the regions of the region server.
				// Assign the regions to any of the other server in the same
				// group.
				// Remove the region from the group.
				moveOutRegionsFromServer(getRegionsOfServer(server));
				group.remove(server);
				// see if the regions are root and meta and call correct
				// functions.
				List<HRegionInfo> regionsTobeMoved = new ArrayList<HRegionInfo>();
				for (HRegionInfo tobeMoved : getRegionsOfServer(server)) {
					if (tobeMoved.isRootRegion()) {
						regionsTobeMoved.remove(tobeMoved);
						try {
							this.master.getAssignmentManager().assignRoot();
						} catch (KeeperException e) {
							LOG.warn(
									"KeeperException while moving root region.",
									e);
						}
					} else if (tobeMoved.isMetaRegion()) {
						regionsTobeMoved.remove(tobeMoved);
						this.master.getAssignmentManager().assignMeta();
					} else {
						regionsTobeMoved.add(tobeMoved);
					}
				}
				if (regionsTobeMoved.size() > 0) {
					this.master.getAssignmentManager().assignUserRegions(
							regionsTobeMoved,
							this.master.getServerManager()
									.getOnlineServersList());
				}
				writeConfig();
			}
		} else {
			LOG.info("The server to be removed " + server.getHostAndPort()
					+ " does not belong to any group.");
		}
	}

	public synchronized boolean addServers(List<ServerName> servers, String targetGroup) {
		GroupInfo group = null;
		// Lock the group to make sure the target group not be deleted.

		for(String gName: groupMap.keySet()){
			if (groupMap.get(gName).contains(servers)) {
				LOG.info("The list of servers : " + servers
						+ " already belong to group: " + gName + ".");
				return false;
			}
		}
		// If targetGroup is not null, should not change the group
		// configuration.
		if ((targetGroup == null)
				|| (this.getGroupInformation(targetGroup) == null)) {
			LOG.info("Adding " + servers + " to the default group.");
			this.groupMap.get(GroupInfo.DEFAULT_GROUP).add(servers);
		} else {
			group = this.getGroupInformation(targetGroup);
			LOG.info("Adding " + servers + " to the " + group.getName() + " group.");
			group.add(servers);
		}
		try {
			writeConfig();
		} catch (IOException e) {
			LOG.error("Write group configuration error !", e);
			this.groupMap.get(targetGroup).getServers().remove(servers);
			return false;
		}
		return true;
	}

	/**
	 * Write the configuration to HDFS.
	 *
	 * @throws IOException
	 */
	private void writeConfig() throws IOException {
		FSDataOutputStream output = null;
		try {
			output = fs.create(path, true);
			GroupInfo.writeGroups(groupMap.values(), output);
		} finally {
			output.close();
		}
	}

	/**
	 * Read group configuration from HDFS, only used when system starts up.
	 *
	 * @throws IOException
	 */
	private void readConfig() throws IOException {
		List<GroupInfo> groupList;
		FSDataInputStream in = null;
		if (fs.exists(path)) {
			in = fs.open(path);
			try {
				this.groupMap.clear();
				groupList = GroupInfo.readGroups(in);
				for (GroupInfo group : groupList) {
					groupMap.put(group.getName(), group);
				}
			} finally {
				in.close();
			}
		}
	}

	/**
	 * Get regions of a region server.
	 *
	 * @param server
	 *            the name of the region server
	 * @return List of HRegionInfo the region server contains
	 */
	public List<HRegionInfo> getRegionsOfServer(ServerName server) {
		List<HRegionInfo> assignments = null;
		ServerName actual = ServerName.findServerWithSameHostnamePort(master
				.getServerManager().getOnlineServersList(), server);
		if (actual != null) {
			assignments = master.getAssignmentManager().getAssignments()
					.get(actual);

		}
		return assignments != null ? assignments : new ArrayList<HRegionInfo>();
	}

	public GroupInfo getGroupInfoOfServer(ServerName server) {
		for(GroupInfo info : groupMap.values()){
			if(info.getServers().contains(server)){
				return info;
			}
		}
		return null;
	}

	/**
	 * Get regions of a region server group.
	 *
	 * @param groupName
	 *            the name of the group
	 * @return list of regions this group contains
	 */
	public List<HRegionInfo> getRegionsOfGroup(String groupName) {
		List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
		if (groupName == null || !groupExist(groupName)) {
			return regions;
		}
		List<ServerName> servers = groupMap.get(groupName).getServers();
		for (ServerName server : servers) {
			List<HRegionInfo> temp = getRegionsOfServer(server);
			if (temp != null) {
				regions.addAll(temp);
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
	public Collection<String> getTablesOfGroup(String groupName) {
		Set<String> set = new HashSet<String>();
		if (groupName == null || !groupExist(groupName)) {
			return null;
		}
		List<HRegionInfo> regions = getRegionsOfGroup(groupName);
		for (HRegionInfo region : regions) {
			set.add(region.getTableNameAsString());
		}
		return set;
	}

	/**
	 * @param groupName
	 * @return An instance of GroupInfo object.
	 */
	public GroupInfo getGroupInformation(String groupName) {
		return this.groupMap.get(groupName);
	}

	/**
	 * Gets the region server group info of a table.
	 *
	 * @param tableName
	 *            the table name
	 * @return the group info of table
	 */
	public GroupInfo getGroupInfoOfTable(String tableName) {
		HTableDescriptor des;
		GroupInfo tableRSGroup;
		try {
			des = master.getTableDescriptors().get(tableName);
		} catch (FileNotFoundException e) {
			LOG.error(
					"FileNotFoundException while retrieving region server group info.",
					e);
			return null;
		} catch (IOException e) {
			LOG.error("IOException while retrieving region server group info.",
					e);
			return null;
		}
		String group = GroupInfo.getGroupString(des);
		tableRSGroup = groupMap.get(group);
		return tableRSGroup;
	}

	/**
	 * Get servers which this table can use,depending on its group information
	 *
	 * @param tableName
	 * @return A list of ServerName.
	 */
	public List<ServerName> getAvailableServersForTable(String tableName) {
		GroupInfo group = getGroupInfoOfTable(tableName);
		return group.getServers();
	}

	/**
	 * Check if the region and server are in the some group.
	 *
	 * @param regionInfo
	 * @param serverInfo
	 * @return
	 *//*
	boolean inSameGroup(HRegionInfo regionInfo, ServerName serverInfo) {
		GroupInfo rsGroup = getGroupInfoOfTable(regionInfo
				.getTableNameAsString());
		if (rsGroup.contains(serverInfo)) {
			return true;
		}
		return false;
	}*/

	/**
	 * Filter servers which are being moved from the list.
	 *
	 * @param servers
	 * @return A list of ServerName.
	 */
	List<ServerName> filterServers(List<ServerName> servers,
			List<ServerName> onlineServers) {
		ArrayList<ServerName> finalList = new ArrayList<ServerName>();
		for (ServerName server : servers) {
			ServerName actual = GroupBasedLoadBalancer.getServerName(
					onlineServers, server);
			if (actual != null) {
				finalList.add(actual);
			}
		}
		return finalList;
	}

	/**
	 * Gets the group to GroupInfo mapping.
	 *
	 * @return the group mapping
	 */
	public Collection<GroupInfo> getExistingGroups() {
		return groupMap.values();
	}

	/**
	 * Check the groups,if there is one group of the string exists, return true.
	 *
	 * @param grpName
	 * @return
	 */
	boolean groupExist(String grpName) {
		if (grpName == null) {
			return false;
		}
		return this.groupMap.containsKey(grpName);
	}

	/**
	 * Delete a group,the group mustn't be default group or contain any servers
	 *
	 * @param groupName
	 *            The name of group you want to delete
	 * @return true if delete successfully
	 */
	public synchronized boolean deleteGroup(String groupName) {
		boolean isDeleteSuccess = false;
		GroupInfo group = groupMap.get(groupName);
		if (group == null) {
			isDeleteSuccess = true;
		} else if (group.isDefault() || group.getServers().size() != 0) {
			LOG.info("The group to be deleted is either the default group or it is not empty.");
			isDeleteSuccess = false;
		} else {
			try {
				groupMap.remove(groupName);
				writeConfig();
				isDeleteSuccess = true;
			} catch (IOException e) {
				LOG.error("Error while writing group configuration.", e);
				groupMap.put(groupName, null);
				isDeleteSuccess = false;
			}
		}
		return isDeleteSuccess;
	}

	/**
	 * Add a new group.
	 *
	 * @param groupName
	 *            The name of group you want to add.
	 * @return true if add successfully
	 */
	public boolean addGroup(String groupName) {
		try {
			if (groupMap.get(groupName) == null) {
				GroupInfo g = new GroupInfo(groupName);
				groupMap.put(g.getName(), g);
				writeConfig();
			}
		} catch (IOException e) {
			LOG.error("Write group configuration error !", e);
			deleteGroup(groupName);
			return false;
		}
		return true;
	}

	/**
	 * Move table to group.
	 *
	 * @param conf
	 *            An instance of HBase configuration
	 * @param targetGroup
	 *            The destination group for the table.
	 * @param tableName
	 *            The HBase table name.
	 * @return true, if successful
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	public boolean moveTableToGroup(String targetGroup,
			String tableName) throws IOException, InterruptedException {
		if (targetGroup == null || tableName == null) {
			LOG.info("The table name or the target region server group found to be null.");
			return false;
		}
		HBaseAdmin admin = new HBaseAdmin(master.getConfiguration());
		HTableDescriptor des = admin.getTableDescriptor(Bytes
				.toBytes(tableName));
		if (des == null) {
			throw new IOException("Unable to obtain HTableDescriptor for "
					+ tableName);
		} else {
			admin.disableTable(tableName);
			byte[] gbyte = Bytes.toBytes(targetGroup);
			des.setValue(GroupInfo.GROUP_KEY, gbyte);
			admin.modifyTable(des.getName(), des);
			List<HRegionInfo> tableRegionList = admin.getTableRegions(Bytes
					.toBytes(tableName));
			this.master.getAssignmentManager().unassign(tableRegionList);
			this.master.getAssignmentManager().assignUserRegions(
					tableRegionList,this.master.getServerManager().getOnlineServersList());
			admin.enableTable(tableName);
		}
		return true;
	}

	public void refresh(){
		try {
			this.readConfig();
		} catch (IOException e) {
			LOG.warn("IOException while refreshing group config.", e);
		}
	}

	/**
	 * Carry out the server movement from one group to another.
	 *
	 * @param serverMovePlan
	 *            {@link GroupOperations#ServerMovingPlan}
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public synchronized void moveServer(ServerPlan serverMovePlan)
			throws IOException, InterruptedException {
		if (serverMovePlan == null) {
			throw new IOException(
					"The region server move plan found to be null.");
		}

		LOG.debug("Plan move "
				+ serverMovePlan.getServername().getHostAndPort()
				+ " from Group:" + serverMovePlan.getSourceGroup()
				+ " to Group:" + serverMovePlan.getTargetGroup());
		removeServer(serverMovePlan.getServername());
		if (addServers(Lists.newArrayList(serverMovePlan.getServername()),
				serverMovePlan.getTargetGroup())) {
			LOG.debug("Successfully move "
					+ serverMovePlan.getServername().getHostAndPort()
					+ " from group " + serverMovePlan.getSourceGroup()
					+ " to group " + serverMovePlan.getTargetGroup());
		} else {
			// There was some failure while moving, so add the rs
			// again to the previous group.
			LOG.debug("Added the region server back to source group.");
			addServers(Lists.newArrayList(serverMovePlan.getServername()),
					serverMovePlan.getSourceGroup());
		}
	}

	private void moveOutRegionsFromServer(List<HRegionInfo> regions)
			throws IOException {
		if (regions != null) {
			this.master.getAssignmentManager().unassign(regions);
		}

	}

	/**
	 * Stores the plan for the move of an individual server.
	 *
	 * Contains {@link ServerName} for the server being moved, {@link GroupInfo}
	 * for the group the server should be moved from, the server should be moved
	 * to.
	 *
	 * The comparable implementation of this class compares only the ServerName
	 * information and not the source/dest group info.
	 */
	public static class ServerPlan implements Comparable<ServerPlan> {
		private ServerName servername;
		private String sourceGroup;
		private String targetGroup;

		public ServerPlan(ServerName serverName, String sourceGroup,
				String targetGroup) {
			this.servername = serverName;
			this.sourceGroup = sourceGroup;
			this.targetGroup = targetGroup;
		}

		/**
		 * Get the ServerName
		 *
		 * @return
		 */
		public ServerName getServername() {
			return servername;
		}

		/**
		 * Set the serverName
		 *
		 * @param servername
		 */
		public void setServerName(ServerName servername) {
			this.servername = servername;
		}

		/**
		 * Get the source group.
		 *
		 * @return
		 */
		public String getSourceGroup() {
			return sourceGroup;
		}

		/**
		 * Set the source group.
		 *
		 * @param sourceGroup
		 */
		public void setSourceGroup(String sourceGroup) {
			this.sourceGroup = sourceGroup;
		}

		/**
		 * Get the target group.
		 *
		 * @return
		 */
		public String getTargetGroup() {
			return targetGroup;
		}

		/**
		 * Set the target group.
		 *
		 * @param targetGroup
		 */
		public void setTargetGroup(String targetGroup) {
			this.targetGroup = targetGroup;
		}

		@Override
		public int compareTo(ServerPlan other) {
			return getServername().compareTo(other.getServername());
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(getServername() + ":" + getSourceGroup() + ":"
					+ getTargetGroup());
			return sb.toString();
		}

		@Override
		public int hashCode() {
			return servername.hashCode();
		}
	}
}
