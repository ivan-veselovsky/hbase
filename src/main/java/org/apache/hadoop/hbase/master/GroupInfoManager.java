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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKTableReadOnly;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.Lists;

/**
 * This class is responsible for managing region server group information.
 */
public class GroupInfoManager {
	private static final Log LOG = LogFactory.getLog(GroupInfoManager.class);
	public static final String GROUP_INFO_FILE_NAME = ".rsgroupinfo";

	private Set<ServerName> serversInTransition = Collections
	.newSetFromMap(new ConcurrentHashMap<ServerName, Boolean>());
	private final FileSystem fs;
	private MasterServices master;
	private final Path path;
	private final long pause;
	private final int numRetries;
	private final int retryLongerMultiplier;
	//Access to this map should always be synchronized.
	private ConcurrentHashMap<String, GroupInfo> groupMap;

	public GroupInfoManager(MasterServices master) throws IOException {
		this.master = master;
		groupMap = new ConcurrentHashMap<String, GroupInfo>();
		Configuration conf = master.getConfiguration();
		this.path = new Path(FSUtils.getRootDir(conf), GROUP_INFO_FILE_NAME);
		this.fs = FSUtils.getRootDir(conf).getFileSystem(conf);
		this.pause = conf.getLong("hbase.client.pause", 1000);
		this.numRetries = conf.getInt("hbase.client.retries.number", 10);
	    this.retryLongerMultiplier = conf.getInt(
	        "hbase.client.retries.longer.multiplier", 10);
		this.readConfig();
	}

	/**
	 * Removes the server from the region server group to which it
	 * belongs presently.
	 * @param server The ServerName of the serve to remove.
	 * @throws InterruptedException the interrupted exception
	 * @throws IOException
	 */
	private void removeServer(ServerName server) throws InterruptedException,
			IOException {
		GroupInfo group = getGroupInfoOfServer(server);
		if (group != null) {
			List<HRegionInfo> regions = getRegionsOfServer(server);
			synchronized (this.groupMap) {
				if (regions != null) {
					this.master.getAssignmentManager().unassign(regions);
				}
				if (group.getName().equalsIgnoreCase(GroupInfo.DEFAULT_GROUP) == false) {
					this.groupMap.get(group.getName()).remove(server);
				}
				this.serversInTransition.add(server);
				LOG.info(server.getHostAndPort() + "added to transition list.");
			}
		} else {
			throw new IOException("The server to be removed "
					+ server.getHostAndPort()
					+ " does not belong to any group.");
		}
	}

	/**
	 * Adds the server to a given region server group.
	 *
	 * @param server The ServerName of the serve to add.
	 * @param targetGroup The name of the region server group.
	* @throws IOException
	 */
	private void addServer(ServerName server, String targetGroup) throws IOException{
		GroupInfo targetGroupInfo = getGroupInformation(targetGroup);
		if (targetGroupInfo.contains(server)) {
			serversInTransition.remove(server);
			LOG.info("The server : " + server + " is already belongs to target group.");
		} else if ((targetGroupInfo == null)
				|| targetGroup.equalsIgnoreCase(GroupInfo.DEFAULT_GROUP)) {
			LOG.info(server + "  wil belong to the default group.");
			serversInTransition.remove(server);
		} else {
			synchronized (this.groupMap) {
				this.groupMap.get(targetGroup).add(server);
				LOG.info("Added " + server + " to the " + targetGroup
						+ " group.");
				serversInTransition.remove(server);
			}
		}
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
			List<GroupInfo> groups = Lists.newArrayList(groupMap.values());
			groups.add(getGroupInformation(GroupInfo.DEFAULT_GROUP));
			GroupInfo.writeGroups(groups, output);
		} finally {
			output.close();
		}
	}

	/**
	 * Read group configuration from HDFS.
	 *
	 * @throws IOException
	 */
	private void readConfig() throws IOException {
		List<GroupInfo> groupList;
		FSDataInputStream in = null;
		if (fs.exists(path)) {
			in = fs.open(path);
			try {
				synchronized (groupMap) {
					this.groupMap.clear();
					groupList = GroupInfo.readGroups(in);
					for (GroupInfo group : groupList) {
						groupMap.put(group.getName(), group);
					}
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

	/**
	 * Gets the group info of server.
	 *
	 * @param server the server
	 * @return An instance of GroupInfo.
	 */
	public GroupInfo getGroupInfoOfServer(ServerName server) {
		ArrayList<GroupInfo> groups = Lists.newArrayList(groupMap.values());
		groups.add(getGroupInformation(GroupInfo.DEFAULT_GROUP));
		for(GroupInfo info : groups){
			if(info.contains(server)){
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
		} else {
			List<ServerName> servers = getGroupInformation(groupName).getServers();
			for (ServerName server : servers) {
				List<HRegionInfo> temp = getRegionsOfServer(server);
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
	 * Gets the group information.
	 *
	 * @param groupName the group name
	 * @return An instance of GroupInfo
	 */
	public GroupInfo getGroupInformation(String groupName) {
		if (groupName.equalsIgnoreCase(GroupInfo.DEFAULT_GROUP)) {
			GroupInfo defaultInfo = new GroupInfo(GroupInfo.DEFAULT_GROUP);
			List<ServerName> unassignedServers = difference(this.master.getServerManager()
					.getOnlineServersList(),getAssignedServers());
			defaultInfo.addAll(difference(unassignedServers, serversInTransition));
			return defaultInfo;
		} else {
			return this.groupMap.get(groupName);
		}
	}


	/**
	 * Gets the group info of table.
	 *
	 * @param tableName the table name
	 * @return An instance of GroupInfo.
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
		tableRSGroup = getGroupInformation(group);
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
	 * Filter servers based on the online servers.
	 *
	 * @param servers the servers
	 * @param onlineServers List of servers which are online.
	 * @return the list
	 */
	List<ServerName> filterServers(List<ServerName> servers,
			List<ServerName> onlineServers) {
		ArrayList<ServerName> finalList = new ArrayList<ServerName>();
		for (ServerName server : servers) {
			ServerName actual = ServerName.findServerWithSameHostnamePort(
					onlineServers, server);
			if (actual != null) {
				finalList.add(actual);
			}
		}
		return finalList;
	}

	List<ServerName> difference(Collection<ServerName> onlineServers,
			Collection<ServerName> servers) {
		if(servers.size() == 0){
			return Lists.newArrayList(onlineServers);
		} else {
			ArrayList<ServerName> finalList = new ArrayList<ServerName>();
			for (ServerName olServer : onlineServers) {
				ServerName actual = ServerName.findServerWithSameHostnamePort(
						servers, olServer);
				if (actual == null) {
					finalList.add(olServer);
				}
			}
			return finalList;
		}
	}


	/**
	 * Gets the existing groups.
	 *
	 * @return Collection of GroupInfo.
	 */
	public Collection<GroupInfo> getExistingGroups() {
		List<GroupInfo> groupsInfo = Lists.newArrayList(groupMap.values());
		groupsInfo.add(getGroupInformation(GroupInfo.DEFAULT_GROUP));
		return groupsInfo;
	}

	private boolean groupExist(String grpName) {
		if (grpName == null) {
			return false;
		} else if (grpName.equalsIgnoreCase(GroupInfo.DEFAULT_GROUP)) {
			return true;
		} else {
			return this.groupMap.containsKey(grpName);
		}
	}


	/**
	 * Delete a region server group.
	 *
	 * @param groupName the group name
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void deleteGroup(String groupName) throws IOException {
		GroupInfo group = getGroupInformation(groupName);
		if (groupName.equalsIgnoreCase(GroupInfo.DEFAULT_GROUP)) {
			throw new IOException("Trying to delete default group.");
		} else if ((group != null) && (group.getServers().size() != 0)) {
			throw new IOException("The group to be deleted is not empty.");
		} else {
			synchronized (groupMap) {
				try {
					groupMap.remove(groupName);
					writeConfig();
				} catch (IOException e) {
					groupMap.put(groupName, new GroupInfo(groupName));
					throw e;
				}
			}
		}
	}


	/**
	 * Adds the group.
	 *
	 * @param groupName the group name
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public synchronized void addGroup(String groupName) throws IOException {
		if (groupMap.get(groupName) == null) {
			GroupInfo gInfo = new GroupInfo(groupName);
			groupMap.put(groupName, gInfo);
			try {
				writeConfig();
			} catch (IOException e) {
				groupMap.remove(groupName);
				throw e;
			}
		}
	}


	public boolean moveTableToGroup(String targetGroup,
			String tableName) throws IOException, InterruptedException {
		if (targetGroup == null || tableName == null) {
			LOG.info("The table name or the target region server group found to be null.");
			return false;
		}
		HMaster hMaster = (HMaster) master;
		HTableDescriptor[] tableDescriptor = hMaster.getHTableDescriptors(Lists
				.newArrayList(tableName));
		if (tableDescriptor.length == 0) {
			throw new TableNotFoundException(tableName);
		}else {
			HTableDescriptor des = tableDescriptor[0];
			if (des == null) {
				throw new IOException("Unable to obtain HTableDescriptor for "
						+ tableName);
			} else {
				byte[] tableBytes = Bytes.toBytes(tableName);
				disableTable(hMaster, tableBytes);
				byte[] gbyte = Bytes.toBytes(targetGroup);
				des.setValue(GroupInfo.GROUP_KEY, gbyte);
				hMaster.modifyTable(des.getName(), des);
				List<HRegionInfo> tableRegionList = master.getAssignmentManager().getRegionsOfTable(
													tableBytes);
				this.master.getAssignmentManager().unassign(tableRegionList);
				enableTable(hMaster, tableBytes);
			}
			return true;
		}
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
	 * @param server the server
	 * @param sourceGroup the source group
	 * @param targetGroup the target group
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException the interrupted exception
	 */
	public synchronized void moveServer(ServerName server,String sourceGroup, String targetGroup)
			throws IOException, InterruptedException {
		if ((server == null) || (StringUtils.isEmpty(targetGroup))) {
			throw new IOException(
					"The region server or the target to move found to be null.");
		}
		try {
			removeServer(server);
			addServer(server, targetGroup);
			LOG.info("Successfully move " + server.getHostAndPort()
					+ " to group " + targetGroup);
			writeConfig();
		} catch (Exception exp) {
			LOG.info(
					"Exception occured while move of the server "
							+ server.getHostAndPort(), exp);
			addServer(server,sourceGroup);
		}
	}

	private  void enableTable(HMaster master, final byte [] tableName)
	  throws IOException {
		master.enableTable(tableName);
	    // Wait until all regions are enabled
	    boolean enabled = false;
	    for (int tries = 0; tries < (numRetries * retryLongerMultiplier); tries++) {
	      enabled = testTableOnlineState(tableName, true);
	      if (enabled) {
	        break;
	      }
	      long sleep = getPauseTime(tries);
	      if (LOG.isDebugEnabled()) {
	        LOG.debug("Sleeping= " + sleep + "ms, waiting for all regions to be " +
	          "enabled in " + Bytes.toString(tableName));
	      }
	      try {
	        Thread.sleep(sleep);
	      } catch (InterruptedException e) {
	        Thread.currentThread().interrupt();
	        // Do this conversion rather than let it out because do not want to
	        // change the method signature.
	        throw new IOException("Interrupted", e);
	      }
	    }
	    if (!enabled) {
	      throw new IOException("Unable to enable table " +
	        Bytes.toString(tableName));
	    }
	    LOG.info("Enabled table " + Bytes.toString(tableName));
	  }

	private void disableTable(HMaster master, final byte [] tableName)
	  throws IOException {
	    master.disableTable(tableName);
	    // Wait until table is disabled
	    boolean disabled = false;
	    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
	      disabled = testTableOnlineState(tableName, false);
	      if (disabled) {
	        break;
	      }
	      long sleep = getPauseTime(tries);
	      if (LOG.isDebugEnabled()) {
	        LOG.debug("Sleeping= " + sleep + "ms, waiting for all regions to be " +
	          "disabled in " + Bytes.toString(tableName));
	      }
	      try {
	        Thread.sleep(sleep);
	      } catch (InterruptedException e) {
	        // Do this conversion rather than let it out because do not want to
	        // change the method signature.
	        Thread.currentThread().interrupt();
	        throw new IOException("Interrupted", e);
	      }
	    }
	    if (!disabled) {
	      throw new RegionException("Retries exhausted, it took too long to wait"+
	        " for the table " + Bytes.toString(tableName) + " to be disabled.");
	    }
	    LOG.info("Disabled " + Bytes.toString(tableName));
	  }

	private long getPauseTime(int tries) {
	    int triesCount = tries;
	    if (triesCount >= HConstants.RETRY_BACKOFF.length) {
	      triesCount = HConstants.RETRY_BACKOFF.length - 1;
	    }
	    return pause * HConstants.RETRY_BACKOFF[triesCount];
	  }

	 /*
     * @param True if table is online
     */
    private boolean testTableOnlineState(byte [] tableName, boolean online)
    throws IOException {
      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        // The root region is always enabled
        return online;
      }
      ZooKeeperWatcher zkw = master.getZooKeeper();
      String tableNameStr = Bytes.toString(tableName);
      try {
        if (online) {
          return ZKTableReadOnly.isEnabledTable(zkw, tableNameStr);
        }
        return ZKTableReadOnly.isDisabledTable(zkw, tableNameStr);
      } catch (KeeperException e) {
        throw new IOException("Enable/Disable failed", e);
      }
    }

    private List<ServerName> getAssignedServers(){
    	List<ServerName> assignedServers = Lists.newArrayList();
    	for(GroupInfo gInfo : groupMap.values()){
    		assignedServers.addAll(gInfo.getServers());
    	}
    	return assignedServers;
    }

/*	*//**
	 * Stores the plan for the move of an individual server.
	 *
	 * Contains {@link ServerName} for the server being moved, {@link GroupInfo}
	 * for the group the server should be moved from, the server should be moved
	 * to.
	 *
	 * The comparable implementation of this class compares only the ServerName
	 * information and not the source/dest group info.
	 *//*
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

		*//**
		 * Get the ServerName
		 *
		 * @return
		 *//*
		public ServerName getServername() {
			return servername;
		}

		*//**
		 * Set the serverName
		 *
		 * @param servername
		 *//*
		public void setServerName(ServerName servername) {
			this.servername = servername;
		}

		*//**
		 * Get the source group.
		 *
		 * @return
		 *//*
		public String getSourceGroup() {
			return sourceGroup;
		}

		*//**
		 * Set the source group.
		 *
		 * @param sourceGroup
		 *//*
		public void setSourceGroup(String sourceGroup) {
			this.sourceGroup = sourceGroup;
		}

		*//**
		 * Get the target group.
		 *
		 * @return
		 *//*
		public String getTargetGroup() {
			return targetGroup;
		}

		*//**
		 * Set the target group.
		 *
		 * @param targetGroup
		 *//*
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
	}*/
}
