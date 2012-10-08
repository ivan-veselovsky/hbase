package org.apache.hadoop.hbase.master;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class GroupInfoManagerImpl implements GroupInfoManager {
	private static final Log LOG = LogFactory.getLog(GroupInfoManagerImpl.class);

  public static final String GROUP_INFO_FILE_NAME = ".rsgroupinfo";

	//Access to this map should always be synchronized.
	private Map<String, GroupInfo> groupMap;
  private long fileModTime = 0;
  private Path path;
  private FileSystem fs;

  private MasterServices master;
  private ZooKeeperWatcher zkw;

  //TODO add zookeeper synchronization
  public GroupInfoManagerImpl(Configuration conf, MasterServices master) throws IOException {
		groupMap = new ConcurrentHashMap<String, GroupInfo>();
		this.path = new Path(FSUtils.getRootDir(conf), GROUP_INFO_FILE_NAME);
		this.fs = FSUtils.getRootDir(conf).getFileSystem(conf);
    this.master = master;
    if(master == null) {
      zkw = new ZooKeeperWatcher(conf, "group_info_store", null);
    }
    if(!fs.exists(path)) {
      fs.createNewFile(path);
    }
    this.reloadConfig();
  }

	/**
	 * Adds the group.
	 *
	 * @param groupInfo the group name
	 * @throws java.io.IOException Signals that an I/O exception has occurred.
	 */
  @Override
  public synchronized void addGroup(GroupInfo groupInfo) throws IOException {
    reloadConfig();
		if (groupMap.get(groupInfo.getName()) != null ||
        groupInfo.getName().equals(GroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group already exists: "+groupInfo.getName());
    }
    groupMap.put(groupInfo.getName(), groupInfo);
    try {
      flushConfig();
    } catch (IOException e) {
      groupMap.remove(groupInfo.getName());
      throw e;
    }
	}

  @Override
  public synchronized boolean moveServer(String hostPort, String srcGroup, String dstGroup) throws IOException {
    reloadConfig();
    GroupInfo src = new GroupInfo(getGroup(srcGroup));
    GroupInfo dst = new GroupInfo(getGroup(dstGroup));

    if(src.removeServer(hostPort)) {
      return false;
    }
    dst.addServer(hostPort);
    Map<String,GroupInfo> newMap = Maps.newHashMap(groupMap);
    if(!src.getName().equals(GroupInfo.DEFAULT_GROUP))
      newMap.put(src.getName(), src);
    if(!dst.getName().equals(GroupInfo.DEFAULT_GROUP))
      newMap.put(dst.getName(), dst);
    flushConfig(newMap);
    groupMap = newMap;
    return true;
  }

	/**
	 * Gets the group info of server.
	 *
	 * @param hostPort the server
	 * @return An instance of GroupInfo.
	 */
  @Override
  public synchronized GroupInfo getGroupOfServer(String hostPort) throws IOException {
    reloadConfig();
		for(GroupInfo info : groupMap.values()){
			if(info.containsServer(hostPort)){
				return info;
			}
		}
		return getGroup(GroupInfo.DEFAULT_GROUP);
	}

	/**
	 * Gets the group information.
	 *
	 * @param groupName the group name
	 * @return An instance of GroupInfo
	 */
  @Override
  public synchronized GroupInfo getGroup(String groupName) throws IOException {
    reloadConfig();
		if (groupName.equalsIgnoreCase(GroupInfo.DEFAULT_GROUP)) {
			GroupInfo defaultInfo = new GroupInfo(GroupInfo.DEFAULT_GROUP, new TreeSet<String>());
      List<ServerName> unassignedServers =
          difference(getOnlineRS(),getAssignedServers());
      for(ServerName serverName: unassignedServers) {
        defaultInfo.addServer(serverName.getHostAndPort());
      }
			return defaultInfo;
		} else {
			return this.groupMap.get(groupName);
		}
	}



	/**
	 * Delete a region server group.
	 *
	 * @param groupName the group name
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
  @Override
  public synchronized void removeGroup(String groupName) throws IOException {
    reloadConfig();
    GroupInfo group = null;
    if(!groupMap.containsKey(groupName) || groupName.equals(GroupInfo.DEFAULT_GROUP)) {
      throw new IllegalArgumentException("Group does not exit or is default group");
    }
    synchronized (groupMap) {
      try {
        group = groupMap.remove(groupName);
        flushConfig();
      } catch (IOException e) {
        groupMap.put(groupName, group);
        throw e;
      }
    }
	}

  @Override
  public synchronized List<GroupInfo> listGroups() throws IOException {
    reloadConfig();
    List<GroupInfo> list = Lists.newLinkedList(groupMap.values());
    list.add(getGroup(GroupInfo.DEFAULT_GROUP));
    return list;
  }

	synchronized void reloadConfig() throws IOException {
    reloadConfig(false);
  }

	/**
	 * Read group configuration from HDFS.
	 *
	 * @throws IOException
	 */
	synchronized void reloadConfig(boolean force) throws IOException {
		List<GroupInfo> groupList;
		FSDataInputStream in = null;
    FileStatus status = fs.getFileStatus(path);
    if(fileModTime == status.getModificationTime() && !force) {
        return;
    }
    in = fs.open(path);
    try {
      synchronized (groupMap) {
        this.groupMap.clear();
        groupList = readGroups(in);
        for (GroupInfo group : groupList) {
          groupMap.put(group.getName(), group);
        }
      }
    } finally {
      in.close();
    }
    fileModTime = status.getModificationTime();
	}

	/**
	 * Write the configuration to HDFS.
	 *
	 * @throws IOException
	 */
	private synchronized void flushConfig() throws IOException {
    flushConfig(groupMap);
	}

	private synchronized void flushConfig(Map<String,GroupInfo> map) throws IOException {
		FSDataOutputStream output = null;
		try {
			output = fs.create(path, true);
			List<GroupInfo> groups = Lists.newArrayList(map.values());
			writeGroups(groups, output);
		} finally {
			output.close();
		}
	}

	/**
	 * Read a list of GroupInfo.
	 *
	 * @param in
	 *            DataInput
	 * @return
	 * @throws IOException
	 */
	private static List<GroupInfo> readGroups(final FSDataInputStream in)
			throws IOException {
		List<GroupInfo> groupList = new ArrayList<GroupInfo>();
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		try {
			while ((line = br.readLine()) != null && (line = line.trim()).length() > 0) {
				GroupInfo group = new GroupInfo();
				if (group.readFields(line)) {
					if (group.getName().equalsIgnoreCase(GroupInfo.DEFAULT_GROUP))
            throw new IOException("Config file contains default group!");
          groupList.add(group);
				}
			}
		} finally {
			br.close();
		}
		return groupList;
	}

	/**
	 * Write a list of group information out.
	 *
	 * @param groups
	 * @param out
	 * @throws IOException
	 */
	private static void writeGroups(Collection<GroupInfo> groups, FSDataOutputStream out)
			throws IOException {
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		try {
			for (GroupInfo group : groups) {
        if (group.getName().equalsIgnoreCase(GroupInfo.DEFAULT_GROUP))
          throw new IOException("Config file contains default group!");
				group.write(bw);
			}
		} finally {
			bw.close();
		}
	}

  private List<ServerName> getOnlineRS() throws IOException{
    if(master != null) {
      return master.getServerManager().getOnlineServersList();
    }
    try {
      List<ServerName> servers = new LinkedList<ServerName>();
      for (String el: ZKUtil.listChildrenNoWatch(zkw, zkw.rsZNode)) {
        servers.add(ServerName.parseServerName(el));
      }
      return servers;
    } catch (KeeperException e) {
      throw new IOException("Failed to retrieve server list for zookeeper", e);
    }
  }

  private List<ServerName> getAssignedServers(){
    List<ServerName> assignedServers = Lists.newArrayList();
    for(GroupInfo gInfo : groupMap.values()){
      for(String hostPort: gInfo.getServers()) {
        assignedServers.add(ServerName.parseServerName(hostPort));
      }
    }
    return assignedServers;
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
	 * Gets the group info of table.
	 *
	 * @param desc the table name
	 * @return An instance of GroupInfo.
	 */
  public String getGroupPropertyOfTable(HTableDescriptor desc) throws IOException {
		return GroupInfo.getGroupString(desc);
	}

  public void setGroupPropertyOfTable(String groupName, HTableDescriptor desc) throws IOException {
		GroupInfo.setGroupString(groupName, desc);
  }
}
