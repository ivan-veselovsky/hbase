package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HTableDescriptor;

import java.io.IOException;
import java.util.List;

public interface GroupInfoManager {
  /**
   * Adds the group.
   *
   * @param groupInfo the group name
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  void addGroup(GroupInfo groupInfo) throws IOException;

  /**
   * Remove a region server group.
   *
   * @param groupName the group name
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  void removeGroup(String groupName) throws IOException;

  boolean moveServer(String hostPort, String srcGroup, String dstGroup) throws IOException;

  /**
   * Gets the group info of server.
   *
   * @param hostPort the server
   * @return An instance of GroupInfo.
   */
  GroupInfo getGroupOfServer(String hostPort) throws IOException;

  /**
   * Gets the group information.
   *
   * @param groupName the group name
   * @return An instance of GroupInfo
   */
  GroupInfo getGroup(String groupName) throws IOException;

  List<GroupInfo> listGroups() throws IOException;

  String getGroupPropertyOfTable(HTableDescriptor desc) throws IOException;

  void setGroupPropertyOfTable(String groupName, HTableDescriptor desc) throws IOException;
}
