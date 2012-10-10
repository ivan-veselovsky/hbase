package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;


import java.io.IOException;
import java.util.List;

public class GroupInfoManagerProxy implements GroupInfoManager{
  private GroupInfoManager proxy;

  public GroupInfoManagerProxy(Configuration conf) throws IOException {
    proxy = new HBaseAdmin(conf).coprocessorProxy(GroupInfoManagerProtocol.class);
//    proxy = new HTable(conf, HConstants.ROOT_TABLE_NAME)
//        .coprocessorProxy(GroupInfoManagerProtocol.class, new byte[0]);
  }

  @Override
  public void addGroup(GroupInfo groupInfo) throws IOException {
    if(groupInfo.getServers() == null)
      throw new IllegalStateException("grah!");
    proxy.addGroup(groupInfo);
  }

  @Override
  public void removeGroup(String groupName) throws IOException {
    proxy.removeGroup(groupName);
  }

  @Override
  public boolean moveServer(String hostPort, String srcGroup, String dstGroup) throws IOException {
    return proxy.moveServer(hostPort, srcGroup, dstGroup);
  }

  @Override
  public GroupInfo getGroupOfServer(String hostPort) throws IOException {
    return proxy.getGroupOfServer(hostPort);
  }

  @Override
  public GroupInfo getGroup(String groupName) throws IOException {
    return proxy.getGroup(groupName);
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    return proxy.listGroups();
  }

  @Override
  public String getGroupPropertyOfTable(HTableDescriptor desc) throws IOException {
    return proxy.getGroupPropertyOfTable(desc);
  }

  @Override
  public void setGroupPropertyOfTable(String groupName, HTableDescriptor desc) throws IOException {
    proxy.setGroupPropertyOfTable(groupName, desc);
  }
}
