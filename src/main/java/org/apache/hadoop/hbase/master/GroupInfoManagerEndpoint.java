package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class GroupInfoManagerEndpoint extends BaseEndpointCoprocessor
  implements GroupInfoManagerProtocol {

  private static GroupInfoManager groupInfoManager;

  @Override
  public void start(CoprocessorEnvironment env) {
    try {
      RegionCoprocessorEnvironment renv = (RegionCoprocessorEnvironment)env;
      if(Bytes.equals(HConstants.ROOT_TABLE_NAME,renv.getRegion().getTableDesc().getName())) {
        groupInfoManager = new GroupInfoManagerImpl(env.getConfiguration(), null);
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void addGroup(GroupInfo groupInfo) throws IOException {
    if(groupInfo.getServers() == null)
      throw new IllegalStateException(groupInfo.getName());
    groupInfoManager.addGroup(groupInfo);
  }

  @Override
  public void removeGroup(String groupName) throws IOException {
    groupInfoManager.removeGroup(groupName);
  }

  @Override
  public boolean moveServer(String hostPort, String srcGroup, String dstGroup) throws IOException {
    return groupInfoManager.moveServer(hostPort, srcGroup, dstGroup);
  }

  @Override
  public GroupInfo getGroupOfServer(String hostPort) throws IOException {
    return groupInfoManager.getGroupOfServer(hostPort);
  }

  @Override
  public GroupInfo getGroup(String groupName) throws IOException {
    return groupInfoManager.getGroup(groupName);
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    return groupInfoManager.listGroups();
  }

  @Override
  public String getGroupPropertyOfTable(HTableDescriptor desc) throws IOException {
    return groupInfoManager.getGroupPropertyOfTable(desc);
  }

  @Override
  public void setGroupPropertyOfTable(String groupName, HTableDescriptor desc) throws IOException {
    groupInfoManager.setGroupPropertyOfTable(groupName, desc);
  }
}
