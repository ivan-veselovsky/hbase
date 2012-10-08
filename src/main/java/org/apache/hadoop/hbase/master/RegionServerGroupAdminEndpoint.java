package org.apache.hadoop.hbase.master;

import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RegionServerGroupAdminEndpoint extends BaseEndpointCoprocessor
  implements GroupInfoManagerProtocol {

  private GroupInfoManager groupInfoManager;

  @Override
  public void start(CoprocessorEnvironment env) {
    try {
      groupInfoManager = new GroupInfoManagerImpl(env.getConfiguration(), null);
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
