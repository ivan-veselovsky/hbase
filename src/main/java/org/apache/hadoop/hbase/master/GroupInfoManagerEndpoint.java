package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class GroupInfoManagerEndpoint extends BaseEndpointCoprocessor
  implements GroupInfoManagerProtocol {

  private MasterCoprocessorEnvironment menv;

  @Override
  public void start(CoprocessorEnvironment env) {
    menv = (MasterCoprocessorEnvironment)env;
  }

  @Override
  public void addGroup(GroupInfo groupInfo) throws IOException {
    if(groupInfo.getServers() == null)
      throw new IllegalStateException(groupInfo.getName());
    getGroupInfoManager().addGroup(groupInfo);
  }

  @Override
  public void removeGroup(String groupName) throws IOException {
    getGroupInfoManager().removeGroup(groupName);
  }

  @Override
  public boolean moveServer(String hostPort, String srcGroup, String dstGroup) throws IOException {
    return getGroupInfoManager().moveServer(hostPort, srcGroup, dstGroup);
  }

  @Override
  public GroupInfo getGroupOfServer(String hostPort) throws IOException {
    return getGroupInfoManager().getGroupOfServer(hostPort);
  }

  @Override
  public GroupInfo getGroup(String groupName) throws IOException {
    return getGroupInfoManager().getGroup(groupName);
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    return getGroupInfoManager().listGroups();
  }

  @Override
  public String getGroupPropertyOfTable(HTableDescriptor desc) throws IOException {
    return getGroupInfoManager().getGroupPropertyOfTable(desc);
  }

  @Override
  public void setGroupPropertyOfTable(String groupName, HTableDescriptor desc) throws IOException {
    getGroupInfoManager().setGroupPropertyOfTable(groupName, desc);
  }

  private GroupInfoManager getGroupInfoManager() {
    return ((GroupBasedLoadBalancer)menv.getMasterServices().getAssignmentManager().getBalancer()).getGroupInfoManager();
  }
}
