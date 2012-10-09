package org.apache.hadoop.hbase.security.access;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.GroupInfo;
import org.apache.hadoop.hbase.master.GroupInfoManagerEndpoint;

import java.io.IOException;
import java.util.List;

public class SecureGroupInfoManagerEndpoint extends GroupInfoManagerEndpoint {
  private RegionCoprocessorEnvironment env;

  @Override
  public void start(CoprocessorEnvironment env) {
    super.start(env);
    this.env = (RegionCoprocessorEnvironment)env;
  }

  @Override
  public void setGroupPropertyOfTable(String groupName, HTableDescriptor desc) throws IOException {
    super.setGroupPropertyOfTable(groupName, desc);
  }

  @Override
  public String getGroupPropertyOfTable(HTableDescriptor desc) throws IOException {
    return super.getGroupPropertyOfTable(desc);
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    return super.listGroups();
  }

  @Override
  public GroupInfo getGroup(String groupName) throws IOException {
    return super.getGroup(groupName);
  }

  @Override
  public GroupInfo getGroupOfServer(String hostPort) throws IOException {
    return super.getGroupOfServer(hostPort);
  }

  @Override
  public boolean moveServer(String hostPort, String srcGroup, String dstGroup) throws IOException {
    getAccessController().requirePermission(HConstants.ROOT_TABLE_NAME, null, null, Permission.Action.WRITE);
    return super.moveServer(hostPort, srcGroup, dstGroup);
  }

  @Override
  public void removeGroup(String groupName) throws IOException {
    getAccessController().requirePermission(HConstants.ROOT_TABLE_NAME, null, null, Permission.Action.WRITE);
    super.removeGroup(groupName);
  }

  @Override
  public void addGroup(GroupInfo groupInfo) throws IOException {
    getAccessController().requirePermission(HConstants.ROOT_TABLE_NAME, null, null, Permission.Action.WRITE);
    super.addGroup(groupInfo);
  }

  private AccessController getAccessController() {
    return (AccessController) this.env.getRegion()
        .getCoprocessorHost().findCoprocessor(AccessController.class.getName());
  }
}
