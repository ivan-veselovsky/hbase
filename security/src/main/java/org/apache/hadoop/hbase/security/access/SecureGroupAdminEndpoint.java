package org.apache.hadoop.hbase.security.access;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.GroupAdminEndpoint;
import org.apache.hadoop.hbase.master.GroupInfo;
import org.apache.hadoop.hbase.master.HMaster;

import java.io.IOException;
import java.util.List;

public class SecureGroupAdminEndpoint extends GroupAdminEndpoint {
  private MasterCoprocessorEnvironment menv;

  @Override
  public void start(CoprocessorEnvironment env) {
    super.start(env);
    menv = (MasterCoprocessorEnvironment)env;
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    return super.listGroups();
  }

  @Override
  public GroupInfo getGroupInfo(String groupName) throws IOException {
    return super.getGroupInfo(groupName);
  }

  @Override
  public GroupInfo getGroupOfServer(String hostPort) throws IOException {
    return super.getGroupOfServer(hostPort);
  }

  @Override
  public void moveServer(String hostPort, String dstGroup) throws IOException {
    getAccessController().requirePermission(HConstants.ROOT_TABLE_NAME, null, null, Permission.Action.ADMIN);
    super.moveServer(hostPort, dstGroup);
  }

  @Override
  public void removeGroup(String groupName) throws IOException {
    getAccessController().requirePermission(HConstants.ROOT_TABLE_NAME, null, null, Permission.Action.ADMIN);
    super.removeGroup(groupName);
  }

  @Override
  public void addGroup(GroupInfo groupInfo) throws IOException {
    getAccessController().requirePermission(HConstants.ROOT_TABLE_NAME, null, null, Permission.Action.ADMIN);
    super.addGroup(groupInfo);
  }

  private AccessController getAccessController() {
    return (AccessController)menv.getMasterServices()
        .getCoprocessorHost().findCoprocessor(AccessController.class.getName());
  }
}
