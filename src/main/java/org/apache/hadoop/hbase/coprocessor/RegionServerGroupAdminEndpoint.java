package org.apache.hadoop.hbase.coprocessor;

import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.GroupInfo;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.util.ReflectionUtils;
import sun.reflect.Reflection;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class RegionServerGroupAdminEndpoint extends BaseEndpointCoprocessor
    implements RegionServerGroupAdminProtocol {

  final String GROUP_INFO_MANAGER_CLASS = "hbase.groups.groupInfoManagerClass";

  @Override
  public void addGroup(String groupName) throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void deleteGroup(String groupName) throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean moveTableToGroup(String tableName, String targetGroup) throws IOException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void moveServer(ServerName server, String targetGroup) throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public GroupInfo getGroupInfoOfServer(String host) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<String> listTablesOfGroup(String groupName) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Collection<GroupInfo> listGroups() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Map<String, Pair<String, String>> getRegionsServerInTransition() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
