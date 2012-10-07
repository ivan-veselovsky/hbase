package org.apache.hadoop.hbase.coprocessor;

import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.master.GroupInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface RegionServerGroupAdminProtocol extends CoprocessorProtocol{

  void addGroup(String groupName) throws IOException;

  void deleteGroup(String groupName) throws IOException;

  boolean moveTableToGroup(String tableName,
                           String targetGroup) throws IOException;

  void moveServer(ServerName server, String targetGroup) throws IOException;

  GroupInfo getGroupInfoOfServer(String host);

  Collection<String> listTablesOfGroup(String groupName);

  Collection<GroupInfo> listGroups();

  Map<String, Pair<String,String>> getRegionsServerInTransition();


}
