package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;

import java.io.IOException;
import java.util.List;

public class GroupMasterObserver extends BaseMasterObserver {

  private GroupInfoManagerImpl groupManager;

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
    groupManager = new GroupInfoManagerImpl(ctx.getConfiguration(),
        ((MasterCoprocessorEnvironment)ctx).getMasterServices());
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    String groupName = GroupInfo.getGroupString(desc);
    if(groupManager.getGroup(groupName) == null) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist.");
    }
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName, HTableDescriptor htd) throws IOException {
    MasterServices master = ctx.getEnvironment().getMasterServices();
    String groupName = GroupInfo.getGroupString(htd);
    if(groupManager.getGroup(groupName) == null) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist.");
    }

    List<HRegionInfo> tableRegionList = master.getAssignmentManager().getRegionsOfTable(tableName);
    master.getAssignmentManager().unassign(tableRegionList);
  }

}
