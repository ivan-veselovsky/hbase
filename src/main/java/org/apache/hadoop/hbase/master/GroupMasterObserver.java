package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterAddressTracker;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class GroupMasterObserver extends BaseMasterObserver {

  private GroupInfoStore groupStore;

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
    groupStore = new GroupInfoStore(ctx.getConfiguration(),
        ((MasterCoprocessorEnvironment)ctx).getMasterServices());
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    String groupName = GroupInfo.getGroupString(desc);
    if(groupStore.getGroupInfo(groupName) == null) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist.");
    }
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName, HTableDescriptor htd) throws IOException {
    MasterServices master = ctx.getEnvironment().getMasterServices();
    String groupName = GroupInfo.getGroupString(htd);
    if(groupStore.getGroupInfo(groupName) == null) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist.");
    }

    List<HRegionInfo> tableRegionList = master.getAssignmentManager().getRegionsOfTable(tableName);
    master.getAssignmentManager().unassign(tableRegionList);
  }

}
