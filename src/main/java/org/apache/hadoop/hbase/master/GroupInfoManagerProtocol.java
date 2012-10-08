package org.apache.hadoop.hbase.master;

import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.master.GroupInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface GroupInfoManagerProtocol extends CoprocessorProtocol, GroupInfoManager {

}
