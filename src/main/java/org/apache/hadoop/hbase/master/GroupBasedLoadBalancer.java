package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.jruby.compiler.ir.operands.Array;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.TreeMultiset;

public class GroupBasedLoadBalancer implements LoadBalancer {

	private static final Log LOG = LogFactory
			.getLog(GroupBasedLoadBalancer.class);
	private Configuration config;
	private ClusterStatus status;
	private MasterServices services;
	private GroupInfoManager groupManager;
	private DefaultLoadBalancer internalBalancer = new DefaultLoadBalancer();

	@Override
	public Configuration getConf() {
		return config;
	}

	@Override
	public void setConf(Configuration conf) {
		this.config = conf;
		internalBalancer.setConf(conf);
	}

	@Override
	public void setClusterStatus(ClusterStatus st) {
		this.status = st;
		internalBalancer.setClusterStatus(st);
	}

	@Override
	public void setMasterServices(MasterServices masterServices) {
		this.services = masterServices;
		internalBalancer.setMasterServices(masterServices);
		try {
			this.groupManager = new GroupInfoManager(this.services);
		} catch (IOException e) {
			LOG.warn("IOException while creating GroupInfoManager.", e);
		}
	}

	@Override
	public List<RegionPlan> balanceCluster(
			Map<ServerName, List<HRegionInfo>> clusterState) {
		/*List<RegionPlan> regionPlans = new ArrayList<RegionPlan>();
		for (GroupInfo info : groupManager.getExistingGroups()) {
			Map<ServerName, List<HRegionInfo>> groupClusterState = new HashMap<ServerName, List<HRegionInfo>>();
			for (ServerName sName : info.getServers()) {
				ServerName actual = getServerName(clusterState.keySet(), sName);
				if (actual!= null) {
					groupClusterState.put(actual, clusterState.get(actual));
				}
			}
			List<RegionPlan> groupPlans = this.internalBalancer
					.balanceCluster(groupClusterState);
			if (groupPlans != null) {
				regionPlans.addAll(groupPlans);
			}
		}*/
		//return regionPlans;
		return this.internalBalancer.balanceCluster(clusterState);
	}

	@Override
	public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
			List<HRegionInfo> regions, List<ServerName> servers) {
		Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
		ArrayListMultimap<String, HRegionInfo> regionGroup = groupRegions(regions);
		for (String groupKey : regionGroup.keys()) {
			GroupInfo info = groupManager.getGroupInformation(groupKey);
			assignments.putAll(this.internalBalancer.roundRobinAssignment(
					regionGroup.get(groupKey),getServerToAssign(info, servers)));
		}
		return assignments;
	}

	@Override
	public Map<ServerName, List<HRegionInfo>> retainAssignment(
			Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
		Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
		ArrayListMultimap<String, HRegionInfo> rGroup = ArrayListMultimap
				.create();
		List<HRegionInfo> misplacedRegions = getMisplacedRegions(regions);
		for (HRegionInfo region : regions.keySet()) {
			if (misplacedRegions.contains(region)) {
				regions.remove(region);
			} else {
				GroupInfo info = groupManager.getGroupInformation(region
						.getTableNameAsString());
				rGroup.put(info.getName(), region);
			}
		}
		// Now the regions map has only the regions which have correct
		// assignments.
		// The region for whom assignments are to be retained are in rGroup.
		for (String key : rGroup.keys()) {
			Map<HRegionInfo, ServerName> currentAssignmentMap = new TreeMap<HRegionInfo, ServerName>();
			List<HRegionInfo> regionList = rGroup.get(key);
			GroupInfo info = groupManager.getGroupInformation(key);
			List<ServerName> candidateList = getServerToAssign(info, servers);
			for (HRegionInfo region : regionList) {
				currentAssignmentMap.put(region, regions.get(region));
			}
			assignments.putAll(this.internalBalancer.retainAssignment(
					currentAssignmentMap, candidateList));
		}

		for (HRegionInfo region : misplacedRegions) {
			GroupInfo info = groupManager.getGroupInformation(region
					.getTableNameAsString());
			List<ServerName> candidateList = getServerToAssign(info, servers);
			ServerName server = this.internalBalancer.randomAssignment(region,
					candidateList, null);
			assignments.get(server).add(region);
		}
		return assignments;
	}

	@Override
	public Map<HRegionInfo, ServerName> immediateAssignment(
			List<HRegionInfo> regions, List<ServerName> servers) {
		Map<HRegionInfo, ServerName> assignments = new TreeMap<HRegionInfo, ServerName>();
		// Need to group regions by the group and servers and then call the
		// internal load balancer.
		ArrayListMultimap<String, HRegionInfo> regionGroups = groupRegions(regions);
		for (String key : regionGroups.keys()) {
			List<HRegionInfo> regionsOfSameGroup = regionGroups.get(key);
			GroupInfo info = groupManager.getGroupInformation(key);
			List<ServerName> candidateList = getServerToAssign(info, servers);
			assignments.putAll(this.internalBalancer.immediateAssignment(
					regionsOfSameGroup, candidateList));
		}
		return assignments;
	}

	@Override
	public ServerName randomAssignment(HRegionInfo region,
			List<ServerName> servers, ServerName prefferedServer) {
		String tableName = region.getTableNameAsString();
		List<ServerName> candidateList;
		GroupInfo groupInfo = groupManager.getGroupInfoOfTable(tableName);
		candidateList = getServerToAssign(groupInfo, servers);

		if ((prefferedServer == null) || (groupInfo.contains(prefferedServer) ==false)) {
			return this.internalBalancer.randomAssignment(region,candidateList, null);
		} else {
			return prefferedServer;
		}
	}

	private List<ServerName> getServerToAssign(GroupInfo groupInfo,
			List<ServerName> onlineServers) {
		List<ServerName> candidateList = new ArrayList<ServerName>();
		if (groupInfo.getServers().size() == 0) {
			LOG.warn("Wanted to do random assignment with "
					+ groupInfo.getName() + "but going with default group.");
			GroupInfo defaultInfo = groupManager
					.getGroupInformation(GroupInfo.DEFAULT_GROUP);
			if (defaultInfo.getServers().size() != 0) {
				candidateList.addAll(defaultInfo.getServers());
			} else {
				LOG.warn("The default group is empty.");
				candidateList.addAll(onlineServers);
			}
		}else {
			candidateList.addAll(groupInfo.getServers());
		}
		return groupManager.filterServers(candidateList, onlineServers);
	}

	private ArrayListMultimap<String, HRegionInfo> groupRegions(
			List<HRegionInfo> regionList) {
		ArrayListMultimap<String, HRegionInfo> regionGroup = ArrayListMultimap
				.create();
		for (HRegionInfo region : regionList) {
			regionGroup.put(
					groupManager.getGroupInfoOfTable(
							region.getTableNameAsString()).getName(), region);
		}
		return regionGroup;
	}

	private List<HRegionInfo> getMisplacedRegions(
			Map<HRegionInfo, ServerName> regions) {
		List<HRegionInfo> misplacedRegions = new ArrayList<HRegionInfo>();
		for (HRegionInfo region : regions.keySet()) {
			ServerName assignedServer = regions.get(region);
			GroupInfo info = groupManager.getGroupInfoOfTable(region
					.getTableNameAsString());
			if(!info.contains(assignedServer)) {
				misplacedRegions.add(region);
			}
		}
		return misplacedRegions;
	}

	@Override
	public void refresh() {
		this.groupManager.refresh();
	}

	static ServerName getServerName(Collection<ServerName> serverNameSet,
			ServerName sn) {
		ServerName actual =
		      ServerName.findServerWithSameHostnamePort(serverNameSet, sn);
		    return actual;
	}

}
