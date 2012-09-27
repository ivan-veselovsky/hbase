/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

/**
 * Stores the group information of region server groups.
 */
public class GroupInfo{

	private HashSet<ServerName> serverNames;
	public static final String DEFAULT_GROUP = "default";
	public static final byte[] GROUP_KEY = Bytes.toBytes("rs_group");
	private String name;

	public GroupInfo(String name) {
		this();
		this.name = name;
	}

	public GroupInfo() {
		this.serverNames = new HashSet<ServerName>();
	}

	/**
	 * Get group name.
	 *
	 * @return
	 */
	public String getName() {
		return name;
	}

	/**
	 * Checks based of equivalence of host name and port.
	 *
	 * @param server
	 * @return true if the server is in this group
	 */
	public boolean contains(ServerName server) {
		ServerName actual = ServerName.findServerWithSameHostnamePort(
				this.serverNames, server);
		return actual == null ? false : true;
	}

	/**
	 * Adds the server to the group.
	 *
	 * @param server the server
	 */
	public void add(ServerName server){
		this.serverNames.add(server);
	}

	/**
	 * Adds a group of servers.
	 *
	 * @param servers the servers
	 */
	public void addAll(List<ServerName> servers){
		this.serverNames.addAll(servers);
	}

	/**
	 * Checks based of equivalence of host name and port.
	 *
	 * @param serverList The list to check for containment.
	 * @return true, if successful
	 */
	public boolean contains(List<ServerName> serverList) {
		if (serverList.size() == 0) {
			return false;
		} else {
			boolean contains = true;
			for (ServerName server : serverList) {
				contains = contains && this.contains(server);
				if (!contains)
					return contains;
			}
			return contains;
		}
	}


	/**
	 * Get a copy of servers.
	 *
	 * @return
	 */
	public List<ServerName> getServers() {
		return Lists.newArrayList(this.serverNames);
	}

	/**
	 * Write the group out.
	 *
	 * @param out
	 * @throws IOException
	 */
	private void write(BufferedWriter out) throws IOException {
		StringBuffer sb = new StringBuffer();
		sb.append(getName());
		sb.append("\t");
		for (ServerName sName : serverNames) {
			if (sb.length() != (getName().length() + 1)) {
				sb.append(",");
			}
			sb.append(sName.getHostAndPort());
		}
		out.write(sb.toString());
		out.newLine();
	}

	private boolean readFields(String line) throws IOException {
		boolean isWellFormed = false;
		String[] groupSplit = line.split("\t");
		switch(groupSplit.length) {
		case 1: this.name = groupSplit[0].trim();
				isWellFormed = true;
				break;
		case 2: this.name = groupSplit[0].trim();
				String[] hostPortPairs = groupSplit[1].trim().split(",");
				for (String sName : hostPortPairs) {
					if (StringUtils.isNotEmpty(sName)) {
						ServerName name = new ServerName(sName.trim(), -1);
						this.serverNames.add(name);
					}
				}
				isWellFormed = true;
				break;
		}

		return isWellFormed;
	}

	/**
	 * Read a list of GroupInfo.
	 *
	 * @param in
	 *            DataInput
	 * @return
	 * @throws IOException
	 */
	public static List<GroupInfo> readGroups(final FSDataInputStream in)
			throws IOException {
		List<GroupInfo> groupList = new ArrayList<GroupInfo>();
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		try {
			while ((line = br.readLine()) != null) {
				GroupInfo group = new GroupInfo();
				if (group.readFields(line)) {
					if (group.getName().equalsIgnoreCase(DEFAULT_GROUP) == false) {
						groupList.add(group);
					}
				}
			}
		} finally {
			br.close();
		}
		return groupList;
	}

	/**
	 * Write a list of group information out.
	 *
	 * @param groups
	 * @param out
	 * @throws IOException
	 */
	public static void writeGroups(Collection<GroupInfo> groups, FSDataOutputStream out)
			throws IOException {
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		try {
			for (GroupInfo group : groups) {
				group.write(bw);
			}
		} finally {
			bw.close();
		}
	}

	/**
	 * Remove a server from this group.
	 *
	 * @param server
	 */
	public void remove(ServerName server) {
		ServerName actual = ServerName.findServerWithSameHostnamePort(
				this.serverNames, server);
		if (actual != null) {
			this.serverNames.remove(actual);
		}
	}

	/**
	 * Get group attribute from a table descriptor.
	 *
	 * @param HTableDescriptor
	 * @return The group name of the table.
	 */
	public static String getGroupString(HTableDescriptor des) {
		byte[] gbyte = des.getValue(GROUP_KEY);
		if (gbyte != null) {
			return Bytes.toString(des.getValue(GROUP_KEY));
		} else
			return GroupInfo.DEFAULT_GROUP;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{GroupName:");
		sb.append(this.name);
		sb.append("-");
		sb.append(" Severs:");
		sb.append(this.serverNames + "}");
		return sb.toString();

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result
				+ ((serverNames == null) ? 0 : serverNames.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof GroupInfo))
			return false;
		GroupInfo other = (GroupInfo) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (serverNames == null) {
			if (other.serverNames != null)
				return false;
		} else if (serverNames.size() != other.getServers().size()){
			return false;
		}else if(!contains(other.getServers())){
			return false;
		}

		return true;
	}
}
