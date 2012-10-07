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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Sets;
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

	private NavigableSet<String> servers;
	public static final String DEFAULT_GROUP = "default";
  public static final String TRANSITION_GROUP_PREFIX = "_transition_";
	public static final byte[] GROUP_KEY = Bytes.toBytes("rs_group");
	private String name;

	public GroupInfo(String name) {
		this();
		this.name = name;
	}

	public GroupInfo() {
		this.servers = new TreeSet<String>();
	}

  public GroupInfo(GroupInfo src) {
    servers = Sets.newTreeSet(src.getServers());
    name = src.getName();
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
	 * Adds the server to the group.
	 *
	 * @param hostPort the server
	 */
	public void addServer(String hostPort){
		this.servers.add(hostPort);
	}

	/**
	 * Adds a group of servers.
	 *
	 * @param hostPort the servers
	 */
	public void addAll(Collection<String> hostPort){
		this.servers.addAll(hostPort);
	}

	public boolean containsServer(String hostPort) {
    return servers.contains(hostPort);
	}

	/**
	 * Checks based of equivalence of host name and port.
	 *
	 * @param serverList The list to check for containment.
	 * @return true, if successful
	 */
	public boolean containsServer(NavigableSet<String> serverList) {
		if (serverList.size() == 0) {
			return false;
		} else {
			boolean contains = true;
			for (String hostPort : serverList) {
				contains = contains && this.getServers().contains(hostPort);
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
	public NavigableSet<String> getServers() {
		return this.servers;
	}

	/**
	 * Write the group out.
	 *
	 * @param out
	 * @throws IOException
	 */
	public void write(BufferedWriter out) throws IOException {
		StringBuffer sb = new StringBuffer();
		sb.append(getName());
		sb.append("\t");
		for (String sName : servers) {
			if (sb.length() != (getName().length() + 1)) {
				sb.append(",");
			}
			sb.append(sName);
		}
		out.write(sb.toString());
		out.newLine();
	}

	public boolean readFields(String line) throws IOException {
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
						this.servers.add(sName);
					}
				}
				isWellFormed = true;
				break;
		}

		return isWellFormed;
	}

	/**
	 * Remove a server from this group.
	 *
	 * @param hostPort
	 */
	public boolean removeServer(String hostPort) {
    return this.servers.remove(hostPort);
	}

	/**
	 * Get group attribute from a table descriptor.
	 *
	 * @param des
	 * @return The group name of the table.
	 */
	public static String getGroupString(HTableDescriptor des) {
		byte[] gbyte = des.getValue(GROUP_KEY);
		if (gbyte != null) {
			return Bytes.toString(des.getValue(GROUP_KEY));
		} else {
			return GroupInfo.DEFAULT_GROUP;
    }
	}


	public static void setGroupString(String group, HTableDescriptor des) {
    if(group.equals(DEFAULT_GROUP)) {
      des.remove(group);
    }
    else {
		  des.setValue(GROUP_KEY, Bytes.toBytes(group));
    }
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{GroupName:");
		sb.append(this.name);
		sb.append("-");
		sb.append(" Severs:");
		sb.append(this.servers+ "}");
		return sb.toString();

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result
				+ ((servers == null) ? 0 : servers.hashCode());
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
		if (servers == null) {
			if (other.servers != null)
				return false;
		} else if (servers.size() != other.getServers().size()){
			return false;
		}else if(!containsServer(other.getServers())){
			return false;
		}

		return true;
	}
}
