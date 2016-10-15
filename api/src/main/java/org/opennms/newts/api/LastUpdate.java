/*
 * Copyright 2016, The OpenNMS Group
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opennms.newts.api;

import java.io.Serializable;

import com.google.common.base.Objects;

public class LastUpdate implements Element<ValueType<?>>, Serializable {
	private static final long serialVersionUID = -4125721947216294455L;
	
	private final Timestamp m_timestamp;
	private final Resource m_resource;
	
	public LastUpdate(Timestamp timestamp, Resource resource) {
		m_timestamp = timestamp;
		m_resource = resource;
	}
	
	@Override
	public Timestamp getTimestamp() {
		return m_timestamp;
	}

	@Override
	public Resource getResource() {
		return m_resource;
	}

	@Override
	public String getName() {
		return "name";
	}

	@Override
	public ValueType<?> getValue() {
		return null;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
	    if (getClass() != obj.getClass()) return false;
	    final LastUpdate other = (LastUpdate) obj;
	    return Objects.equal(this.m_timestamp, other.m_timestamp) && 
	    		Objects.equal(this.m_resource, other.m_resource);
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(this.m_timestamp, this.m_resource);
	}
}