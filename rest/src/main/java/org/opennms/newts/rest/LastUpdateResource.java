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
package org.opennms.newts.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.opennms.newts.api.LastUpdateRepository;
import org.opennms.newts.api.Resource;

@Path("/last-update")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class LastUpdateResource {
	private final LastUpdateRepository m_lastUpdateReposotiry;
	
	public LastUpdateResource(LastUpdateRepository lastUpdateRepository) {
		m_lastUpdateReposotiry = lastUpdateRepository;
	}
	
	@GET
	@Path("/{resource}")
	public Collection<LastUpdateDTO> getLastUpdate(@PathParam("resource") Resource resource) {
		Collection<LastUpdateDTO> result;
		long lastUpdateTimestamp = m_lastUpdateReposotiry.selectLastUpdate(resource).m_time;
		if (lastUpdateTimestamp == 0) {
			result = new ArrayList<LastUpdateDTO>(0);
		} else {
			result = Arrays.asList(new LastUpdateDTO(lastUpdateTimestamp, resource.getId()));
		}
		return result;
	}
	
    @POST
    public Response writeSamples(Collection<LastUpdateDTO> lastUpdates) {
    	m_lastUpdateReposotiry.insert(Transform.lastUpdates(lastUpdates));
        return Response.status(Response.Status.CREATED).build();
    }
}