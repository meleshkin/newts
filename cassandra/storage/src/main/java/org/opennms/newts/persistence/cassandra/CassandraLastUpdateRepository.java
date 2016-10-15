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
package org.opennms.newts.persistence.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.datastax.driver.core.querybuilder.QueryBuilder.unloggedBatch;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.inject.Named;

import org.opennms.newts.api.LastUpdate;
import org.opennms.newts.api.LastUpdateRepository;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.cassandra.CassandraSession;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

public class CassandraLastUpdateRepository implements LastUpdateRepository{
	private final CassandraSession m_session;
	private final int m_ttl;
	private final PreparedStatement m_selectLastUpdateStatement;
	
	@Inject
    public CassandraLastUpdateRepository(CassandraSession session, @Named("samples.cassandra.time-to-live") int ttl) {
		m_session = checkNotNull(session, "session argument");
		checkArgument(ttl >= 0, "Negative Cassandra column TTL");
		m_ttl = ttl;
		
		Select selectLastUpdate = QueryBuilder.select().from(SchemaConstants.T_LAST_UPDATE);
	    selectLastUpdate.where(eq(SchemaConstants.F_RESOURCE, bindMarker(SchemaConstants.F_RESOURCE)));
	    m_selectLastUpdateStatement = m_session.prepare(selectLastUpdate.toString());     
    }


	@Override
	public Timestamp selectLastUpdate(Resource resource) {
    	LastUpdateDriverAdapter adapter = new LastUpdateDriverAdapter(cassandraSelectLastUpdate(resource));
    	Timestamp result = null;
    	if (adapter.hasNext()) {
    		result =  adapter.next().getTimestamp();
    	} else {
    		result = new Timestamp(0, TimeUnit.MILLISECONDS);
    	}
        return result;
	}

	@Override
	public void insert(Collection<LastUpdate> lastUpdates) {
		Batch batch = unloggedBatch();
		for (LastUpdate lu : lastUpdates) {
			Insert insertLastUpdate = insertInto(SchemaConstants.T_LAST_UPDATE)
	           		.value(SchemaConstants.F_RESOURCE, lu.getResource().getId())
	           		.value(SchemaConstants.F_LAST_UPDATE, lu.getTimestamp().asMillis());
			batch.add(insertLastUpdate.using(ttl(m_ttl)));
		}
		m_session.execute(batch);
	}
	
    private Iterator<com.datastax.driver.core.Row> cassandraSelectLastUpdate(Resource resource) {
    	List<Future<ResultSet>> futures = Lists.newArrayList();
    	BoundStatement bindStatement = m_selectLastUpdateStatement.bind();
    	bindStatement.setString(SchemaConstants.F_RESOURCE, resource.getId());
    	bindStatement.setConsistencyLevel(ConsistencyLevel.ONE);
    	futures.add(m_session.executeAsync(bindStatement));
    	return new ConcurrentResultWrapper(futures);
    }
}
