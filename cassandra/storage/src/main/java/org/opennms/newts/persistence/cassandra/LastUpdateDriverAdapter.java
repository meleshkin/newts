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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.opennms.newts.api.LastUpdate;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Results;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.Timestamp;

public class LastUpdateDriverAdapter implements Iterable<Results.Row<LastUpdate>>, Iterator<Results.Row<LastUpdate>>{

	private final Iterator<com.datastax.driver.core.Row> m_results;
    private final Set<String> m_metrics;
    private Results.Row<LastUpdate> m_next = null;
    private int m_count = 0;
    
    
    public LastUpdateDriverAdapter(Iterator<com.datastax.driver.core.Row> input) {
    	this(input, Collections.<String> emptySet());
    }
    
    public LastUpdateDriverAdapter(Iterator<com.datastax.driver.core.Row> input, Set<String> metrics) {
    	m_results = input;
    	m_metrics = metrics;
    	if (m_results.hasNext()) {
    		LastUpdate lu = getNextLastUpdate();
    		m_next = new Results.Row<>(lu.getTimestamp(), lu.getResource());
    	}
	}
    
	@Override
	public Iterator<Row<LastUpdate>> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return m_next != null;
	}

	@Override
	public Row<LastUpdate> next() {
		if (!hasNext()) throw new NoSuchElementException();
        Results.Row<LastUpdate> nextNext = null;
        
        while (m_results.hasNext()) {
        	LastUpdate lu = getNextLastUpdate();
        	if (lu.getTimestamp().gt(m_next.getTimestamp())) {
        		nextNext = new Results.Row<>(lu.getTimestamp(), lu.getResource());
        		addLastUpdate(nextNext, lu);
        		break;
        	}
        	addLastUpdate(m_next, lu);
        }
        try {
        	return m_next;
        } finally {
        	m_next = nextNext;
        }
	}
	public int getResultCount() {
		return m_count;
	}
	
	private void addLastUpdate(Results.Row<LastUpdate> row, LastUpdate lastUpdate) {
		if (m_metrics.isEmpty() || m_metrics.contains(lastUpdate.getResource().getId())) {
			row.addElement(lastUpdate);
		}
	}
	
	private LastUpdate getNextLastUpdate() {
		m_count += 1;
		return getLastUpdate(m_results.next());
	}
	private static LastUpdate getLastUpdate(com.datastax.driver.core.Row row) {
		return new LastUpdate(Timestamp.fromEpochMillis(row.getTimestamp(SchemaConstants.F_LAST_UPDATE).getTime()), 
				new Resource(row.getString(SchemaConstants.F_RESOURCE)));
	}

}