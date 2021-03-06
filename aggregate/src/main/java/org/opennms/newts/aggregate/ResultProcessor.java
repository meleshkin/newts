/*
 * Copyright 2014, The OpenNMS Group
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
package org.opennms.newts.aggregate;


import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Results;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.query.ResultDescriptor;

// Encapsulation of results processing.

public class ResultProcessor {

    private final Resource m_resource;
    private final Timestamp m_start;
    private final Timestamp m_end;
    private final ResultDescriptor m_resultDescriptor;
    private final Duration m_resolution;

    public ResultProcessor(Resource resource, Timestamp start, Timestamp end, ResultDescriptor descriptor, Duration resolution) {
        m_resource = checkNotNull(resource, "resource argument");
        m_start = checkNotNull(start, "start argument");
        m_end = checkNotNull(end, "end argument");
        m_resultDescriptor = checkNotNull(descriptor, "result descriptor argument");
        m_resolution = checkNotNull(resolution, "resolution argument");
    }

    public Results<Measurement> process(Iterator<Row<Sample>> samples) {
        checkNotNull(samples, "samples argument");

        // Build chain of iterators to process results as a stream
        Rate rate = new Rate(samples, m_resultDescriptor.getSourceNames());
        PrimaryData primaryData = new PrimaryData(m_resource, m_start.minus(m_resolution), m_end, m_resultDescriptor, rate);
        Aggregation aggregation = new Aggregation(m_resource, m_start, m_end, m_resultDescriptor, m_resolution, primaryData);
        Compute compute = new Compute(m_resultDescriptor, aggregation);
        Export exports = new Export(m_resultDescriptor.getExports(), compute);

        Results<Measurement> measurements = new Results<>();

        for (Row<Measurement> row : exports) {
            measurements.addRow(row);
        }

        return measurements;
    }

}
