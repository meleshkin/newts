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
package org.opennms.newts.rest;


import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.opennms.newts.api.Duration;
import org.opennms.newts.api.LastUpdate;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Results;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.ValueType;
import org.opennms.newts.api.query.ResultDescriptor;
import org.opennms.newts.api.search.SearchResults;
import org.opennms.newts.api.search.SearchResults.Result;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;


class Transform {
    
    private Transform() {}

    private static final Function<SampleDTO, Sample> DTO_TO_SAMPLE;
    private static final Function<LastUpdateDTO, LastUpdate> DTO_TO_LAST_UPDATE;

    static {
        DTO_TO_SAMPLE = new Function<SampleDTO, Sample>() {

            @Override
            public Sample apply(SampleDTO input) {
                return new Sample(
                        Timestamp.fromEpochMillis(input.getTimestamp()),
                        input.getContext(),
                        new Resource(input.getResource().getId(), wrapMap(input.getResource().getAttributes())),
                        input.getName(),
                        input.getType(),
                        ValueType.compose(input.getValue(), input.getType()),
                        input.getAttributes());
            }
        };
        
        DTO_TO_LAST_UPDATE = new Function<LastUpdateDTO, LastUpdate>() {
        	
			@Override
			public LastUpdate apply(LastUpdateDTO input) {
				return new LastUpdate(Timestamp.fromEpochMillis(input.getTimestamp()), new Resource(input.getResource()));
			}
		};
    }

    private static Optional<Map<String, String>> wrapMap(Map<String, String> map) {
        if (map == null) return Optional.<Map<String, String>> absent();
        return Optional.of(map);
    }

    /**
     * Convert a {@link ResultDescriptorDTO} to {@link ResultDescriptor}.
     *
     * @param rDescriptorDTO
     *            the DTO to transform
     * @return the corresponding descriptor
     */
    static ResultDescriptor resultDescriptor(ResultDescriptorDTO rDescriptorDTO) {

        ResultDescriptor rDescriptor = new ResultDescriptor(rDescriptorDTO.getInterval());

        for (ResultDescriptorDTO.Datasource ds : rDescriptorDTO.getDatasources()) {
            if (ds.getHeartbeat() != null) {
                rDescriptor.datasource(ds.getLabel(), ds.getSource(), ds.getHeartbeat(), ds.getFunction());
            }
            else {
                rDescriptor.datasource(ds.getLabel(), ds.getSource(), ds.getFunction());
            }
        }
        
        for (ResultDescriptorDTO.Expression expr : rDescriptorDTO.getExpressions()) {
            rDescriptor.expression(expr.getLabel(), expr.getExpression());
        }

        rDescriptor.export(rDescriptorDTO.getExports());

        return rDescriptor;
    }

    /**
     * Convert {@link SampleDTO}s to {@link Sample}s.
     *
     * @param samples
     *            samples to convert
     * @return converted samples
     */
    static Collection<Sample> samples(Collection<SampleDTO> samples) {
        return Collections2.transform(samples, DTO_TO_SAMPLE);
    }
    
    static Collection<LastUpdate> lastUpdates(Collection<LastUpdateDTO> lastUpdates) {
    	return Collections2.transform(lastUpdates, DTO_TO_LAST_UPDATE);
    }

    /**
     * Convert samples to {@link SampleDTO}s.
     *
     * @param samples
     *            samples to convert.
     * @return converted samples.
     */
    static Collection<Collection<SampleDTO>> sampleDTOs(Results<Sample> samples) {
        return Lists.newArrayList(Iterables.transform(samples, new Function<Results.Row<Sample>, Collection<SampleDTO>>() {

            @Override
            public Collection<SampleDTO> apply(Row<Sample> input) {
                return Collections2.transform(input.getElements(), new Function<Sample, SampleDTO>() {

                    @Override
                    public SampleDTO apply(Sample input) {
                        return new SampleDTO(
                                input.getTimestamp().asMillis(),
                                new ResourceDTO(input.getResource().getId(), unwrapMap(input.getResource().getAttributes())),
                                input.getName(),
                                input.getType(),
                                input.getValue(),
                                input.getAttributes(),
                                input.getContext().getId());
                    }
                });
            }
        }));
    }

    /**
     * Convert measurements to {@link MeasurementDTO}s.
     *
     * @param measurements
     *            measurements to convert.
     * @return converted measurements.
     */
    static Collection<Collection<MeasurementDTO>> measurementDTOs(Results<Measurement> measurements) {
        return Lists.newArrayList(Iterables.transform(measurements, new Function<Results.Row<Measurement>, Collection<MeasurementDTO>>() {

            @Override
            public Collection<MeasurementDTO> apply(Row<Measurement> input) {
                return Collections2.transform(input.getElements(), new Function<Measurement, MeasurementDTO>() {

                    @Override
                    public MeasurementDTO apply(Measurement input) {
                        return new MeasurementDTO(
                                input.getTimestamp().asMillis(),
                                new ResourceDTO(input.getResource().getId(), unwrapMap(input.getResource().getAttributes())),
                                input.getName(),
                                input.getValue(),
                                input.getAttributes());
                    }
                });
            }
        }));
    }

    /**
     * Convert search results to {@link SearchResultDTO}s.
     *
     * @param results
     *            search results to convert.
     * @return converted search results.
     */
    public static Collection<SearchResultDTO> searchResultDTOs(SearchResults results) {
        return Lists.newArrayList(Iterables.transform(results, new Function<SearchResults.Result, SearchResultDTO>() {
            @Override
            public SearchResultDTO apply(Result input) {
                return new SearchResultDTO(
                        new ResourceDTO(input.getResource().getId(), unwrapMap(input.getResource().getAttributes())),
                        input.getMetrics());
            }
        }));
    }

    private static Map<String, String> unwrapMap(Optional<Map<String, String>> wrapped) {
        if (!wrapped.isPresent()) return Collections.<String, String> emptyMap();
        return wrapped.get();
    }

    static Optional<Timestamp> toTimestamp(Optional<TimestampParam> value) {
        return value.isPresent() ? Optional.of(value.get().get()) : Optional.<Timestamp>absent();
    }

    static Optional<Duration> toDuration(Optional<DurationParam> value) {
        return value.isPresent() ? Optional.of(value.get().get()) : Optional.<Duration>absent();
    }
}
