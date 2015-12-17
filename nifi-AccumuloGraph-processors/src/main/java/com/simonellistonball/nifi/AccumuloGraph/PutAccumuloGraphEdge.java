/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.simonellistonball.nifi.AccumuloGraph;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import edu.jhuapl.tinkerpop.AccumuloBulkIngester;
import edu.jhuapl.tinkerpop.AccumuloBulkIngester.PropertyBuilder;

@Tags({ "accumulo" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({
		@ReadsAttribute(attribute = "id", description = "Edge Id in the graph"),
		@ReadsAttribute(attribute = "a", description = "Start vertex id"),
		@ReadsAttribute(attribute = "b", description = "End vertex id") })
public class PutAccumuloGraphEdge extends AbstractAccumuloGraph {

	@Override
	public void onTrigger(final ProcessContext context,
			final ProcessSession session) throws ProcessException {
		List<FlowFile> flowFiles = session.get(context.getProperty(BATCH_SIZE)
				.asInteger().intValue());
		if (flowFiles == null || flowFiles.size() == 0) {
			return;
		}

		try {
			AccumuloBulkIngester ingestor = new AccumuloBulkIngester(cfg);
			boolean includeAttributes = context.getProperty(ADD_ATTRIBUTES)
					.equals(TRUE);

			for (FlowFile flowFile : flowFiles) {
				try {
					PropertyBuilder addEdge = ingestor.addEdge(
							flowFile.getAttribute("a"),
							flowFile.getAttribute("b"),
							flowFile.getAttribute("id"));
					if (includeAttributes) {
						final Map<String, String> attributes = flowFile
								.getAttributes();
						for (final Entry<String, String> entry : attributes
								.entrySet()) {
							// TODO - blacklist some attributes?
							addEdge.add(entry.getKey(), entry.getValue());
						}
					}
					addEdge.finish();
				} catch (MutationsRejectedException e) {
					session.transfer(flowFile, FAILURE);
				}
				session.transfer(flowFile, SUCCESS);
			}

			ingestor.shutdown(true);

		} catch (AccumuloException | AccumuloSecurityException
				| TableNotFoundException | TableExistsException | IOException
				| InterruptedException e1) {
			session.transfer(flowFiles, FAILURE);
		}

	}

}
