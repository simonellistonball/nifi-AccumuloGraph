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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PutAccumuloGraphEdgeTest {

	private static final String PASSWORD = "password";

	private static final String USER = "root";

	private TestRunner vertexRunner;
	private TestRunner testRunner;

	@Rule
	public TemporaryFolder testFolder = new TemporaryFolder();

	private MiniAccumuloCluster accumulo;

	@Before
	public void init() throws IOException, InterruptedException,
			AccumuloException, AccumuloSecurityException {
		testRunner = TestRunners.newTestRunner(PutAccumuloGraphEdge.class);
		vertexRunner = TestRunners.newTestRunner(PutAccumuloGraphVertex.class);

		// setup a local accumulo for the graph client
		File tempDirectory = testFolder.newFolder("accumulo");
		accumulo = new MiniAccumuloCluster(tempDirectory, PASSWORD);
		accumulo.start();
	}

	@After
	public void cleanup() throws IOException, InterruptedException {
		accumulo.stop();
	}

	@Test
	public void testProcessor() {
		configureEdgeRunner(testRunner);
		configureVertexRunner(vertexRunner);

		String content = "test";

		createVertex("test2");
		createVertex("test3");

		final Map<String, String> attributes = new HashMap<>();
		attributes.put("id", "test2-3");
		attributes.put("a", "test2");
		attributes.put("b", "test3");

		testRunner
				.enqueue(content.getBytes(StandardCharsets.UTF_8), attributes);
		testRunner.run();
		testRunner
				.assertAllFlowFilesTransferred(PutAccumuloGraphVertex.SUCCESS);
	}

	private void createVertex(String string) {
		MockFlowFile ff = new MockFlowFile(0);
		Map<String, String> attrs = new HashMap<String, String>();
		attrs.put("id", string);
		ff.putAttributes(attrs);
		vertexRunner.enqueue(ff);
	}

	private void configureVertexRunner(TestRunner vertexRunner2) {
		testRunner.setProperty(PutAccumuloGraphVertex.ZOOKEEPER,
				accumulo.getZooKeepers());
		testRunner.setProperty(PutAccumuloGraphVertex.USERNAME, USER);
		testRunner.setProperty(PutAccumuloGraphVertex.PASSWORD, PASSWORD);
		testRunner.setProperty(PutAccumuloGraphVertex.GRAPH, "test");
		testRunner.setProperty(PutAccumuloGraphVertex.INSTANCE,
				accumulo.getInstanceName());
		testRunner.setProperty(PutAccumuloGraphVertex.ADD_ATTRIBUTES,
				PutAccumuloGraphVertex.TRUE);
		testRunner.setProperty(PutAccumuloGraphVertex.BATCH_SIZE, "1");
	}

	private void configureEdgeRunner(TestRunner testRunner2) {
		testRunner.setProperty(PutAccumuloGraphEdge.ZOOKEEPER,
				accumulo.getZooKeepers());
		testRunner.setProperty(PutAccumuloGraphEdge.USERNAME, USER);
		testRunner.setProperty(PutAccumuloGraphEdge.PASSWORD, PASSWORD);
		testRunner.setProperty(PutAccumuloGraphEdge.GRAPH, "test");
		testRunner.setProperty(PutAccumuloGraphEdge.INSTANCE,
				accumulo.getInstanceName());
		testRunner.setProperty(PutAccumuloGraphEdge.ADD_ATTRIBUTES,
				PutAccumuloGraphEdge.TRUE);
		testRunner.setProperty(PutAccumuloGraphEdge.BATCH_SIZE, "1");
	}

}
