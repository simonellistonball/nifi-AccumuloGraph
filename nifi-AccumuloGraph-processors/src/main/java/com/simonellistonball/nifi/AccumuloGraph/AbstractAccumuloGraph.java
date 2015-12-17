package com.simonellistonball.nifi.AccumuloGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

public abstract class AbstractAccumuloGraph extends AbstractProcessor {

	public static final PropertyDescriptor ZOOKEEPER = new PropertyDescriptor.Builder()
			.name("Zookeeper").description("Zookeepers for Accumulo")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
			.name("Username").description("Username for Accumulo")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor INSTANCE = new PropertyDescriptor.Builder()
			.name("Instance").description("").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
			.name("Password").description("").required(true).sensitive(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor GRAPH = new PropertyDescriptor.Builder()
			.name("Graph").description("").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
			.name("Batch size").description("Number of flow files per batch")
			.required(false).defaultValue("1")
			.addValidator(StandardValidators.INTEGER_VALIDATOR).build();

	public static final String FALSE = "no";
	public static final String TRUE = "yes";
	public static final PropertyDescriptor ADD_ATTRIBUTES = new PropertyDescriptor.Builder()
			.name("Add attributes")
			.description(
					"Include attributes in vertex properties as well as flow file contents")
			.required(false).defaultValue(FALSE).allowableValues(TRUE, FALSE)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship FAILURE = new Relationship.Builder()
			.name("failure").description("failed").build();
	public static final Relationship SUCCESS = new Relationship.Builder()
			.name("success").description("success").build();
	protected List<PropertyDescriptor> descriptors;
	protected Set<Relationship> relationships;
	
	protected AccumuloGraphConfiguration cfg;

	public AbstractAccumuloGraph() {
		super();
	}

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(ZOOKEEPER);
		descriptors.add(USERNAME);
		descriptors.add(PASSWORD);
		descriptors.add(INSTANCE);
		descriptors.add(GRAPH);
		descriptors.add(BATCH_SIZE);
		descriptors.add(ADD_ATTRIBUTES);

		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(SUCCESS);
		relationships.add(FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);

	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		cfg = createConfig(context);
	}

	protected AccumuloGraphConfiguration createConfig(
			final ProcessContext context) {
		String zookeeper = context.getProperty(ZOOKEEPER).toString();
		String instance = context.getProperty(INSTANCE).toString();
		String user = context.getProperty(USERNAME).toString();
		String pass = context.getProperty(PASSWORD).toString();
		String graph = context.getProperty(GRAPH).toString();

		cfg = new AccumuloGraphConfiguration()
				.setInstanceType(InstanceType.Distributed)
				.setZooKeeperHosts(zookeeper).setInstanceName(instance)
				.setUser(user).setPassword(pass).setGraphName(graph)
				.setCreate(true);
		return cfg;
	}

}