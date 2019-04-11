package org.ekstep.jobs.samza.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.ekstep.common.dto.Request;
import org.ekstep.common.dto.Response;
import org.ekstep.common.exception.ServerException;
import org.ekstep.graph.dac.enums.GraphDACParams;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.graph.engine.router.GraphEngineManagers;
import org.ekstep.graph.model.node.DefinitionDTO;
import org.ekstep.jobs.samza.exception.PlatformErrorCodes;
import org.ekstep.jobs.samza.service.task.JobMetrics;
import org.ekstep.jobs.samza.util.FailedEventsUtil;
import org.ekstep.jobs.samza.util.JSONUtils;
import org.ekstep.jobs.samza.util.JobLogger;
import org.ekstep.learning.hierarchy.store.HierarchyStore;
import org.ekstep.learning.router.LearningRequestRouterPool;
import org.ekstep.learning.util.ControllerUtil;


import java.util.*;
import java.util.stream.Collectors;

public class CollectionMigrationService implements ISamzaService {

	protected static final String DEFAULT_CONTENT_IMAGE_OBJECT_SUFFIX = ".img";
	private static JobLogger LOGGER = new JobLogger(CollectionMigrationService.class);
	private Config config = null;
	private SystemStream systemStream = null;
	private ControllerUtil util = new ControllerUtil();
//	private ObjectMapper mapper = new ObjectMapper();
	private HierarchyStore hierarchyStore = null;

	public void initialize(Config config) throws Exception {
		this.config = config;
		JSONUtils.loadProperties(config);
		LOGGER.info("Service config initialized.");
		LearningRequestRouterPool.init();
		LOGGER.info("Akka actors initialized");
		systemStream = new SystemStream("kafka", config.get("output.failed.events.topic.name"));
		LOGGER.info("Stream initialized for Failed Events");
		hierarchyStore = new HierarchyStore();
	}

	@Override
	public void processMessage(Map<String, Object> message, JobMetrics metrics, MessageCollector collector) throws Exception {
		if (null == message) {
			LOGGER.info("Ignoring the message because it is not valid for collection migration.");
			return;
		}
		Map<String, Object> edata = (Map<String, Object>) message.get("edata");
		Map<String, Object> object = (Map<String, Object>) message.get("object");

		if (!validateEdata(edata) || null == object) {
			LOGGER.info("Ignoring the message because it is not valid for collection migration.");
			return;
		}
		try {
			String nodeId = (String) object.get("id");
			if (StringUtils.isNotBlank(nodeId)) {
				Node node = getNode(nodeId);
				if (null != node && validNode(node)) {

					Number version = (Number) node.getMetadata().get("version");
					if (version != null && version.intValue() >= 2) {
						LOGGER.info("Migration is already completed for Content ID: " + node.getIdentifier() + ". So, skipping this message.");
						return;
					}
					LOGGER.info("Initializing migration for collection ID: " + node.getIdentifier());
					LOGGER.info("Fetching hierarchy from Neo4J DB.");
					String rootId = node.getIdentifier();
					DefinitionDTO definition = getDefinition("domain", node.getObjectType());
					Map<String, Object> rootHierarchy = util.getHierarchyMap("domain", rootId, definition, "edit",null);
					LOGGER.info("Got hierarchy data from Neo4J DB.");
					if (MapUtils.isNotEmpty(rootHierarchy)) {
						Map<String, Object> hierarchy = new HashMap<>();
						hierarchy.put("identifier", rootHierarchy.get("identifier"));
						List<Map<String, Object>> children = (List<Map<String, Object>>) rootHierarchy.get("children");
						List<String> collectionIds = new ArrayList<>();
						updateAndGetCollectionsInHierarchy(children, collectionIds);
						LOGGER.info("Total number of collections to delete: " + collectionIds.size());
						hierarchy.put("children", children);
						LOGGER.info("Saving hierarchy to Cassandra.");
						hierarchyStore.saveOrUpdateHierarchy(rootId, hierarchy);
						LOGGER.info("Saved hierarchy to Cassandra.");
						LOGGER.info("Updating the node version to 2 for collection ID: " + node.getIdentifier());
						node.getMetadata().put("version", 2);
						Response response = util.updateNode(node);
						if (!util.checkError(response)) {
							LOGGER.info("Updated the node version to 2 for collection ID: " + node.getIdentifier());
						} else {
							LOGGER.error("Failed to update the node version to 2 for collection ID: " + node.getIdentifier() + " with error: " + response.getParams().getErrmsg(), response.getResult(), null);
						}
						LOGGER.info("Migration completed for collection ID: " + node.getIdentifier());
					} else {
						LOGGER.info("There is no hierarchy data for the content ID: " + node.getIdentifier());
					}
				} else {
					metrics.incSkippedCounter();
					FailedEventsUtil.pushEventForRetry(systemStream, message, metrics, collector,
							PlatformErrorCodes.PROCESSING_ERROR.name(), new ServerException("ERR_COLLECTION_MIGRATION", "Please check neo4j connection or identifier to migrate."));
					LOGGER.info("Invalid Node Object. Unable to process the event", message);
				}
			} else {
				metrics.incSkippedCounter();
				LOGGER.info("Invalid NodeId. Unable to process the event", message);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void updateAndGetCollectionsInHierarchy(List<Map<String, Object>> children, List<String> collectionIds) {
		if (CollectionUtils.isNotEmpty(children)) {
			children = children.stream().map(child ->  {
				if (StringUtils.equalsIgnoreCase((String) child.get("mimeType"), "application/vnd.ekstep.content-collection") && StringUtils.equalsIgnoreCase((String) child.get("visibility"), "Parent")) {
					collectionIds.add((String) child.get("identifier"));
					String id = ((String) child.get("identifier")).replaceAll(".img", "");
					child.put("status", "Draft");
					child.put("objectType", "Content");
					child.put("identifier", id);
				}
				return child;
			}).collect(Collectors.toList());
			List<Map<String, Object>> nextChildren = children.stream()
					.map(child -> (List<Map<String, Object>>) child.get("children"))
					.filter(f -> CollectionUtils.isNotEmpty(f)).flatMap(f -> f.stream())
					.collect(Collectors.toList());
			if (CollectionUtils.isNotEmpty(nextChildren)) {
				updateAndGetCollectionsInHierarchy(nextChildren, collectionIds);
			}
		} else {
			LOGGER.info("Children is empty: "+ children);
		}
	}

	/**
	 * Checking is it a valid node for migration.
	 *
	 * @param node
	 * @return boolean
	 */
	private boolean validNode(Node node) {
		Map<String, Object> metadata = (Map<String, Object>) node.getMetadata();
		String visibility = (String) metadata.get("visibility");
		String mimeType = (String) metadata.get("mimeType");
		if (StringUtils.equalsIgnoreCase("Default", visibility) && StringUtils.equalsIgnoreCase("application/vnd.ekstep.content-collection", mimeType))
			return true;
		else
			return false;
	}

	private boolean validateEdata(Map<String, Object> edata) {
		String action = (String) edata.get("action");
		if (StringUtils.equalsIgnoreCase("collection-migration", action)) {
			return true;
		}
		return false;
	}

	private Node getNode(String nodeId) {
		String imgNodeId = nodeId + DEFAULT_CONTENT_IMAGE_OBJECT_SUFFIX;
		Node node = util.getNode("domain", imgNodeId);
		if (null == node) {
			node = util.getNode("domain", nodeId);
		}
		return node;
	}

	protected DefinitionDTO getDefinition(String graphId, String objectType) {
		Request request = util.getRequest(graphId, GraphEngineManagers.SEARCH_MANAGER, "getNodeDefinition",
				GraphDACParams.object_type.name(), objectType);
		Response response = util.getResponse(request);
		if (!util.checkError(response)) {
			DefinitionDTO definition = (DefinitionDTO) response.get(GraphDACParams.definition_node.name());
			return definition;
		}
		return null;
	}
}