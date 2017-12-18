package org.ekstep.graph.model.node;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.ekstep.common.dto.Request;
import org.ekstep.common.dto.Response;
import org.ekstep.common.exception.ClientException;
import org.ekstep.common.exception.ResponseCode;
import org.ekstep.graph.common.mgr.BaseGraphManager;
import org.ekstep.graph.dac.enums.GraphDACParams;
import org.ekstep.graph.dac.enums.SystemNodeTypes;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.graph.exception.GraphEngineErrorCodes;
import org.ekstep.graph.model.IRelation;
import org.ekstep.graph.model.relation.HasRelRelation;

import akka.dispatch.Futures;
import scala.concurrent.Future;
import scala.concurrent.Promise;

public class MetadataNode extends AbstractIndexNode {

    private String objectType;
    private String name;
    private static final String METADATA_NODE_NAME_KEY = "METADATA_PROPERTY_NAME";


    public MetadataNode(BaseGraphManager manager, String graphId, String objectType, String name) {
        super(manager, graphId);
        if (StringUtils.isBlank(objectType) || StringUtils.isBlank(name))
            throw new ClientException(GraphEngineErrorCodes.ERR_INVALID_NODE.name(), "Invalid Metadata Node");
        this.objectType = objectType;
        setNodeId(getSystemNodeType() + "_" + objectType + "_" + name);
        this.name = name;
    }

    public Future<Map<String, Object>> create(final Request req) {
        final Promise<Map<String, Object>> promise = Futures.promise();
        Future<Map<String, Object>> future = promise.future();
        final String defNodeId = SystemNodeTypes.DEFINITION_NODE.name() + "_" + objectType;

		Response response = getNodeObject(req, searchMgr, getNodeId());
		if (manager.checkError(response)) {
			if (!StringUtils.equals(ResponseCode.RESOURCE_NOT_FOUND.name(), response.getResponseCode().name())) {
				failPromise(promise, GraphEngineErrorCodes.ERR_GRAPH_CREATE_METADATA_NODE_FAILED.name(),
						manager.getErrorMessage(response));
			} else {

				Response getDefNodeFuture = getNodeObject(req, searchMgr, defNodeId);

				if (manager.checkError(getDefNodeFuture)) {
					failPromise(promise, GraphEngineErrorCodes.ERR_GRAPH_CREATE_METADATA_NODE_FAILED.name(),
							manager.getErrorMessage(getDefNodeFuture));
				} else {
					Request request = new Request(req);
					request.put(GraphDACParams.node.name(), toNode());
					Response createFuture = nodeMgr.addNode(request);
					final IRelation rel = new HasRelRelation(getManager(), getGraphId(), defNodeId, getNodeId());
					createIndexNodeRelation(promise, null, createFuture, req, rel,
							GraphEngineErrorCodes.ERR_GRAPH_CREATE_METADATA_NODE_FAILED.name(),
							"Failed to create Metadata Node: " + getObjectType() + " - " + getName());
				}
			}
		} else {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put(GraphDACParams.node_id.name(), getNodeId());
			promise.success(map);
		}

        return future;
    }

    public String getSystemNodeType() {
        return SystemNodeTypes.METADATA_NODE.name();
    }

    public Node toNode() {
        Node node = new Node(getNodeId(), getSystemNodeType(), null);
        Map<String, Object> metadata = new HashMap<String, Object>();
        metadata.put(METADATA_NODE_NAME_KEY, getName());
        node.setMetadata(metadata);
        return node;
    }

    public String getObjectType() {
        return objectType;
    }

    public String getName() {
        return name;
    }

}
