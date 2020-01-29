package org.ekstep.content.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.rits.cloning.Cloner;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.ekstep.common.Platform;
import org.ekstep.common.dto.Request;
import org.ekstep.common.dto.Response;
import org.ekstep.common.enums.TaxonomyErrorCodes;
import org.ekstep.common.exception.ClientException;
import org.ekstep.common.exception.ServerException;
import org.ekstep.common.mgr.ConvertGraphNode;
import org.ekstep.common.mgr.ConvertToGraphNode;
import org.ekstep.content.common.ContentConfigurationConstants;
import org.ekstep.content.common.ContentErrorMessageConstants;
import org.ekstep.content.common.EcarPackageType;
import org.ekstep.content.common.ExtractionType;
import org.ekstep.content.enums.ContentErrorCodeConstants;
import org.ekstep.content.enums.ContentWorkflowPipelineParams;
import org.ekstep.content.util.ContentBundle;
import org.ekstep.content.util.GraphUtil;
import org.ekstep.content.util.SyncMessageGenerator;
import org.ekstep.graph.cache.util.RedisStoreUtil;
import org.ekstep.graph.dac.enums.GraphDACParams;
import org.ekstep.graph.dac.enums.RelationTypes;
import org.ekstep.graph.dac.enums.SystemProperties;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.graph.dac.model.Relation;
import org.ekstep.graph.engine.router.GraphEngineManagers;
import org.ekstep.graph.model.node.DefinitionDTO;
import org.ekstep.graph.service.common.DACConfigurationConstants;
import org.ekstep.learning.common.enums.ContentAPIParams;
import org.ekstep.learning.util.CloudStore;
import org.ekstep.searchindex.elasticsearch.ElasticSearchUtil;
import org.ekstep.searchindex.util.CompositeSearchConstants;
import org.ekstep.telemetry.logger.TelemetryManager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class CollectionPublisher extends BasePublisher {
    private static final String  ES_INDEX_NAME = CompositeSearchConstants.COMPOSITE_SEARCH_INDEX;
    private static final String DOCUMENT_TYPE = Platform.config.hasPath("search.document.type") ? Platform.config.getString("search.document.type") : CompositeSearchConstants.COMPOSITE_SEARCH_INDEX_TYPE;

    public CollectionPublisher(String basePath, String contentId){
        if (!isValidBasePath(basePath))
            throw new ClientException(ContentErrorCodeConstants.INVALID_PARAMETER.name(),
                    ContentErrorMessageConstants.INVALID_CWP_CONST_PARAM + " | [Path does not Exist.]");
        if (StringUtils.isBlank(contentId))
            throw new ClientException(ContentErrorCodeConstants.INVALID_PARAMETER.name(),
                    ContentErrorMessageConstants.INVALID_CWP_CONST_PARAM + " | [Invalid Content Id.]");
        this.basePath = basePath;
        this.contentId = contentId;
    }

    public Response publish(Map<String, Object> parameterMap) {
        Node node = (Node) parameterMap.get(ContentWorkflowPipelineParams.node.name());
        RedisStoreUtil.delete(COLLECTION_CACHE_KEY_PREFIX + contentId);
        List<String> unitNodes = new ArrayList<>();
        if (node.getIdentifier().endsWith(".img")) {
            String updatedVersion = preUpdateNode(node.getIdentifier());
            node.getMetadata().put(GraphDACParams.versionKey.name(), updatedVersion);
            getUnitFromLiveContent(unitNodes);
            cleanUnitsInRedis(unitNodes);
        }
        node.setIdentifier(contentId);
        node.setObjectType(ContentWorkflowPipelineParams.Content.name());
        createThumbnail(basePath, node);
        resetNodeForPublish(node);

        Map<String,Object> collectionHierarchy = getHierarchy(node.getIdentifier(), true);
        TelemetryManager.log("Hierarchy for content : " + node.getIdentifier() + " : " + collectionHierarchy);
        List<Map<String, Object>> children = null;
        if(MapUtils.isNotEmpty(collectionHierarchy)) {
            Set<String> collectionResourceChildNodes = new HashSet<>();
            children = (List<Map<String,Object>>)collectionHierarchy.get("children");
            enrichChildren(children, collectionResourceChildNodes, node);
            if(!collectionResourceChildNodes.isEmpty()) {
                List<String> collectionChildNodes = getList(node.getMetadata().get(ContentWorkflowPipelineParams.childNodes.name()));
                collectionChildNodes.addAll(collectionResourceChildNodes);
            }

        }
        processCollection(node, children);
        processForEcar(node, children);

        try {
            TelemetryManager.log("Deleting the temporary folder: " + basePath);
            delete(new File(basePath));
        } catch (Exception e) {
            e.printStackTrace();
            TelemetryManager.error("Error deleting the temporary folder: " + basePath, e);
        }
        if (BooleanUtils.isTrue(ContentConfigurationConstants.IS_ECAR_EXTRACTION_ENABLED)) {
            contentPackageExtractionUtil.copyExtractedContentPackage(contentId, node, ExtractionType.version);
            contentPackageExtractionUtil.copyExtractedContentPackage(contentId, node, ExtractionType.latest);
        }

        Node newNode = new Node(node.getIdentifier(), node.getNodeType(), node.getObjectType());
        newNode.setGraphId(node.getGraphId());
        newNode.setMetadata(node.getMetadata());
        newNode.setTags(node.getTags());

        // Setting default version key for internal node update
        String graphPassportKey = Platform.config.getString(DACConfigurationConstants.PASSPORT_KEY_BASE_PROPERTY);
        newNode.getMetadata().put(GraphDACParams.versionKey.name(), graphPassportKey);

        newNode.setInRelations(node.getInRelations());
        newNode.setOutRelations(node.getOutRelations());

        TelemetryManager.log("Migrating the Image Data to the Live Object. | [Content Id: " + contentId + ".]");
        Response response = migrateContentImageObjectData(contentId, newNode);

        // delete image..
        Request request = getRequest(TAXONOMY_ID, GraphEngineManagers.NODE_MANAGER,
                "deleteDataNode");
        request.put(ContentWorkflowPipelineParams.node_id.name(), contentId + ".img");

        getResponse(request);

        Node publishedNode = util.getNode(ContentWorkflowPipelineParams.domain.name(), newNode.getIdentifier());
        updateHierarchyMetadata(children, publishedNode);
        publishHierarchy(publishedNode, children);
        syncNodes(children, unitNodes);

        return response;

    }

    private void resetNodeForPublish(Node node) {
        // Set Package Version
        double version = 1.0;
        if (null != node && null != node.getMetadata()
                && null != node.getMetadata().get(ContentWorkflowPipelineParams.pkgVersion.name()))
            version = getDoubleValue(node.getMetadata().get(ContentWorkflowPipelineParams.pkgVersion.name())) + 1;
        node.getMetadata().put(ContentWorkflowPipelineParams.pkgVersion.name(), version);
        node.getMetadata().put(ContentWorkflowPipelineParams.lastPublishedOn.name(), formatCurrentDate());
        node.getMetadata().put(ContentWorkflowPipelineParams.flagReasons.name(), null);
        node.getMetadata().put(ContentWorkflowPipelineParams.body.name(), null);
        node.getMetadata().put(ContentWorkflowPipelineParams.publishError.name(), null);
        node.getMetadata().put(ContentWorkflowPipelineParams.variants.name(), null);

        setCompatibilityLevel(node);
        String publishType = (String) node.getMetadata().get(ContentWorkflowPipelineParams.publish_type.name());
        node.getMetadata().put(ContentWorkflowPipelineParams.status.name(),
                StringUtils.equalsIgnoreCase(publishType, ContentWorkflowPipelineParams.Unlisted.name())?
                        ContentWorkflowPipelineParams.Unlisted.name(): ContentWorkflowPipelineParams.Live.name());
        node.getMetadata().put(ContentWorkflowPipelineParams.publish_type.name(), null);
    }

    private String preUpdateNode(String identifier) {
        identifier = identifier.replace(".img", "");
        Response response = getDataNode(TAXONOMY_ID, identifier);
        if (!checkError(response)) {
            Node node = (Node) response.get(GraphDACParams.node.name());
            Response updateResp = updateNode(node, true);
            if (!checkError(updateResp)) {
                return (String) updateResp.get(GraphDACParams.versionKey.name());
            } else {
                throw new ServerException(ContentErrorCodeConstants.PUBLISH_ERROR.name(),
                        ContentErrorMessageConstants.CONTENT_IMAGE_MIGRATION_ERROR + " | [Content Id: " + contentId
                                + "]" + "::" + updateResp.getParams().getErr() + " :: " + updateResp.getParams().getErrmsg() + " :: "
                                + updateResp.getResult());
            }
        } else {
            throw new ServerException(ContentErrorCodeConstants.PUBLISH_ERROR.name(),
                    ContentErrorMessageConstants.CONTENT_IMAGE_MIGRATION_ERROR + " | [Content Id: " + contentId + "]");
        }
    }

    private void getUnitFromLiveContent(List<String> unitNodes){
        Map<String, Object> liveContentHierarchy = getHierarchy(contentId, false);
        if(MapUtils.isNotEmpty(liveContentHierarchy)) {
            List<Map<String, Object>> children = (List<Map<String, Object>>)liveContentHierarchy.get("children");
            getUnitFromLiveContent(unitNodes, children);
        }
    }
    private void getUnitFromLiveContent(List<String> unitNodes, List<Map<String, Object>> children) {
        if(CollectionUtils.isNotEmpty(children)) {
            children.stream().forEach(child -> {
                if(StringUtils.equalsIgnoreCase("Parent", (String) child.get("visibility"))) {
                    unitNodes.add((String)child.get("identifier"));
                    getUnitFromLiveContent(unitNodes, (List<Map<String, Object>>) child.get("children"));
                }
            });
        }
    }

    private Map<String, Object> getHierarchy(String nodeId, boolean needImageHierarchy) {
        String identifier = nodeId;
        if(needImageHierarchy) {
            identifier = StringUtils.endsWith(nodeId, ".img") ? nodeId : nodeId + ".img";
        }

        return hierarchyStore.getHierarchy(identifier);
    }

    private void cleanUnitsInRedis(List<String> unitNodes) {
        if(CollectionUtils.isNotEmpty(unitNodes)) {
            String[] unitsIds = unitNodes.stream().map(id -> (COLLECTION_CACHE_KEY_PREFIX + id)).collect(Collectors.toList()).toArray(new String[unitNodes.size()]);
            if(unitsIds.length > 0)
                RedisStoreUtil.delete(unitsIds);
        }
    }

    private void setCompatibilityLevel(Node node) {
        if (LEVEL4_CONTENT_TYPES.contains(node.getMetadata().getOrDefault(ContentWorkflowPipelineParams.contentType.name(), "").toString())) {
            TelemetryManager.info("setting compatibility level for content id : " + node.getIdentifier() + " as 4.");
            node.getMetadata().put(ContentWorkflowPipelineParams.compatibilityLevel.name(), 4);
        }
    }

    private void enrichChildren(List<Map<String, Object>> children, Set<String> collectionResourceChildNodes, Node node) {
        try {
            if (CollectionUtils.isNotEmpty(children)) {
                List<Map<String, Object>> newChildren = new ArrayList<>(children);
                if (null != newChildren && !newChildren.isEmpty()) {
                    for (Map<String, Object> child : newChildren) {
                        if (StringUtils.equalsIgnoreCase((String) child.get(ContentWorkflowPipelineParams.visibility.name()), "Parent") &&
                                StringUtils.equalsIgnoreCase((String) child.get(ContentWorkflowPipelineParams.mimeType.name()), COLLECTION_MIMETYPE))
                            enrichChildren((List<Map<String, Object>>) child.get(ContentWorkflowPipelineParams.children.name()), collectionResourceChildNodes, node);
                        if (StringUtils.equalsIgnoreCase((String) child.get(ContentWorkflowPipelineParams.visibility.name()), "Default") &&
                                StringUtils.equalsIgnoreCase((String) child.get(ContentWorkflowPipelineParams.mimeType.name()), COLLECTION_MIMETYPE)) {
                            Map<String, Object> collectionHierarchy = getHierarchy((String) child.get(ContentWorkflowPipelineParams.identifier.name()), false);
                            TelemetryManager.log("Collection hierarchy for childNode : " + child.get(ContentWorkflowPipelineParams.identifier.name()) + " : " + collectionHierarchy);
                            if (MapUtils.isNotEmpty(collectionHierarchy)) {
                                collectionHierarchy.put(ContentWorkflowPipelineParams.index.name(), child.get(ContentWorkflowPipelineParams.index.name()));
                                collectionHierarchy.put(ContentWorkflowPipelineParams.parent.name(), child.get(ContentWorkflowPipelineParams.parent.name()));
                                List<String> childNodes = getList(collectionHierarchy.get(ContentWorkflowPipelineParams.childNodes.name()));
                                if (!CollectionUtils.isEmpty(childNodes))
                                    collectionResourceChildNodes.addAll(childNodes);
                                if (!MapUtils.isEmpty(collectionHierarchy)) {
                                    children.remove(child);
                                    children.add(collectionHierarchy);
                                }
                            }
                        }
                        if (StringUtils.equalsIgnoreCase((String) child.get(ContentWorkflowPipelineParams.visibility.name()), "Default") &&
                                !StringUtils.equalsIgnoreCase((String) child.get(ContentWorkflowPipelineParams.mimeType.name()), COLLECTION_MIMETYPE)) {
                            Response readResponse = getDataNode(TAXONOMY_ID, (String) child.get(ContentWorkflowPipelineParams.identifier.name()));
                            children.remove(child);
                            List<String> childNodes = getList(node.getMetadata().get(ContentWorkflowPipelineParams.childNodes.name()));
                            if (!checkError(readResponse)) {
                                Node resNode = (Node) readResponse.get(GraphDACParams.node.name());
                                if (PUBLISHED_STATUS_LIST.contains(resNode.getMetadata().get(ContentWorkflowPipelineParams.status.name()))) {
                                    DefinitionDTO definition = util.getDefinition(TAXONOMY_ID, ContentWorkflowPipelineParams.Content.name());

                                    String nodeString = mapper.writeValueAsString(ConvertGraphNode.convertGraphNode(resNode, TAXONOMY_ID, definition, null));
                                    Map<String, Object> resourceNode = mapper.readValue(nodeString, Map.class);
                                    resourceNode.put("index", child.get(ContentWorkflowPipelineParams.index.name()));
                                    resourceNode.put("depth", child.get(ContentWorkflowPipelineParams.depth.name()));
                                    resourceNode.put("parent", child.get(ContentWorkflowPipelineParams.parent.name()));
                                    resourceNode.remove(ContentWorkflowPipelineParams.collections.name());
                                    resourceNode.remove(ContentWorkflowPipelineParams.children.name());
                                    children.add(resourceNode);
                                } else {
                                    childNodes.remove((String) child.get(ContentWorkflowPipelineParams.identifier.name()));
                                }
                                node.getMetadata().put(ContentWorkflowPipelineParams.childNodes.name(), childNodes);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new ServerException("ERR_INTERNAL_SERVER_ERROR", e.getMessage(), e);
        }
    }

    private List<String> getList(Object obj) {
        List<String> list = new ArrayList<String>();
        try {
            if (obj instanceof String) {
                list.add((String) obj);
            } else if (obj instanceof String[]) {
                list = Arrays.asList((String[]) obj);
            } else if (obj instanceof List){
                list.addAll((List<String>) obj);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (null != list) {
            list = list.stream().filter(x -> StringUtils.isNotBlank(x)).collect(toList());
        }
        return list;
    }

    private void processCollection(Node node, List<Map<String, Object>> children) {

        String contentId = node.getIdentifier();
        Map<String, Object> dataMap = null;
        dataMap = processChildren(node, children);
        TelemetryManager.log("Children nodes process for collection - " + contentId);
        if (MapUtils.isNotEmpty(dataMap)) {
            for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                if ("concepts".equalsIgnoreCase(entry.getKey()) || "keywords".equalsIgnoreCase(entry.getKey())) {
                    continue;
                } else {
                    Set<String> valueSet = (HashSet<String>) entry.getValue();
                    String[] value = valueSet.toArray(new String[valueSet.size()]);
                    node.getMetadata().put(entry.getKey(), value);
                }
            }
            Set<String> keywords = (HashSet<String>) dataMap.get("keywords");
            if (null != keywords && !keywords.isEmpty()) {
                if (null != node.getMetadata().get("keywords")) {
                    Object object = node.getMetadata().get("keywords");
                    if (object instanceof String[]) {
                        String[] stringArray = (String[]) node.getMetadata().get("keywords");
                        keywords.addAll(Arrays.asList(stringArray));
                    } else if (object instanceof String) {
                        String keyword = (String) node.getMetadata().get("keywords");
                        keywords.add(keyword);
                    }
                }
                List<String> keywordsList = new ArrayList<>();
                keywordsList.addAll(keywords);
                node.getMetadata().put("keywords", keywordsList);
            }
        }

        enrichCollection(node, children);

        addResourceToCollection(node, children);

    }

    private Map<String, Object> processChildren(Node node, List<Map<String, Object>> children) {
        Map<String, Object> dataMap = new HashMap<>();
        processChildren(children, dataMap);
        return dataMap;
    }

    private void processChildren(List<Map<String, Object>> children, Map<String, Object> dataMap){
        if (null!=children && !children.isEmpty()) {
            for (Map<String, Object> child : children) {
                mergeMap(dataMap, processChild(child));
                processChildren((List<Map<String, Object>>)child.get("children"), dataMap);
            }
        }
    }

    private Map<String, Object> processChild(Map<String, Object> node) {
        Map<String, Object> result = new HashMap<>();
        List<String> taggingProperties = Arrays.asList("language", "domain", "ageGroup", "genre", "theme", "keywords");
        for(String prop : node.keySet()) {
            if(taggingProperties.contains(prop)) {
                Object o = node.get(prop);
                if(o instanceof String)
                    result.put(prop, new HashSet<Object>(Arrays.asList((String)o)));
                if(o instanceof List)
                    result.put(prop, new HashSet<Object>((List<String>)o));
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mergeMap(Map<String, Object> dataMap, Map<String, Object> childDataMap){
        if (dataMap.isEmpty()) {
            dataMap.putAll(childDataMap);
        } else {
            for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                Set<Object> value = new HashSet<Object>();
                if (childDataMap.containsKey(entry.getKey())) {
                    value.addAll((Collection<? extends Object>) childDataMap.get(entry.getKey()));
                }
                value.addAll((Collection<? extends Object>) entry.getValue());
                dataMap.replace(entry.getKey(), value);
            }
            if (!dataMap.keySet().containsAll(childDataMap.keySet())) {
                for (Map.Entry<String, Object> entry : childDataMap.entrySet()) {
                    if (!dataMap.containsKey(entry.getKey())) {
                        dataMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        return dataMap;
    }

    public void enrichCollection(Node node, List<Map<String, Object>> children)  {

        String contentId = node.getIdentifier();
        TelemetryManager.info("Processing Collection Content :" + contentId);
        if (null != children && !children.isEmpty()) {
            Map<String, Object> content = getContentMap(node, children);
            if(MapUtils.isEmpty(content))
                return;
            int leafCount = getLeafNodeCount(content);
            double totalCompressedSize = 0.0;
            totalCompressedSize = getTotalCompressedSize(content, totalCompressedSize);
            content.put(ContentAPIParams.leafNodesCount.name(), leafCount);
            node.getMetadata().put(ContentAPIParams.leafNodesCount.name(), leafCount);
            content.put(ContentAPIParams.totalCompressedSize.name(), totalCompressedSize);
            node.getMetadata().put(ContentAPIParams.totalCompressedSize.name(), totalCompressedSize);
            updateLeafNodeIds(node, content);


            Map<String, Object> mimeTypeMap = new HashMap<>();
            Map<String, Object> contentTypeMap = new HashMap<>();
            List<String> childNodes = getChildNode(content);

            getTypeCount(content, "mimeType", mimeTypeMap);
            getTypeCount(content, "contentType", contentTypeMap);

            content.put(ContentAPIParams.mimeTypesCount.name(), mimeTypeMap);
            content.put(ContentAPIParams.contentTypesCount.name(), contentTypeMap);
            content.put(ContentAPIParams.childNodes.name(), childNodes);

            node.getMetadata().put(ContentAPIParams.toc_url.name(), generateTOC(node, content));
            try {
                node.getMetadata().put(ContentAPIParams.mimeTypesCount.name(), convertToString(mimeTypeMap));
                node.getMetadata().put(ContentAPIParams.contentTypesCount.name(), convertToString(contentTypeMap));
            } catch (Exception e) {
                TelemetryManager.error("Error while stringifying mimeTypeCount or contentTypesCount.", e);
            }
            node.getMetadata().put(ContentAPIParams.childNodes.name(), childNodes);
        }
    }


    private void processForEcar(Node node, List<Map<String, Object>> children) {
        List<Node> nodes = new ArrayList<Node>();
        String downloadUrl = null;
        String s3Key = null;
        String mimeType = (String) node.getMetadata().get(ContentWorkflowPipelineParams.mimeType.name());
        nodes.add(node);

        DefinitionDTO definition = util.getDefinition(TAXONOMY_ID, "Content");
        updateHierarchyMetadata(children, node);

        List<String> nodeIds = new ArrayList<>();
        nodeIds.add(node.getIdentifier());
        updateRootChildrenList(node, children);
        getNodeMap(children, nodes, nodeIds, definition);

        List<Map<String, Object>> contents = new ArrayList<Map<String, Object>>();
        List<String> childrenIds = new ArrayList<String>();
        getContentBundleData(node.getGraphId(), nodes, contents, childrenIds);

        // Cloning contents to spineContent
        Cloner cloner = new Cloner();
        List<Map<String, Object>> spineContents = cloner.deepClone(contents);
        List<Map<String, Object>> onlineContents = cloner.deepClone(contents);

        TelemetryManager.info("Initialising the ECAR variant Map For Content Id: " + node.getIdentifier());
        ContentBundle contentBundle = new ContentBundle();
        // ECARs Generation - START
        node.getMetadata().put(ContentWorkflowPipelineParams.variants.name(), new HashMap<String, Object>());
        TelemetryManager.log("Disabled full ECAR generation for collections. So not generating for collection id: " + node.getIdentifier());
        // TODO: START : Remove the below when mobile app is ready to accept Resources as Default in manifest
        List<String> nodeChildList = getList(node.getMetadata().get("childNodes"));
        if(CollectionUtils.isNotEmpty(nodeChildList))
            childrenIds = nodeChildList;
        // Generate spine ECAR.
        List<String> spineECARUrl = generateEcar(EcarPackageType.SPINE, node, contentBundle, spineContents, childrenIds, children);

        downloadUrl = spineECARUrl.get(IDX_S3_URL);
        s3Key = spineECARUrl.get(IDX_S3_KEY);
        generateEcar(EcarPackageType.ONLINE, node, contentBundle, onlineContents, childrenIds, children);
        node.getMetadata().remove("children");

        node.getMetadata().put(ContentWorkflowPipelineParams.s3Key.name(), s3Key);
        node.getMetadata().put(ContentWorkflowPipelineParams.downloadUrl.name(), downloadUrl);
        node.getMetadata().put(ContentWorkflowPipelineParams.size.name(), getCloudStorageFileSize(s3Key));
    }

    private Response    migrateContentImageObjectData(String contentId, Node contentImage) {
        Response response = new Response();
        if (null != contentImage && StringUtils.isNotBlank(contentId)) {
            String contentImageId = contentId + ContentConfigurationConstants.DEFAULT_CONTENT_IMAGE_OBJECT_SUFFIX;

            TelemetryManager.info("Fetching the Content Image Node for actual state . | [Content Id: " + contentImageId + "]");
            if(null == contentImage.getInRelations())
                contentImage.setInRelations(new ArrayList<>());
            if(null == contentImage.getOutRelations())
                contentImage.setOutRelations(new ArrayList<>());
            removeExtraProperties(contentImage);
            //}
            TelemetryManager.info("Migrating the Content Body. | [Content Id: " + contentId + "]");

            TelemetryManager.log("Migrating the Content Object Metadata. | [Content Id: " + contentId + "]");
            response = updateNode(contentImage);
            if (checkError(response)) {
                TelemetryManager.error(response.getParams().getErrmsg() + " :: " + response.getParams().getErr() + " :: " + response.getResult());
                throw new ServerException(ContentErrorCodeConstants.PUBLISH_ERROR.name(),
                        ContentErrorMessageConstants.CONTENT_IMAGE_MIGRATION_ERROR + " | [Content Id: " + contentId
                                + "]" + response.getParams().getErrmsg() + " :: " + response.getParams().getErr() + " :: " + response.getResult());
            }
        }

        TelemetryManager.log("Returning the Response Object After Migrating the Content Body and Metadata.");
        return response;
    }

    private void updateHierarchyMetadata(List<Map<String, Object>> children, Node node) {
        if(CollectionUtils.isNotEmpty(children)) {
            for(Map<String, Object> child : children) {
                if(StringUtils.equalsIgnoreCase("Parent",
                        (String)child.get("visibility"))){
                    //set child metadata -- compatibilityLevel, appIcon, posterImage, lastPublishedOn, pkgVersion, status
                    populatePublishMetadata(child, node);
                    updateHierarchyMetadata((List<Map<String,Object>>)child.get("children"), node);
                }
            }
        }
    }

    private void populatePublishMetadata(Map<String, Object> content, Node node) {
        content.put("compatibilityLevel", null != content.get("compatibilityLevel") ?
                ((Number) content.get("compatibilityLevel")).intValue() : 1);
        //TODO:  For appIcon, posterImage and screenshot createThumbNail method has to be implemented.
        content.put(ContentWorkflowPipelineParams.lastPublishedOn.name(), node.getMetadata().get(ContentWorkflowPipelineParams.lastPublishedOn.name()));
        content.put(ContentWorkflowPipelineParams.pkgVersion.name(), node.getMetadata().get(ContentWorkflowPipelineParams.pkgVersion.name()));
        content.put(ContentWorkflowPipelineParams.leafNodesCount.name(), getLeafNodeCount(content));
        content.put(ContentWorkflowPipelineParams.status.name(), node.getMetadata().get(ContentWorkflowPipelineParams.status.name()));
        content.put(ContentWorkflowPipelineParams.lastUpdatedOn.name(), node.getMetadata().get(ContentWorkflowPipelineParams.lastUpdatedOn.name()));
        content.put(ContentWorkflowPipelineParams.downloadUrl.name(), node.getMetadata().get(ContentWorkflowPipelineParams.downloadUrl.name()));
        content.put(ContentWorkflowPipelineParams.variants.name(), node.getMetadata().get(ContentWorkflowPipelineParams.variants.name()));
    }

    @SuppressWarnings("unchecked")
    private Integer getLeafNodeCount(Map<String, Object> data) {
        Set<String> leafNodeIds = new HashSet<>();
        getLeafNodesIds(data, leafNodeIds);
        return leafNodeIds.size();
    }

    private void getLeafNodesIds(Map<String, Object> data, Set<String> leafNodeIds) {
        List<Map<String,Object>> children = (List<Map<String,Object>>)data.get("children");
        if(CollectionUtils.isNotEmpty(children)) {
            for(Map<String, Object> child : children) {
                getLeafNodesIds(child, leafNodeIds);
            }
        } else {
            if (!StringUtils.equalsIgnoreCase(COLLECTION_MIMETYPE, (String) data.get(ContentAPIParams.mimeType.name()))) {
                leafNodeIds.add((String) data.get(ContentAPIParams.identifier.name()));
            }
        }
    }

    private void publishHierarchy(Node node, List<Map<String,Object>> childrenList) {
        Map<String, Object> hierarchy = getContentMap(node, childrenList);
        hierarchyStore.saveOrUpdateHierarchy(node.getIdentifier(), hierarchy);
    }
    private void syncNodes(List<Map<String, Object>> children, List<String> unitNodes) {
        DefinitionDTO definition = util.getDefinition(TAXONOMY_ID, ContentWorkflowPipelineParams.Content.name());
        if (null == definition) {
            TelemetryManager.error("Content definition is null.");
        }
        List<String> nodeIds = new ArrayList<>();
        List<Node> nodes = new ArrayList<>();
        getNodeForSyncing(children, nodes, nodeIds, definition);
        if(CollectionUtils.isNotEmpty(unitNodes))
            unitNodes.removeAll(nodeIds);

        if(CollectionUtils.isEmpty(nodes) && CollectionUtils.isEmpty(unitNodes))
            return;

        Map<String, String> errors;
        Map<String, Object> def =  mapper.convertValue(definition, new TypeReference<Map<String, Object>>() {});
        Map<String, String> relationMap = GraphUtil.getRelationMap(ContentWorkflowPipelineParams.Content.name(), def);
        if(CollectionUtils.isNotEmpty(nodes)) {
            while (!nodes.isEmpty()) {
                int currentBatchSize = (nodes.size() >= batchSize) ? batchSize : nodes.size();
                List<Node> nodeBatch = nodes.subList(0, currentBatchSize);

                if (CollectionUtils.isNotEmpty(nodeBatch)) {

                    errors = new HashMap<>();
                    Map<String, Object> messages = SyncMessageGenerator.getMessages(nodeBatch, ContentWorkflowPipelineParams.Content.name(), relationMap, errors);
                    if (!errors.isEmpty())
                        TelemetryManager.error("Error! while forming ES document data from nodes, below nodes are ignored: " + errors);
                    if(MapUtils.isNotEmpty(messages)) {
                        try {
                            System.out.println("Number of units to be synced : " + messages.size());
                            ElasticSearchUtil.bulkIndexWithIndexId(ES_INDEX_NAME, DOCUMENT_TYPE, messages);
                            System.out.println("UnitIds synced : " + messages.keySet());
                        } catch (Exception e) {
                            e.printStackTrace();
                            TelemetryManager.error("Elastic Search indexing failed: " + e);
                        }
                    }
                }
                // clear the already batched node ids from the list
                nodes.subList(0, currentBatchSize).clear();
            }
        }
        try {
            //Unindexing not utilized units
            if(CollectionUtils.isNotEmpty(unitNodes))
                ElasticSearchUtil.bulkDeleteDocumentById(ES_INDEX_NAME, DOCUMENT_TYPE, unitNodes);
        } catch (Exception e) {
            TelemetryManager.error("Elastic Search indexing failed: " + e);
        }
    }

    private void addResourceToCollection(Node node, List<Map<String, Object>> children) {
        List<Map<String, Object>> leafNodes = getLeafNodes(children, 1);
        if (CollectionUtils.isNotEmpty(leafNodes)) {
            List<Relation> relations = new ArrayList<>();
            for(Map<String, Object> leafNode : leafNodes) {

                String id = (String) leafNode.get("identifier");
                int index = 1;
                Number num = (Number) leafNode.get("index");
                if (num != null) {
                    index = num.intValue();
                }
                Relation rel = new Relation(node.getIdentifier(), RelationTypes.SEQUENCE_MEMBERSHIP.relationName(), id);
                Map<String, Object> metadata = new HashMap<>();
                metadata.put(SystemProperties.IL_SEQUENCE_INDEX.name(), index);
                metadata.put("depth", leafNode.get("depth"));
                rel.setMetadata(metadata);
                relations.add(rel);
            }
            List<Relation> existingRelations = node.getOutRelations();
            if (CollectionUtils.isNotEmpty(existingRelations)) {
                relations.addAll(existingRelations);
            }
            node.setOutRelations(relations);
        }

    }

    private Map<String, Object> getContentMap(Node node, List<Map<String,Object>> childrenList) {
        DefinitionDTO definition = util.getDefinition(TAXONOMY_ID, "Content");
        Map<String, Object> collectionHierarchy  = ConvertGraphNode.convertGraphNode(node, TAXONOMY_ID, definition, null);
        collectionHierarchy.put("children", childrenList);
        collectionHierarchy.put("identifier", node.getIdentifier());
        collectionHierarchy.put("objectType", node.getObjectType());
        return collectionHierarchy;
    }

    private double getTotalCompressedSize(Map<String, Object> data, double totalCompressed) {
        List<Map<String,Object>> children = (List<Map<String,Object>>) data.get("children");
        if(CollectionUtils.isNotEmpty(children)) {
            for(Map<String,Object> child : children ){
                if(!StringUtils.equals((String)child.get(ContentAPIParams.mimeType.name()), COLLECTION_MIMETYPE)
                        && StringUtils.equals((String) child.get(ContentAPIParams.visibility.name()),"Default")) {
                    if(null != child.get(ContentAPIParams.totalCompressedSize.name())) {
                        totalCompressed += ((Number) child.get(ContentAPIParams.totalCompressedSize.name())).doubleValue();
                    } else if(null != child.get(ContentAPIParams.size.name())) {
                        totalCompressed += ((Number) child.get(ContentAPIParams.size.name())).doubleValue();
                    }
                }
                totalCompressed = getTotalCompressedSize(child, totalCompressed);
            }
        }
        return totalCompressed;
    }

    private void updateLeafNodeIds(Node node, Map<String, Object> content) {
        Set<String> leafNodeIds = new HashSet<>();
        getLeafNodesIds(content, leafNodeIds);
        node.getMetadata().put(ContentAPIParams.leafNodes.name(), new ArrayList<>(leafNodeIds));
    }

    private List<String> getChildNode(Map<String, Object> data) {
        Set<String> childrenSet = new HashSet<>();
        getChildNode(data, childrenSet);
        return new ArrayList<>(childrenSet);
    }

    @SuppressWarnings("unchecked")
    private void getChildNode(Map<String, Object> data, Set<String> childrenSet) {
        List<Object> children = (List<Object>) data.get("children");
        if (null != children && !children.isEmpty()) {
            for (Object child : children) {
                Map<String, Object> childMap = (Map<String, Object>) child;
                childrenSet.add((String) childMap.get("identifier"));
                getChildNode(childMap, childrenSet);
            }
        }
    }

    private void getTypeCount(Map<String, Object> data, String type, Map<String, Object> typeMap) {
        List<Object> children = (List<Object>) data.get("children");
        if (null != children && !children.isEmpty()) {
            for (Object child : children) {
                Map<String, Object> childMap = (Map<String, Object>) child;
                String typeValue = childMap.get(type).toString();
                if (typeMap.containsKey(typeValue)) {
                    int count = (int) typeMap.get(typeValue);
                    count++;
                    typeMap.put(typeValue, count);
                } else {
                    typeMap.put(typeValue, 1);
                }
                if (childMap.containsKey("children")) {
                    getTypeCount(childMap, type, typeMap);
                }
            }
        }

    }

    public String generateTOC(Node node, Map<String, Object> content) {
        TelemetryManager.info("Write hirerachy to JSON File :" + node.getIdentifier());
        String url = null;
        String data = null;
        File file = new File(getTOCBasePath(node.getIdentifier()) + "_toc.json");

        try {
            data = mapper.writeValueAsString(content);
            FileUtils.writeStringToFile(file, data);
            if (file.exists()) {
                TelemetryManager.info("Upload File to cloud storage :" + file.getName());
                String[] uploadedFileUrl = CloudStore.uploadFile(getAWSPath(node.getIdentifier()), file, true);
                if (null != uploadedFileUrl && uploadedFileUrl.length > 1) {
                    url = uploadedFileUrl[IDX_S3_URL];
                    TelemetryManager.info("Update cloud storage url to node" + url);
                }
            }
        } catch(JsonProcessingException e) {
            TelemetryManager.error("Error while parsing map object to string.", e);
        }catch (Exception e) {
            TelemetryManager.error("Error while uploading file ", e);
        }finally {
            try {
                TelemetryManager.info("Deleting Uploaded files");
                FileUtils.deleteDirectory(file.getParentFile());
            } catch (IOException e) {
                TelemetryManager.error("Error while deleting file ", e);
            }
        }
        return url;
    }

    private String convertToString(Object obj) throws Exception {
        return mapper.writeValueAsString(obj);
    }

    // TODO: rewrite this specific code
    private void updateRootChildrenList(Node node, List<Map<String, Object>> nextLevelNodes) {
        List<Map<String, Object>> childrenMap = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(nextLevelNodes)) {
            for (Map<String, Object> nextLevelNode: nextLevelNodes) {
                childrenMap.add(new HashMap<String, Object>() {{
                    put("identifier", nextLevelNode.get("identifier"));
                    put("name", nextLevelNode.get("name"));
                    put("objectType", "Content");
                    put("description", nextLevelNode.get("description"));
                    put("index", nextLevelNode.get("index"));
                }});
            }
        }
        node.getMetadata().put("children", childrenMap);
    }

    private void getNodeMap(List<Map<String, Object>> children, List<Node> nodes, List<String> nodeIds, DefinitionDTO definition) {
        if (CollectionUtils.isNotEmpty(children)) {
            children.stream().forEach(child -> {
                Node node = null;
                try {
                    if(StringUtils.equalsIgnoreCase("Default", (String) child.get("visibility"))) {
                        node = util.getNode(ContentWorkflowPipelineParams.domain.name(), (String)child.get("identifier"));//getContentNode(TAXONOMY_ID, (String) child.get("identifier"), null);
                        node.getMetadata().remove("children");
                        Map<String, Object> childData = new HashMap<>();
                        childData.putAll(child);
                        List<Map<String, Object>> nextLevelNodes = (List<Map<String, Object>>) childData.get("children");
                        List<Map<String, Object>> finalChildList = new ArrayList<>();
                        if (CollectionUtils.isNotEmpty(nextLevelNodes)) {
                            finalChildList = nextLevelNodes.stream().map(nextLevelNode -> {
                                Map<String, Object> metadata = new HashMap<String, Object>() {{
                                    put("identifier", nextLevelNode.get("identifier"));
                                    put("name", nextLevelNode.get("name"));
                                    put("objectType", "Content");
                                    put("description", nextLevelNode.get("description"));
                                    put("index", nextLevelNode.get("index"));
                                }};
                                return metadata;
                            }).collect(Collectors.toList());
                        }
                        node.getMetadata().put("children", finalChildList);

                    }else {
                        Map<String, Object> childData = new HashMap<>();
                        childData.putAll(child);
                        List<Map<String, Object>> nextLevelNodes = (List<Map<String, Object>>) childData.get("children");
                        childData.remove("children");
                        node = ConvertToGraphNode.convertToGraphNode(childData, definition, null);
                        List<Map<String, Object>> finalChildList = new ArrayList<>();
                        if (CollectionUtils.isNotEmpty(nextLevelNodes)) {
                            finalChildList = nextLevelNodes.stream().map(nextLevelNode -> {
                                Map<String, Object> metadata = new HashMap<String, Object>() {{
                                    put("identifier", nextLevelNode.get("identifier"));
                                    put("name", nextLevelNode.get("name"));
                                    put("objectType", "Content");
                                    put("description", nextLevelNode.get("description"));
                                    put("index", nextLevelNode.get("index"));
                                }};
                                return metadata;
                            }).collect(Collectors.toList());
                        }
                        node.getMetadata().put("children", finalChildList);
                        if(StringUtils.isBlank(node.getObjectType()))
                            node.setObjectType(ContentWorkflowPipelineParams.Content.name());
                        if(StringUtils.isBlank(node.getGraphId()))
                            node.setGraphId(ContentWorkflowPipelineParams.domain.name());
                    }
                    if(!nodeIds.contains(node.getIdentifier())) {
                        nodes.add(node);
                        nodeIds.add(node.getIdentifier());
                    }
                } catch (Exception e) {
                    TelemetryManager.error("Error while generating node map. ", e);
                }
                getNodeMap((List<Map<String, Object>>) child.get("children"), nodes, nodeIds, definition);
            });
        }
    }

    private List<String> generateEcar(EcarPackageType pkgType, Node node, ContentBundle contentBundle,
                                      List<Map<String, Object>> ecarContents, List<String> childrenIds, List<Map<String, Object>> children) {

        Map<Object, List<String>> downloadUrls = null;
        TelemetryManager.log("Creating " + pkgType.toString() + " ECAR For Content Id: " + node.getIdentifier());
        String bundleFileName = getBundleFileName(contentId, node, pkgType);
        downloadUrls = contentBundle.createContentManifestData(ecarContents, childrenIds, null,
                pkgType);

        List<String> ecarUrl = Arrays.asList(contentBundle.createContentBundle(ecarContents, bundleFileName,
                ContentConfigurationConstants.DEFAULT_CONTENT_MANIFEST_VERSION, downloadUrls, node, children));
        TelemetryManager.log(pkgType.toString() + " ECAR created For Content Id: " + node.getIdentifier());

        if (!EcarPackageType.FULL.name().equalsIgnoreCase(pkgType.toString())) {
            Map<String, Object> ecarMap = new HashMap<>();
            ecarMap.put(ContentWorkflowPipelineParams.ecarUrl.name(), ecarUrl.get(IDX_S3_URL));
            ecarMap.put(ContentWorkflowPipelineParams.size.name(), getCloudStorageFileSize(ecarUrl.get(IDX_S3_KEY)));

            TelemetryManager.log("Adding " + pkgType.toString() + " Ecar Information to Variants Map For Content Id: " + node.getIdentifier());
            ((Map<String, Object>) node.getMetadata().get(ContentWorkflowPipelineParams.variants.name())).put(pkgType.toString().toLowerCase(), ecarMap);

        }
        return ecarUrl;
    }

    private void removeExtraProperties(Node imgNode) {
        Response originalResponse = getDataNode(TAXONOMY_ID, imgNode.getIdentifier());
        if (checkError(originalResponse))
            throw new ClientException(TaxonomyErrorCodes.ERR_TAXONOMY_INVALID_CONTENT.name(),
                    "Error! While Fetching the Content for Operation | [Content Id: " + imgNode.getIdentifier() + "]");
        Node originalNode = (Node)originalResponse.get(GraphDACParams.node.name());

        Set<String> originalNodeMetaDataSet = originalNode.getMetadata().keySet();
        Set<String> imgNodeMetaDataSet = imgNode.getMetadata().keySet();
        Set<String> extraMetadata = originalNodeMetaDataSet.stream().filter(element -> !imgNodeMetaDataSet.contains(element)).collect(Collectors.toSet());

        if(!extraMetadata.isEmpty()) {
            for(String prop : extraMetadata) {
                imgNode.getMetadata().put(prop, null);
            }
        }
    }

    private void getNodeForSyncing(List<Map<String, Object>> children, List<Node> nodes, List<String> nodeIds, DefinitionDTO definition) {
        List<String> relationshipProperties = getRelationList(definition);
        if (CollectionUtils.isNotEmpty(children)) {
            children.stream().forEach(child -> {
                try {
                    if(StringUtils.equalsIgnoreCase("Parent", (String) child.get("visibility"))) {
                        Map<String, Object> childData = new HashMap<>();
                        childData.putAll(child);
                        Map<String, Object> relationProperties = new HashMap<>();
                        for(String property : relationshipProperties) {
                            if(childData.containsKey(property)) {
                                relationProperties.put(property, (List<Map<String, Object>>) childData.get(property));
                                childData.remove(property);
                            }
                        }
                        Node node = ConvertToGraphNode.convertToGraphNode(childData, definition, null);
                        if(MapUtils.isNotEmpty(relationProperties)) {
                            for(String key : relationProperties.keySet()) {
                                List<String> finalPropertyList = null;
                                List<Map<String, Object>> properties = (List<Map<String, Object>>)relationProperties.get(key);
                                if (CollectionUtils.isNotEmpty(properties)) {
                                    finalPropertyList = properties.stream().map(property -> {
                                        String identifier = (String)property.get("identifier");
                                        return identifier;
                                    }).collect(Collectors.toList());
                                }
                                if(CollectionUtils.isNotEmpty(finalPropertyList))
                                    node.getMetadata().put(key, finalPropertyList);
                            }
                        }
                        if(StringUtils.isBlank(node.getObjectType()))
                            node.setObjectType(ContentWorkflowPipelineParams.Content.name());
                        if(StringUtils.isBlank(node.getGraphId()))
                            node.setGraphId(ContentWorkflowPipelineParams.domain.name());
                        if(!nodeIds.contains(node.getIdentifier())) {
                            nodes.add(node);
                            nodeIds.add(node.getIdentifier());
                        }
                        getNodeForSyncing((List<Map<String, Object>>) child.get("children"), nodes, nodeIds, definition);
                    }

                } catch (Exception e) {
                    TelemetryManager.error("Error while fetching unit nodes for syncing. ", e);
                }

            });
        }
    }

    private List<Map<String, Object>> getLeafNodes(List<Map<String, Object>> children, int depth) {
        List<Map<String, Object>> leafNodes = new ArrayList<>();
        if(CollectionUtils.isNotEmpty(children)) {
            int index = 1;
            for(Map<String, Object> child : children) {
                String visibility = (String) child.get(ContentWorkflowPipelineParams.visibility.name());
                if(StringUtils.equalsIgnoreCase(visibility, ContentWorkflowPipelineParams.Parent.name())) {
                    List<Map<String,Object>> nextChildren = (List<Map<String,Object>>)child.get("children");
                    int nextDepth = depth + 1;
                    List<Map<String, Object>> nextLevelLeafNodes = getLeafNodes(nextChildren, nextDepth);
                    leafNodes.addAll(nextLevelLeafNodes);
                }else {
                    Map<String, Object> leafNode = new HashMap<>(child);
                    leafNode.put("index", index);
                    leafNode.put("depth", depth);
                    leafNodes.add(leafNode);
                    index++;
                }
            }
        }
        return leafNodes;
    }
}
