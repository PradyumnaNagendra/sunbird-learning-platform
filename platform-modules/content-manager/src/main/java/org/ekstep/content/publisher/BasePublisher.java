package org.ekstep.content.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.ekstep.common.Platform;
import org.ekstep.common.Slug;
import org.ekstep.common.util.S3PropertyReader;
import org.ekstep.content.common.EcarPackageType;
import org.ekstep.content.enums.ContentWorkflowPipelineParams;
import org.ekstep.content.operation.finalizer.BaseFinalizer;
import org.ekstep.content.util.ContentPackageExtractionUtil;
import org.ekstep.graph.dac.model.Node;
import org.ekstep.graph.model.node.DefinitionDTO;
import org.ekstep.learning.common.enums.ContentAPIParams;
import org.ekstep.learning.hierarchy.store.HierarchyStore;
import org.ekstep.learning.util.ControllerUtil;
import org.ekstep.telemetry.logger.TelemetryManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BasePublisher extends BaseFinalizer {
    protected static final int IDX_S3_KEY = 0;
    /** The BasePath. */
    protected String basePath;

    /** The CollectionId. */
    protected String contentId;

    protected static ObjectMapper mapper = new ObjectMapper();
    protected HierarchyStore hierarchyStore = new HierarchyStore();
    protected ControllerUtil util = new ControllerUtil();
    protected static final String COLLECTION_CACHE_KEY_PREFIX = "hierarchy_";
    protected static final List<String> LEVEL4_CONTENT_TYPES = Arrays.asList("Course","CourseUnit","LessonPlan","LessonPlanUnit");
    protected static ContentPackageExtractionUtil contentPackageExtractionUtil = new ContentPackageExtractionUtil();
    protected static final String COLLECTION_MIMETYPE = "application/vnd.ekstep.content-collection";
    protected static final List<String> PUBLISHED_STATUS_LIST = Arrays.asList("Live", "Unlisted");
    protected static int batchSize = 50;

    private static final String CONTENT_FOLDER = Platform.config.getString("cloud_storage.content.folder");
    private static final String ARTEFACT_FOLDER = Platform.config.getString("cloud_storage.artefact.folder");


    protected String getAWSPath(String identifier) {
        String folderName = S3PropertyReader.getProperty(CONTENT_FOLDER);
        if (!StringUtils.isBlank(CONTENT_FOLDER)) {
            folderName = CONTENT_FOLDER + File.separator + Slug.makeSlug(identifier, true) + File.separator
                    + ARTEFACT_FOLDER;
        }
        return folderName;
    }

    protected String getTOCBasePath(String contentId) {
        String path = "";
        if (!StringUtils.isBlank(contentId))
            //TODO: Get the configuration of tmp file location from publish Pipeline
            path = "/tmp" + File.separator + System.currentTimeMillis()
                    + ContentAPIParams._temp.name() + File.separator + contentId;
        return path;
    }

    protected String getBundleFileName(String contentId, Node node, EcarPackageType packageType) {
        TelemetryManager.info("Generating Bundle File Name For ECAR Package Type: " + packageType.name());
        String fileName = "";
        if (null != node && null != node.getMetadata() && null != packageType) {
            String suffix = "";
            if (packageType != EcarPackageType.FULL)
                suffix = "_" + packageType.name();
            fileName = Slug.makeSlug((String) node.getMetadata().get(ContentWorkflowPipelineParams.name.name()), true)
                    + "_" + System.currentTimeMillis() + "_" + contentId + "_"
                    + node.getMetadata().get(ContentWorkflowPipelineParams.pkgVersion.name()) + suffix + ".ecar";
        }
        return fileName;
    }

    protected List<String> getRelationList(DefinitionDTO definition){
        List<String> relationshipProperties = new ArrayList<>();
        relationshipProperties.addAll(definition.getInRelations().stream().map(rel -> rel.getTitle()).collect(Collectors.toList()));
        relationshipProperties.addAll(definition.getOutRelations().stream().map(rel -> rel.getTitle()).collect(Collectors.toList()));
        return relationshipProperties;
    }

}
