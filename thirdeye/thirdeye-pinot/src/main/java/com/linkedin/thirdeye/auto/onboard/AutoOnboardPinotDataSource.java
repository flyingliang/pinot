package com.linkedin.thirdeye.auto.onboard;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean.DimensionAsMetricProperties;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.DataSourceConfig;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a service to onboard datasets automatically to thirdeye from pinot
 * The run method is invoked periodically by the AutoOnboardService, and it checks for new tables in pinot, to add to thirdeye
 * It also looks for any changes in dimensions or metrics to the existing tables
 */
public class AutoOnboardPinotDataSource extends AutoOnboard {
  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardPinotDataSource.class);


  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private AutoOnboardPinotMetricsUtils autoLoadPinotMetricsUtils;

  public AutoOnboardPinotDataSource(DataSourceConfig dataSourceConfig) {
    super(dataSourceConfig);
    autoLoadPinotMetricsUtils = new AutoOnboardPinotMetricsUtils(dataSourceConfig);
    LOG.info("Created {}", AutoOnboardPinotDataSource.class.getName());
  }

  public AutoOnboardPinotDataSource(DataSourceConfig dataSourceConfig, AutoOnboardPinotMetricsUtils utils) {
    super(dataSourceConfig);
    autoLoadPinotMetricsUtils = utils;
  }

  public void run() {
    LOG.info("Running auto load for {}", AutoOnboardPinotDataSource.class.getSimpleName());
    try {
      List<String> allDatasets = new ArrayList<>();
      Map<String, Schema> allSchemas = new HashMap<>();
      loadDatasets(allDatasets, allSchemas);
      LOG.info("Checking all datasets");
      for (String dataset : allDatasets) {
        LOG.info("Checking dataset {}", dataset);

        Schema schema = allSchemas.get(dataset);
        DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
        addPinotDataset(dataset, schema, datasetConfig);
      }
    } catch (Exception e) {
      LOG.error("Exception in loading datasets", e);
    }
  }

  /**
   * Adds a dataset to the thirdeye database
   * @param dataset
   * @param schema
   * @param datasetConfig
   */
  public void addPinotDataset(String dataset, Schema schema, DatasetConfigDTO datasetConfig) throws Exception {
    if (datasetConfig == null) {
      LOG.info("Dataset {} is new, adding it to thirdeye", dataset);
      addNewDataset(dataset, schema);
    } else {
      LOG.info("Dataset {} already exists, checking for updates", dataset);
      refreshOldDataset(dataset, datasetConfig, schema);
    }
  }

  /**
   * Adds a new dataset to the thirdeye database
   * @param dataset
   * @param schema
   */
  private void addNewDataset(String dataset, Schema schema) throws Exception {
    List<MetricFieldSpec> metricSpecs = schema.getMetricFieldSpecs();

    // Create DatasetConfig
    DatasetConfigDTO datasetConfigDTO = ConfigGenerator.generateDatasetConfig(dataset, schema);
    LOG.info("Creating dataset for {}", dataset);
    DAO_REGISTRY.getDatasetConfigDAO().save(datasetConfigDTO);

    // Create MetricConfig
    for (MetricFieldSpec metricFieldSpec : metricSpecs) {
      MetricConfigDTO metricConfigDTO = ConfigGenerator.generateMetricConfig(metricFieldSpec, dataset);
      LOG.info("Creating metric {} for {}", metricConfigDTO.getName(), dataset);
      DAO_REGISTRY.getMetricConfigDAO().save(metricConfigDTO);
    }

    // Create Default DashboardConfig
    List<Long> metricIds = ConfigGenerator.getMetricIdsFromMetricConfigs(DAO_REGISTRY.getMetricConfigDAO().findByDataset(dataset));
    DashboardConfigDTO dashboardConfigDTO = ConfigGenerator.generateDefaultDashboardConfig(dataset, metricIds);
    LOG.info("Creating default dashboard for dataset {}", dataset);
    DAO_REGISTRY.getDashboardConfigDAO().save(dashboardConfigDTO);
  }

  /**
   * Refreshes an existing dataset in the thirdeye database
   * with any dimension/metric changes from pinot schema
   * @param dataset
   * @param datasetConfig
   * @param schema
   */
  private void refreshOldDataset(String dataset, DatasetConfigDTO datasetConfig, Schema schema) throws Exception {
    checkDimensionChanges(dataset, datasetConfig, schema);
    checkMetricChanges(dataset, datasetConfig, schema);
  }



  private void checkDimensionChanges(String dataset, DatasetConfigDTO datasetConfig, Schema schema) {

    LOG.info("Checking for dimensions changes in {}", dataset);
    List<String> schemaDimensions = schema.getDimensionNames();
    List<String> datasetDimensions = datasetConfig.getDimensions();

    // in dimensionAsMetric case, the dimension name will be used in the METRIC_NAMES_COLUMNS property of the metric
    List<String> dimensionsAsMetrics = new ArrayList<>();
    List<MetricConfigDTO> metricConfigs = DAO_REGISTRY.getMetricConfigDAO().findByDataset(dataset);
    for (MetricConfigDTO metricConfig : metricConfigs) {
      if (metricConfig.isDimensionAsMetric()) {
        Map<String, String> metricProperties = metricConfig.getMetricProperties();
        if (MapUtils.isNotEmpty(metricProperties)) {
          String metricNames = metricProperties.get(DimensionAsMetricProperties.METRIC_NAMES_COLUMNS.toString());
          if (StringUtils.isNotBlank(metricNames)) {
            dimensionsAsMetrics.addAll(Lists.newArrayList(metricNames.split(MetricConfigBean.METRIC_PROPERTIES_SEPARATOR)));
          }
        }
      }
    }
    List<String> dimensionsToAdd = new ArrayList<>();
    List<String> dimensionsToRemove = new ArrayList<>();

    // dimensions which are new in the pinot schema
    for (String dimensionName : schemaDimensions) {
      if (!datasetDimensions.contains(dimensionName) && !dimensionsAsMetrics.contains(dimensionName)) {
        dimensionsToAdd.add(dimensionName);
      }
    }
    // dimensions which are removed from pinot schema
    for (String dimensionName : datasetDimensions) {
      if (!schemaDimensions.contains(dimensionName)) {
        dimensionsToRemove.add(dimensionName);
      }
    }
    if (CollectionUtils.isNotEmpty(dimensionsToAdd) || CollectionUtils.isNotEmpty(dimensionsToRemove)) {
      datasetDimensions.addAll(dimensionsToAdd);
      datasetDimensions.removeAll(dimensionsToRemove);
      datasetConfig.setDimensions(datasetDimensions);

      if (!datasetConfig.isAdditive()
          && CollectionUtils.isNotEmpty(datasetConfig.getDimensionsHaveNoPreAggregation())) {
        List<String> dimensionsHaveNoPreAggregation = datasetConfig.getDimensionsHaveNoPreAggregation();
        dimensionsHaveNoPreAggregation.removeAll(dimensionsToRemove);
        datasetConfig.setDimensionsHaveNoPreAggregation(dimensionsHaveNoPreAggregation);
      }
      LOG.info("Added dimensions {}, removed {}", dimensionsToAdd, dimensionsToRemove);
      DAO_REGISTRY.getDatasetConfigDAO().update(datasetConfig);
    }
  }

  private void checkMetricChanges(String dataset, DatasetConfigDTO datasetConfig, Schema schema) {

    LOG.info("Checking for metric changes in {}", dataset);
    List<MetricFieldSpec> schemaMetricSpecs = schema.getMetricFieldSpecs();
    List<MetricConfigDTO> datasetMetricConfigs = DAO_REGISTRY.getMetricConfigDAO().findByDataset(dataset);
    Set<String> datasetMetricNames = new HashSet<>();
    for (MetricConfigDTO metricConfig : datasetMetricConfigs) {
      // In dimensionAsMetric case, the metric name will be used in the METRIC_VALUES_COLUMN property of the metric
      if (metricConfig.isDimensionAsMetric()) {
        Map<String, String> metricProperties = metricConfig.getMetricProperties();
        if (MapUtils.isNotEmpty(metricProperties)) {
          String metricValuesColumn = metricProperties.get(DimensionAsMetricProperties.METRIC_VALUES_COLUMN.toString());
          datasetMetricNames.add(metricValuesColumn);
        }
      } else {
        datasetMetricNames.add(metricConfig.getName());
      }
    }
    List<Long> metricsToAdd = new ArrayList<>();

    for (MetricFieldSpec metricSpec : schemaMetricSpecs) {
      // metrics which are new in pinot schema, create them
      String metricName = metricSpec.getName();
      if (!datasetMetricNames.contains(metricName)) {
        MetricConfigDTO metricConfigDTO = ConfigGenerator.generateMetricConfig(metricSpec, dataset);
        LOG.info("Creating metric {} for {}", metricName, dataset);
        metricsToAdd.add(DAO_REGISTRY.getMetricConfigDAO().save(metricConfigDTO));
      }
    }

    // add new metricIds to default dashboard
    if (CollectionUtils.isNotEmpty(metricsToAdd)) {
      LOG.info("Metrics to add {}", metricsToAdd);
      String dashboardName = ThirdEyeUtils.getDefaultDashboardName(dataset);
      DashboardConfigDTO dashboardConfig = DAO_REGISTRY.getDashboardConfigDAO().findByName(dashboardName);
      List<Long> metricIds = dashboardConfig.getMetricIds();
      metricIds.addAll(metricsToAdd);
      DAO_REGISTRY.getDashboardConfigDAO().update(dashboardConfig);
    }

    // TODO: write a tool, which given a metric id, erases all traces of that metric from the database
    // This will include:
    // 1) delete the metric from metricConfigs
    // 2) remove any derived metrics which use the deleted metric
    // 3) remove the metric, and derived metrics from all dashboards
    // 4) remove any anomaly functions associated with the metric
    // 5) remove any alerts associated with these anomaly functions

  }

  /**
   * Reads all table names in pinot, and loads their schema
   * @param allSchemas
   * @param allDatasets
   * @throws IOException
   */
  private void loadDatasets(List<String> allDatasets, Map<String, Schema> allSchemas) throws IOException {

    JsonNode tables = autoLoadPinotMetricsUtils.getAllTablesFromPinot();
    LOG.info("Getting all schemas");
    for (JsonNode table : tables) {
      String dataset = table.asText();
      Schema schema = autoLoadPinotMetricsUtils.getSchemaFromPinot(dataset);
      if (schema != null) {
        if (!autoLoadPinotMetricsUtils.verifySchemaCorrectness(schema)) {
          LOG.info("Skipping {} due to incorrect schema", dataset);
        } else {
          allDatasets.add(dataset);
          allSchemas.put(dataset, schema);
        }
      }
    }
  }


  @Override
  public void runAdhoc() {
    LOG.info("Triggering adhoc run for AutoOnboard Pinot data source");
    run();
  }

}
