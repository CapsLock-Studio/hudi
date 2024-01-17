/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources.debezium;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.RowSource;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;

/**
 * Base class for Debezium streaming source which expects change events as Kafka Avro records.
 * Obtains the schema from the confluent schema-registry.
 */
public abstract class DebeziumJSONSource extends RowSource {

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumSource.class);
  // these are native kafka's config. do not change the config names.
  private static final String NATIVE_KAFKA_KEY_DESERIALIZER_PROP = "key.deserializer";
  private static final String NATIVE_KAFKA_VALUE_DESERIALIZER_PROP = "value.deserializer";
  private static final String OVERRIDE_CHECKPOINT_STRING = "hoodie.debezium.override.initial.checkpoint.key";
  private static StructType schema;

  private final KafkaOffsetGen offsetGen;
  private final HoodieIngestionMetrics metrics;

  public DebeziumJSONSource(TypedProperties props, JavaSparkContext sparkContext,
                            SparkSession sparkSession,
                            SchemaProvider schemaProvider,
                            HoodieIngestionMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider);

    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class.getName());
    props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, StringDeserializer.class.getName());

    // Pass desearializer class with string for KafkaConsumer.
    props.put("hoodie.deltastreamer.source.kafka.value.deserializer.class", StringDeserializer.class.getName());

    offsetGen = new KafkaOffsetGen(props);
    this.metrics = metrics;
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    String overrideCheckpointStr = props.getString(OVERRIDE_CHECKPOINT_STRING, "");

    OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCkptStr, sourceLimit, metrics);
    long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
    LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());

    if (totalNewMsgs == 0) {
      // If there are no new messages, use empty dataframe with no schema. This is because the schema from schema registry can only be considered
      // up to date if a change event has occurred.
      return Pair.of(Option.of(sparkSession.emptyDataFrame()), overrideCheckpointStr.isEmpty() ? CheckpointUtils.offsetsToStr(offsetRanges) : overrideCheckpointStr);
    } else {
      Dataset<Row> dataset = toDataset(offsetRanges, offsetGen);
      LOG.info(String.format("Spark schema of Kafka Payload for topic %s:\n%s", offsetGen.getTopicName(), dataset.schema().treeString()));
      LOG.info(String.format("New checkpoint string: %s", CheckpointUtils.offsetsToStr(offsetRanges)));
      return Pair.of(Option.of(dataset), overrideCheckpointStr.isEmpty() ? CheckpointUtils.offsetsToStr(offsetRanges) : overrideCheckpointStr);
    }
  }

  /**
   * Debezium Kafka Payload has a nested structure, flatten it specific to the Database type.
   * @param rawKafkaData Dataset of the Debezium CDC event from the kafka
   * @return A flattened dataset.
   */
  protected abstract Dataset<Row> processDataset(Dataset<Row> rawKafkaData);

  /**
   * Converts a Kafka Topic offset into a Spark dataset.
   *
   * @param offsetRanges Offset ranges
   * @param offsetGen    KafkaOffsetGen
   * @return Spark dataset
   */
  private Dataset<Row> toDataset(OffsetRange[] offsetRanges, KafkaOffsetGen offsetGen) {
    Dataset<Row> kafkaData;

    // Using JSON format must be with config "key.converter.schemas.enable: false" and "value.converter.schemas.enable: false"
    // Without schema, there is no need to get JSON object from payload.
    RDD<String> rdd = KafkaUtils.createRDD(
          sparkContext,
            offsetGen.getKafkaParams(),
            offsetRanges,
            LocationStrategies.PreferConsistent()
        )
        .filter(x -> !StringUtils.isNullOrEmpty((String) x.value()))
        .map(x -> x.value().toString())
        .rdd();

    if (rdd.isEmpty()) {
      kafkaData = sparkSession.emptyDataFrame();
    } else {
      Dataset<String> jsonDataset = sparkSession.createDataset(rdd, Encoders.STRING());
      kafkaData = sparkSession.read().json(jsonDataset);
    }

    // Check and cast the fields we wanna used for selectExpr since it will throw error when type is not struct.
    kafkaData = convertStructColumns(kafkaData);

    // Flatten debezium payload, specific to each DB type (postgres/ mysql/ etc..)
    Dataset<Row> debeziumDataset = processDataset(kafkaData);

    // Some required transformations to ensure debezium data types are converted to spark supported types.
    return convertArrayColumnsToString(convertColumnToNullable(sparkSession, debeziumDataset));
  }

  private static boolean isFieldStructType(StructType schema, String fieldName) {
    try {
      return (schema.apply(fieldName).dataType() instanceof StructType);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static Dataset<Row> convertStructColumns(Dataset<Row> dataset) {
    schema = dataset.schema();
    String afterField = DebeziumConstants.INCOMING_AFTER_FIELD;
    String beforeField = DebeziumConstants.INCOMING_BEFORE_FIELD;

    boolean isAfterFieldStruct = isFieldStructType(schema, afterField);
    boolean isBeforeFieldStruct = isFieldStructType(schema, beforeField);

    // Make sure the schema has the same schema for union data.
    if (!isAfterFieldStruct && isBeforeFieldStruct) {
      dataset = dataset.withColumn(afterField, functions.lit(null).cast(schema.apply(beforeField).dataType()));
    } else if (isAfterFieldStruct && !isBeforeFieldStruct) {
      dataset = dataset.withColumn(beforeField, functions.lit(null).cast(schema.apply(afterField).dataType()));
    } else {
      dataset = dataset.withColumn(beforeField, functions.lit(null).cast(new StructType()));
      dataset = dataset.withColumn(afterField, functions.lit(null).cast(new StructType()));
    }

    return dataset;
  }

  /**
   * Utility function for converting columns to nullable. This is useful when required to make a column nullable to match a nullable column from Debezium change
   * events.
   *
   * @param sparkSession SparkSession object
   * @param dataset      Dataframe to modify
   * @return Modified dataframe
   */
  private static Dataset<Row> convertColumnToNullable(SparkSession sparkSession, Dataset<Row> dataset) {
    List<String> columns = Arrays.asList(dataset.columns());
    StructField[] modifiedStructFields = Arrays.stream(dataset.schema().fields()).map(field -> columns
        .contains(field.name()) ? new StructField(field.name(), field.dataType(), true, field.metadata()) : field)
        .toArray(StructField[]::new);

    return sparkSession.createDataFrame(dataset.rdd(), new StructType(modifiedStructFields));
  }

  /**
   * Converts Array types to String types because not all Debezium array columns are supported to be converted
   * to Spark array columns.
   *
   * @param dataset Dataframe to modify
   * @return Modified dataframe
   */
  private static Dataset<Row> convertArrayColumnsToString(Dataset<Row> dataset) {
    List<String> arrayColumns = Arrays.stream(dataset.schema().fields())
        .filter(field -> field.dataType().typeName().toLowerCase().startsWith("array"))
        .map(StructField::name)
        .collect(Collectors.toList());

    for (String colName : arrayColumns) {
      dataset = dataset.withColumn(colName, functions.col(colName).cast(DataTypes.StringType));
    }

    return dataset;
  }

  @Override
  public void onCommit(String lastCkptStr) {
    if (getBooleanWithAltKeys(this.props, KafkaSourceConfig.ENABLE_KAFKA_COMMIT_OFFSET)) {
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }
}

