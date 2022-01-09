/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.omeregeozkaya.boun;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.StreamSupport;

public class PubSubToBigQueryIotDataPipeline {
    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubToBigQueryIotDataPipeline.class);
    private static final String FIELD_NAME_TIMESTAMP = "timestamp";
    private static final String FIELD_NAME_DEVICE = "device";
    private static final String FIELD_NAME_TEMPERATURE = "temperature";
    private static final String FIELD_NAME_LOCATION = "location";
    private static final String FIELD_NAME_AVG_TEMPERATURE = "avg_temperature_of_the_last_minute_by_location";
    private static final String FIELD_NAME_UUID = "uuid";

    public static void main(String[] args) {
        PubSubBigQueryGCStorageOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubBigQueryGCStorageOptions.class);

        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

//        pipeline
//            .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
//            .apply(MapElements.via(
//                new SimpleFunction<String, String>() {
//                    @Override
//                    public String apply(String input) {
//                        long targetTime = System.currentTimeMillis() + 120 * 1000;
//                        int x = 0;
//                        while (System.currentTimeMillis() < targetTime) {
//                            x = x + (x%2 == 0 ? -1 : 1);
//                        }
//                        return input;
//                    }
//                }
//            ))
//            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))))
//            .apply("Write files to GCS", new WriteOneFilePerWindow(options.getOutput(), 1));

//        PCollection<String> messages = pipeline.apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()));


        TestStream<String> createEvents = TestStream.create(StringUtf8Coder.of())
            .addElements(
                TimestampedValue.of("{\"timestamp\": 20210113, \"device\": \"swe590-sensor-1\", \"temperature\": 10}", Instant.now()),
                TimestampedValue.of("{\"timestamp\": 20210114, \"device\": \"swe590-sensor-1\", \"temperature\": 15}", Instant.now().plus(Duration.standardSeconds(10L))),
                TimestampedValue.of("{\"timestamp\": 20210115, \"device\": \"swe590-sensor-1\", \"temperature\": 20}", Instant.now().plus(Duration.standardSeconds(20L))),
                TimestampedValue.of("{\"timestamp\": 20210116, \"device\": \"swe590-sensor-1\", \"temperature\": 25}", Instant.now().plus(Duration.standardSeconds(30L))),
                TimestampedValue.of("{\"timestamp\": 20210117, \"device\": \"swe590-sensor-1\", \"temperature\": 30}", Instant.now().plus(Duration.standardSeconds(40L))),
                TimestampedValue.of("{\"timestamp\": 20210118, \"device\": \"swe590-sensor-1\", \"temperature\": 35}", Instant.now().plus(Duration.standardSeconds(50L))),
                TimestampedValue.of("{\"timestamp\": 20220119, \"device\": \"swe590-sensor-1\", \"temperature\": 40}", Instant.now().plus(Duration.standardSeconds(60L))),
                TimestampedValue.of("{\"timestamp\": 20220120, \"device\": \"swe590-sensor-1\", \"temperature\": 45}", Instant.now().plus(Duration.standardSeconds(70L))),
                TimestampedValue.of("{\"timestamp\": 20220121, \"device\": \"swe590-sensor-1\", \"temperature\": 50}", Instant.now().plus(Duration.standardSeconds(80L))),
                TimestampedValue.of("{\"timestamp\": 20220122, \"device\": \"swe590-sensor-1\", \"temperature\": 55}", Instant.now().plus(Duration.standardSeconds(90L))),
                TimestampedValue.of("{\"timestamp\": 20220123, \"device\": \"swe590-sensor-1\", \"temperature\": 60}", Instant.now().plus(Duration.standardSeconds(100L))),
                TimestampedValue.of("{\"timestamp\": 20220124, \"device\": \"swe590-sensor-1\", \"temperature\": 65}", Instant.now().plus(Duration.standardSeconds(110L))),
                TimestampedValue.of("{\"timestamp\": 20220125, \"device\": \"swe590-sensor-1\", \"temperature\": 70}", Instant.now().plus(Duration.standardSeconds(120L)))
            ).advanceWatermarkToInfinity();

        final List<KV<String, String>> testLookUpData = Arrays.asList(
            KV.of("swe590-sensor-1", "İstanbul"),
            KV.of("swe590-sensor-2", "İzmir")
        );
        PCollection<KV<String, String>> lookUpData = pipeline
            .apply("Creation of lookup data", Create.of(testLookUpData));
        PCollectionView<Map<String, String>> lookUpDataView = lookUpData
            .apply(View.asMap());

        Schema schema = Schema.builder()
            .addNullableField(FIELD_NAME_UUID, Schema.FieldType.STRING)
            .addInt64Field(FIELD_NAME_TIMESTAMP)
            .addStringField(FIELD_NAME_DEVICE)
            .addDoubleField(FIELD_NAME_TEMPERATURE)
            .addNullableField(FIELD_NAME_LOCATION, Schema.FieldType.STRING)
            .addNullableField(FIELD_NAME_AVG_TEMPERATURE, Schema.FieldType.DOUBLE)
            .build();

        PCollection<Row> jsonMessages = pipeline
//            .apply("Ingest events", createEvents)
            .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
            .apply("Artificial load", ArtificialLoad.create())
            .apply("Convert JSON string into to Row object", JsonToRow.withSchema(schema));

        PCollection<KV<String, Row>> enrichedData = jsonMessages
            .apply("Assign device field as key", WithKeys.of(row -> row.getString(FIELD_NAME_DEVICE))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(schema)))
            .apply("Add randomly generated UUID. Add location information using the key, then assign location field as key.",
                ParDo.of(
                    new DoFn<KV<String, Row>, KV<String, Row>>() {
                        @ProcessElement
                        public void pe(@Element KV<String, Row> input, OutputReceiver<KV<String, Row>> outputReceiver, ProcessContext pc) {
                            Map<String, String> deviceToLocation = pc.sideInput(lookUpDataView);
                            Row rowWithLocation = Row.fromRow(input.getValue())
                                .withFieldValue(FIELD_NAME_LOCATION, deviceToLocation.get(input.getKey()))
                                .withFieldValue(FIELD_NAME_UUID, UUID.randomUUID().toString())
                                .build();
                            KV<String, Row> output = KV.of(rowWithLocation.getString(FIELD_NAME_LOCATION), rowWithLocation);
                            outputReceiver.output(output);
                        }
                    }
                ).withSideInputs(lookUpDataView)
            );

        PCollectionView<Map<String, Double>> averageTemperatureByLocation = enrichedData
            .apply("Window stream into sliding windows of duration of 1 minute every 5 seconds", Window.into(SlidingWindows.of(Duration.standardMinutes(1L)).every(Duration.standardSeconds(5L))))
            .apply("Group by using the location key", GroupByKey.create())
            .apply("Calculate and add the average temperature of the last 1 minute by location", ParDo.of(
                new DoFn<KV<String, Iterable<Row>>, KV<String, Double>>() {
                    @ProcessElement
                    public void pe(ProcessContext pc) {
                        Double avg = StreamSupport.stream(pc.element().getValue().spliterator(), false)
                            .mapToDouble(row -> row.getDouble(FIELD_NAME_TEMPERATURE))
                            .average().orElseThrow();
                        KV<String, Double> result = KV.of(pc.element().getKey(), avg);
                        pc.output(result);
                    }
                }
            ))
            .apply("Convert location-temperature PCollection into PCView", View.asMap());

        PCollection<Row> dataWithAverageTemperature = enrichedData
            .apply("Window stream into fixed windows of duration of 5 seconds", Window.into(FixedWindows.of(Duration.standardSeconds(5L))))
            .apply(ParDo.of(
                new DoFn<KV<String, Row>, Row>() {
                    @ProcessElement
                    public void pe(@Element KV<String, Row> input, OutputReceiver<Row> outputReceiver, ProcessContext pc) {
                        String location = input.getKey();
                        Double averageTemperatureOfLocation = pc.sideInput(averageTemperatureByLocation).get(location);
                        Row result = Row.fromRow(input.getValue())
                            .withFieldValue(FIELD_NAME_AVG_TEMPERATURE, averageTemperatureOfLocation)
                            .build();
                        System.out.println(pc.timestamp().toString() + result.toString());
                        outputReceiver.output(result);
                    }
                }
            ).withSideInputs(averageTemperatureByLocation)).setRowSchema(schema);

        PCollection<TableRow> dataToWriteIntoBigQuery = dataWithAverageTemperature
            .apply("Row to TableRow conversion", ParDo.of(
                new DoFn<Row, TableRow>() {
                    @ProcessElement
                    public void pe(@Element Row row, OutputReceiver<TableRow> outputReceiver) {
                        TableRow tableRow = BigQueryUtils.toTableRow(row);
                        outputReceiver.output(tableRow);
                    }
                }
            ));

        dataToWriteIntoBigQuery.apply("Write into BigQuery", BigQueryIO
            .writeTableRows()
            .to(String.format("%s:%s.%s", options.getProject(), options.getBigQueryDatasetName(), options.getBigQueryTableName()))
            .withSchema(BigQueryUtils.toTableSchema(schema))
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry())
            .optimizedWrites()
        );

        pipeline.run().waitUntilFinish();
    }

    private static void recreateBigQueryTable(PubSubBigQueryGCStorageOptions options) {
        try {
            com.google.cloud.bigquery.Schema bqSchema = com.google.cloud.bigquery.Schema.of(
                Field.of(FIELD_NAME_UUID, StandardSQLTypeName.STRING),
                Field.of(FIELD_NAME_TIMESTAMP, StandardSQLTypeName.STRING),
                Field.of(FIELD_NAME_DEVICE, StandardSQLTypeName.STRING),
                Field.of(FIELD_NAME_TEMPERATURE, StandardSQLTypeName.FLOAT64),
                Field.of(FIELD_NAME_LOCATION, StandardSQLTypeName.STRING),
                Field.of(FIELD_NAME_AVG_TEMPERATURE, StandardSQLTypeName.FLOAT64)
            );

            Table bqTable;
            BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
            TableId tableId = TableId.of(options.getProject(), options.getBigQueryDatasetName(), options.getBigQueryTableName());
            bqTable = bigQuery.getTable(tableId);

            LOGGER.info("Deleting table: [{}]", bqTable);
            boolean deleted = bigQuery.delete(tableId);
            LOGGER.info("{} BigQuery Table: [{}]", deleted ? "Deleted" : "Could not delete", options.getBigQueryTableName());

            TableDefinition bqTableDefinition = StandardTableDefinition.of(bqSchema);
            TableInfo bqTableInfo = TableInfo.of(tableId, bqTableDefinition);
            bqTable = bigQuery.create(bqTableInfo);
            LOGGER.info("Table is created. Waiting for the table to become available:\n[{}]", bqTable);
            for (int i = 0; ; i++) {
                LOGGER.info("Table is not available. Seconds passed: {}", i);
                Thread.sleep(1000);
                Table bqTable2 = bqTable.reload();
                if (bqTable2 != null) {
                    LOGGER.info("Table is available after [{}] seconds:\n[{}]", i, bqTable2);
                    Thread.sleep(30000);
                    break;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

