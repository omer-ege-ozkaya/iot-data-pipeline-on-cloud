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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

class PrintPCollection<T> extends PTransform<PCollection<T>, PCollection<T>> {

    static <T> PrintPCollection<T> create() {
        return new PrintPCollection<>();
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        Coder<T> coder = input.getCoder();
        PCollection<T> pCollection = input.apply(
            MapElements.via(
                new SimpleFunction<T, T>() {
                    @Override
                    public T apply(T input) {
                        System.out.println(input);
                        return input;
                    }
                }
            )
        ).setCoder(coder);
        return pCollection;
    }
}

public class StarterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

    public static void main(String[] args) {
        int numShards = 1;
        PubSubToGcsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToGcsOptions.class);

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
//            .apply("Write files to GCS", new WriteOneFilePerWindow(options.getOutput(), numShards));

//        PCollection<String> messages = pipeline.apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()));


        TestStream<String> createEvents = TestStream.create(StringUtf8Coder.of())
            .addElements(
                TimestampedValue.of("{\"timestamp\": \"2021-01-13\", \"device\": \"swe590-sensor-1\", \"temperature\": \"10\"}", Instant.now()),
                TimestampedValue.of("{\"timestamp\": \"2021-01-14\", \"device\": \"swe590-sensor-1\", \"temperature\": \"15\"}", Instant.now().plus(Duration.standardSeconds(30L))),
                TimestampedValue.of("{\"timestamp\": \"2022-01-15\", \"device\": \"swe590-sensor-1\", \"temperature\": \"20\"}", Instant.now().plus(Duration.standardSeconds(60L))),
                TimestampedValue.of("{\"timestamp\": \"2022-01-16\", \"device\": \"swe590-sensor-1\", \"temperature\": \"25\"}", Instant.now().plus(Duration.standardSeconds(90L))),
                TimestampedValue.of("{\"timestamp\": \"2022-01-17\", \"device\": \"swe590-sensor-1\", \"temperature\": \"30\"}", Instant.now().plus(Duration.standardSeconds(120L)))
            ).advanceWatermarkToInfinity();


//        final List<String> testData = Arrays.asList(
//            "{\"timestamp\": \"2021-06-12\", \"device\": \"swe590-sensor-1\", \"temperature\": \"22\"}",
//            "{\"timestamp\": \"2021-07-13\", \"device\": \"swe590-sensor-1\", \"temperature\": \"18\"}",
//            "{\"timestamp\": \"2022-08-14\", \"device\": \"swe590-sensor-2\", \"temperature\": \"24\"}",
//            "{\"timestamp\": \"2022-09-15\", \"device\": \"swe590-sensor-2\", \"temperature\": \"19\"}"
//        );
//        PCollection<String> rawMessages = pipeline.apply("Creation of data", Create.of(testData));
        final List<KV<String, String>> testLookUpData = Arrays.asList(
            KV.of("swe590-sensor-1", "İstanbul"),
            KV.of("swe590-sensor-2", "İzmir")
        );
        PCollection<KV<String, String>> lookUpData = pipeline
            .apply("Creation of lookup data", Create.of(testLookUpData));

        Schema schema = Schema.builder()
            .addStringField("timestamp")
            .addStringField("device")
            .addStringField("temperature")
            .addNullableField("location", Schema.FieldType.STRING)
            .addNullableField("avg_temperature_of_the_last_minute_by_location", Schema.FieldType.DOUBLE)
            .build();


        PCollection<Row> jsonMessage = pipeline
            .apply(createEvents)
            .apply("json to row", JsonToRow.withSchema(schema));

        PCollection<Row> test2 = jsonMessage
            .apply("Key device", WithKeys.of(row -> row.getString("device"))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(schema)))
            .apply("Fix window", Window.into(FixedWindows.of(Duration.standardMinutes(1L))))
            .apply("Inner join", Join.InnerJoin.with(lookUpData))
            .apply("Inner join step 2", ParDo.of(
                new DoFn<KV<String, KV<Row, String>>, KV<String, Row>>() {
                    @ProcessElement
                    public void pe(@Element KV<String, KV<Row, String>> input, OutputReceiver<KV<String, Row>> outputReceiver) {
                        System.out.println(input);
                        KV<String, Row> output = KV.of(
                            input.getValue().getValue(),
                            input.getValue().getKey()
                        );
                        outputReceiver.output(output);
                    }
                }
            )).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(schema)))
            .apply("Group by", GroupByKey.create())
            .apply("Average", ParDo.of(
                new DoFn<KV<String, Iterable<Row>>, Row>() {
                    @ProcessElement
                    public void pe(ProcessContext pc) {
                        Double avg = StreamSupport.stream(pc.element().getValue().spliterator(), false)
                            .mapToDouble(row -> Double.parseDouble(row.getString("avg_temperature_of_the_last_minute_by_location")))
                            .average().orElseThrow();
                        for (Row row : pc.element().getValue()) {
                            Row result = Row.fromRow(row)
                                .withFieldValue("avg_temperature_of_the_last_minute_by_location", avg)
                                .build();
                            System.out.println(result);
                            pc.output(result);
                        }
                    }
                }
            )).setRowSchema(schema);

        PCollection<Row> test3 = test2.apply(PrintPCollection.create());

//        PCollection<JsonMessage> jsonMessage = rawMessages.apply("Telemetry data to JsonObject", ParDo.of(
//                new DoFn<String, JsonMessage>() {
//                    @ProcessElement
//                    public void processElement(@Element String message, OutputReceiver<JsonMessage> out) {
//                        Gson gson = new Gson();
//                        JsonMessage jsonMessage = gson.fromJson(message, JsonMessage.class);
//                        out.output(jsonMessage);
//                    }
//                }
//            )
//        );
//        PCollection<KV<String, JsonMessage>> keyedMessages = jsonMessage.apply("Device id key added to PCollection", ParDo.of(
//                new DoFn<JsonMessage, KV<String, JsonMessage>>() {
//                    @ProcessElement
//                    public void processElement(@Element JsonMessage jsonMessage, OutputReceiver<KV<String, JsonMessage>> out) {
//                        KV<String, JsonMessage> kv = KV.of(jsonMessage.getDevice(), jsonMessage);
//                        out.output(kv);
//                    }
//                }
//            )
//        );
//
//        PCollection<KV<String, KV<JsonMessage, String>>> joined = Join.innerJoin(keyedMessages, lookUpData);
//
//        PCollection<KV<String, KV<JsonMessage, String>>> test = joined.apply("test", ParDo.of(
//            new DoFn<KV<String, KV<JsonMessage, String>>, KV<String, KV<JsonMessage, String>>>() {
//                @ProcessElement
//                public void processElement(@Element KV<String, KV<JsonMessage, String>> input, OutputReceiver<KV<String, KV<JsonMessage, String>>> out) {
//                    System.out.println(input);
//                    out.output(input);
//                }
//            }
//        ));

//        PCollection<KV<String, Iterable<JsonMessage>>> groupedMessagesByDevice = keyedMessages.apply("Group messages by device id",
//            GroupByKey.<String, JsonMessage>create()
//        );
//        PCollection<KV<String, Double>> averageTemperatureByDevice = groupedMessagesByDevice.apply("Average temperature by device",
//            ParDo.of(
//                new DoFn<KV<String, Iterable<JsonMessage>>, KV<String, Double>>() {
//                    @ProcessElement
//                    public void processElement(@Element KV<String, Iterable<JsonMessage>> kv, OutputReceiver<KV<String, Double>> out) {
//                        int sum = 0;
//                        int count = 0;
//                        for (JsonMessage jsonMessage : Objects.requireNonNull(kv.getValue())) {
//                            sum += Integer.parseInt(jsonMessage.getTemperature());
//                            count++;
//                        }
//                        Double average = (double) sum / count;
//                        KV<String, Double> averageOfDevice = KV.of(kv.getKey(), average);
//                        out.output(averageOfDevice);
//                    }
//                }
//            )
//        );


//        final TupleTag<Double> averageTemperatureByDeviceTag = new TupleTag<>();
//        final TupleTag<String> lookUpDataTag = new TupleTag<>();
//        PCollection<KV<String, CoGbkResult>> joined =
//            KeyedPCollectionTuple
//                .of(averageTemperatureByDeviceTag, averageTemperatureByDevice)
//                .and(lookUpDataTag, lookUpData)
//                .apply("Join by cogroupbykey", CoGroupByKey.create());
//        PCollection<String> test = joined.apply("Test", ParDo.of(
//            new DoFn<KV<String, CoGbkResult>, String>() {
//                @ProcessElement
//                public void processElement(ProcessContext processContext) {
//                    KV<String, CoGbkResult> result = processContext.element();
//                    String key = result.getKey();
//                    Iterable<Double> temp = result.getValue().getAll(averageTemperatureByDeviceTag);
//                    Iterable<String> city = result.getValue().getAll(lookUpDataTag);
//                    System.out.println(key + temp.toString() + city.toString());
//                }
//            }
//        ) );


//        final List<KV<String, String>> emailsList =
//            Arrays.asList(
//                KV.of("amy", "amy@example.com"),
//                KV.of("carl", "carl@example.com"),
//                KV.of("julia", "julia@example.com"),
//                KV.of("carl", "carl@email.com"));
//
//        final List<KV<String, String>> phonesList =
//            Arrays.asList(
//                KV.of("amy", "111-222-3333"),
//                KV.of("james", "222-333-4444"),
//                KV.of("amy", "333-444-5555"),
//                KV.of("carl", "444-555-6666"));
//
//        PCollection<KV<String, String>> emails = pipeline.apply("CreateEmails", Create.of(emailsList));
//        PCollection<KV<String, String>> phones = pipeline.apply("CreatePhones", Create.of(phonesList));
//        final TupleTag<String> emailsTag = new TupleTag<>();
//        final TupleTag<String> phonesTag = new TupleTag<>();
//
//        final List<KV<String, CoGbkResult>> expectedResults =
//            Arrays.asList(
//                KV.of(
//                    "amy",
//                    CoGbkResult.of(emailsTag, Arrays.asList("amy@example.com"))
//                        .and(phonesTag, Arrays.asList("111-222-3333", "333-444-5555"))
//                ),
//                KV.of(
//                    "carl",
//                    CoGbkResult.of(emailsTag, Arrays.asList("carl@email.com", "carl@example.com"))
//                        .and(phonesTag, Arrays.asList("444-555-6666"))
//                ),
//                KV.of(
//                    "james",
//                    CoGbkResult.of(emailsTag, Arrays.asList())
//                        .and(phonesTag, Arrays.asList("222-333-4444"))
//                ),
//                KV.of(
//                    "julia",
//                    CoGbkResult.of(emailsTag, Arrays.asList("julia@example.com"))
//                        .and(phonesTag, Arrays.asList())
//                )
//            );
//
//        PCollection<KV<String, CoGbkResult>> results =
//            KeyedPCollectionTuple.of(emailsTag, emails)
//                .and(phonesTag, phones)
//                .apply(CoGroupByKey.create());
//
//        PCollection<String> contactLines =
//            results.apply(
//                ParDo.of(
//                    new DoFn<KV<String, CoGbkResult>, String>() {
//                        @ProcessElement
//                        public void processElement(ProcessContext c) {
//                            KV<String, CoGbkResult> e = c.element();
//                            String name = e.getKey();
//                            Iterable<String> emailsIter = e.getValue().getAll(emailsTag);
//                            Iterable<String> phonesIter = e.getValue().getAll(phonesTag);
//                            String formattedResult =
//                                Snippets.formatCoGbkResults(name, emailsIter, phonesIter);
//                            c.output(formattedResult);
//                        }
//                    }));
//
//        final List<String> formattedResults =
//            Arrays.asList(
//                "amy; ['amy@example.com']; ['111-222-3333', '333-444-5555']",
//                "carl; ['carl@email.com', 'carl@example.com']; ['444-555-6666']",
//                "james; []; ['222-333-4444']",
//                "julia; ['julia@example.com']; []");

        pipeline.run().waitUntilFinish();
    }
}
