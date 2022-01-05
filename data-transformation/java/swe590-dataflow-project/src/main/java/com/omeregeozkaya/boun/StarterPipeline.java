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

import com.google.api.client.json.Json;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=DataflowRunner
 */
public class StarterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

    public static void main(String[] args) {
        //region helloworld
//    Pipeline p = Pipeline.create(
//        PipelineOptionsFactory.fromArgs(args).withValidation().create());
//
//    p.apply(Create.of("Hello", "World"))
//    .apply(MapElements.via(new SimpleFunction<String, String>() {
//      @Override
//      public String apply(String input) {
//        return input.toUpperCase();
//      }
//    }))
//    .apply(ParDo.of(new DoFn<String, Void>() {
//      @ProcessElement
//      public void processElement(ProcessContext c)  {
//        LOG.info(c.element());
//      }
//    }));
//
//    p.run();
        //endregion
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
        List<String> testData = Arrays.asList(
            "{\"timestamp\": \"2021-06-12\", \"device\": \"istanbul\", \"temperature\": \"22\"}",
            "{\"timestamp\": \"2021-07-13\", \"device\": \"istanbul\", \"temperature\": \"18\"}",
            "{\"timestamp\": \"2022-08-14\", \"device\": \"ordu\", \"temperature\": \"24\"}",
            "{\"timestamp\": \"2022-09-15\", \"device\": \"ordu\", \"temperature\": \"19\"}"
        );
        PCollection<String> rawMessages = pipeline.apply("Creation of data", Create.of(testData));
        PCollection<JsonMessage> jsonMessage = rawMessages.apply("Message to JsonMessage", ParDo.of(
                new DoFn<String, JsonMessage>() {
                    @ProcessElement
                    public void processElement(@Element String message, OutputReceiver<JsonMessage> out) {
                        Gson gson = new Gson();
                        JsonMessage jsonMessage = gson.fromJson(message, JsonMessage.class);
                        System.out.println(jsonMessage);
                        out.output(jsonMessage);
                    }
                }
            )
        );
        PCollection<KV<String, JsonMessage>> keyedMessages = jsonMessage.apply("Key added to JsonMessage", ParDo.of(
                new DoFn<JsonMessage, KV<String, JsonMessage>>() {
                    @ProcessElement
                    public void processElement(@Element JsonMessage jsonMessage, OutputReceiver<KV<String, JsonMessage>> out) {
                        KV<String, JsonMessage> kv = KV.of(jsonMessage.getDevice(), jsonMessage);
                        System.out.println(kv);
                        out.output(kv);
                    }
                }
            )
        );
        PCollection<KV<String, Iterable<JsonMessage>>> groupedMessagesByDevice = keyedMessages.apply("Group messages by device",
            GroupByKey.<String, JsonMessage>create()
        );
        PCollection<KV<String, Double>> averageTemperatureByDevice = groupedMessagesByDevice.apply("Average temperature by device",
            ParDo.of(
                new DoFn<KV<String, Iterable<JsonMessage>>, KV<String, Double>>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Iterable<JsonMessage>> kv, OutputReceiver<KV<String, Double>> out) {
                        int sum = 0;
                        int count = 0;
                        for(JsonMessage jsonMessage : Objects.requireNonNull(kv.getValue())) {
                            sum += Integer.parseInt(jsonMessage.getTemperature());
                            count++;
                        }
                        Double average = (double) sum / count;
                        KV<String, Double> averageOfDevice = KV.of(kv.getKey(), average);
                        System.out.println(averageOfDevice);
                        out.output(averageOfDevice);
                    }
                }
            )
        );


        pipeline.run().waitUntilFinish();
    }
}
