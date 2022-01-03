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
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    //  long targetTime = System.currentTimeMillis() + options.getDelaySeconds() * 1000;
//            while (System.currentTimeMillis() < targetTime) {
//    int x = 1 + 1;
//  }
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

        pipeline
            .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
            .apply(MapElements.via(
                new SimpleFunction<String, String>() {
                    @Override
                    public String apply(String input) {
                        long targetTime = System.currentTimeMillis() + options.getDelaySeconds() * 1000;
                        int x = 0;
                        while (System.currentTimeMillis() < targetTime) {
                            x = x + (x%2 == 0 ? -1 : 1);
                        }
                        return input;
                    }
                }
            ))
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))))
            .apply("Write files to GCS", new WriteOneFilePerWindow(options.getOutput(), numShards));

        pipeline.run().waitUntilFinish();
    }
}
