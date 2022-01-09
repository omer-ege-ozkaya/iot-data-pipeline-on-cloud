package com.omeregeozkaya.boun;


import com.google.cloud.bigquery.BigQueryOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;

public interface PubSubBigQueryGCStorageOptions extends DataflowPipelineOptions {
    @Description("The Cloud Pub/Sub topic to read from.")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String value);

    @Description("The BigQuery dataset to write into.")
    @Validation.Required
    String getBigQueryDatasetName();

    void setBigQueryDatasetName(String value);

    @Description("The BigQuery table to write into.")
    @Validation.Required
    String getBigQueryTableName();

    void setBigQueryTableName(String value);

    @Description("Output file's window size in number of seconds.")
    @Default.Integer(1)
    Integer getWindowSize();

    void setWindowSize(Integer value);

    @Description("Path of the output file including its filename prefix.")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
}
