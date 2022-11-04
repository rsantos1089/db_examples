package com.databases.Options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

public interface ParameterOptions extends DataflowPipelineOptions {

    @Description("GCS temp bucket for BQ Loads")
    @Required
    ValueProvider<String> getTempGCSBQBucket();
    void setTempGCSBQBucket(ValueProvider<String> value);

    @Description("GCS temp bucket for Dataflow Processing")
    @Required
    String getTempGCSDataflowBucket();
    void setTempGCSDataflowBucket(String value);
    

}
