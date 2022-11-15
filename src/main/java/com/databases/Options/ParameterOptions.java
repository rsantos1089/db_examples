package com.databases.Options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

public interface ParameterOptions extends DataflowPipelineOptions {

    @Description("Project id")
    @Required
    String getProjectId();
    void setProjectId(String value);

    @Description("Region to process dataflow")
    @Required
    String getRegion();
    void setRegion(String value);

    @Description("Fields names of file")
    @Required
    ValueProvider<String> getFieldsNamesFile();
    void setFieldsNamesFile(String value);

    @Description("Positions index of file")
    @Required
    String getPositionsIndexFile();
    void setPositionsIndexFile(String value);
   
    @Description("Path of file")
    @Required
    ValueProvider<String> getPathFile();
    void setPathFile(ValueProvider<String> value);

    @Description("Bigquery dataset target")
    @Required
    String getBigqueryDataset();
    void setBigqueryDataset(String value);

    @Description("Bigquery table target")
    @Required
    String getBigqueryTable();
    void setBigqueryTable(String value);

    @Description("Path of temp Bigquery process")
    @Required
    ValueProvider<String> getTempGCSBQBucket();
    void setTempGCSBQBucket(ValueProvider<String> value);

    @Description("Total num workers in dataflow")
    @Required
    String getWorkers();
    void setWorkers(String value);

}
