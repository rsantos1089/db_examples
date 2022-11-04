package com.databases.Process;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConvertJSONtoTableRow extends DoFn<String, TableRow>{
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ConvertJSONtoTableRow.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        // Parse the JSON into a {@link TableRow} object.
        String json = c.element();
        LOG.info("Message since pubsub :" + c.element());
        //System.out.println("json:"+json);
        TableRow row_internal;
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            row_internal = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
            LOG.info("Table row to bigquery :" + row_internal);
            //System.out.println("row_internal:"+row_internal);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }
        c.output(row_internal);
    }
}
