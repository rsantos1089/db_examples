package com.databases.Process;

import com.google.api.services.bigquery.model.TableRow;

import org.apache.arrow.flatbuf.Null;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConvertStringtoTableRow extends DoFn<String, TableRow>{
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ConvertStringtoTableRow.class);

    private final String[] fields;
    
    public ConvertStringtoTableRow(String[] fields){
        this.fields = fields;

    } 
    @ProcessElement
    public void processElement(ProcessContext c) {
        // Parse the String into a {@link TableRow} object.
        String[] values = c.element().split(",");
        //LOG.info("Message since pubsub :" + c.element());

        TableRow row = new TableRow();
       
        for(int i = 0; i< values.length; i++) {
            
            row.set(this.fields[i], values[i]) ;
           
            }
        //LOG.info("row:"+row);
        c.output(row);
    }
}
