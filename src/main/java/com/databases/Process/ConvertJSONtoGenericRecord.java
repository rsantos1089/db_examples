package com.databases.Process;

import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;

import java.util.LinkedHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.beam.sdk.coders.AvroCoder;

public class ConvertJSONtoGenericRecord extends DoFn<String,GenericRecord> implements java.io.Serializable {
    private final String schema;
    private final LinkedHashMap<Integer, String> fieldsDictDwn ;
    public ConvertJSONtoGenericRecord(String schema,LinkedHashMap<Integer, String> fieldsDictDwn) {
            this.schema=schema;
            this.fieldsDictDwn = fieldsDictDwn;
    }

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<GenericRecord> out){
            JSONObject json=new JSONObject(element);
            Schema schema_avro=new org.apache.avro.Schema.Parser().parse(this.schema);
            GenericRecord record = new GenericData.Record(schema_avro);
            for (int i = 0; i < this.fieldsDictDwn.size(); i++) {    
                record.put(this.fieldsDictDwn.get(i), json.getString(this.fieldsDictDwn.get(i)));
            }
            System.out.println("record:"+record);
            out.output(record);
    }
    }
