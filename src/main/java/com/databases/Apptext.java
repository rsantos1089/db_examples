package com.databases;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.LinkedHashMap;

import javax.print.event.PrintEvent;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import com.google.cloud.storage.*;
import com.google.errorprone.annotations.Immutable;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.healthcare.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.json.JSONObject;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.beam.sdk.coders.AvroCoder;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.Row;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.databases.Helpers.Functions;
import com.databases.Helpers.Functions.filterMessages;
import com.databases.Options.ParameterOptions;
import com.databases.Process.ConvertJSONtoGenericRecord;
import com.fasterxml.jackson.core.JsonParser;

import com.databases.Process.ConvertStringtoTableRow;
public final class Apptext {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        String propFileName = "configuration.properties";
        Functions.GetPropertyValues objProperty = new Functions.GetPropertyValues();
        String projectId = objProperty.getPropValues("gcp_project_id", propFileName);
        String bigquery_dataset = objProperty.getPropValues("bigquery_dataset", propFileName);
        String bigquery_table = objProperty.getPropValues("bigquery_table", propFileName);
        String bucket_url =objProperty.getPropValues("bucket_url", propFileName);
        String bucket_schema_root_path =objProperty.getPropValues("bucket_schema_root_path", propFileName);
        String bigquery_schema =objProperty.getPropValues("bigquery_schema", propFileName);
        Date date = new Date();
        Timestamp timestamp2 = new Timestamp(date.getTime());
        String appname = "Databases-"+timestamp2;
        
        Storage storage = StorageOptions.newBuilder()
        .setProjectId(projectId)
        .build()
        .getService();
        LOG.info("storage is :" + storage);

        ParameterOptions options = PipelineOptionsFactory.fromArgs(args).as(ParameterOptions.class);

        options.setAppName(appname);
        options.setProject(objProperty.getPropValues("gcp_project_id", propFileName));
        options.setTempLocation(objProperty.getPropValues("gcp_temp_location",propFileName));
        options.setStagingLocation(objProperty.getPropValues("gcp_staging_location",propFileName));
        options.setRegion(objProperty.getPropValues("gcp_region",propFileName));
        options.setMaxNumWorkers(Integer.parseInt(objProperty.getPropValues("max_workers",propFileName)));

        Pipeline p = Pipeline.create(options);

        String fields_txt = objProperty.getPropValues("fields_txt",propFileName);
        String types_txt = objProperty.getPropValues("types_txt",propFileName);  
        String[] fieldsArrTxt = fields_txt.split(",");
        String[] typeArrTxt = types_txt.split(",");
        String[] List_index = objProperty.getPropValues("indexs",propFileName).split(","); 
        List<Integer> List_index_int = new ArrayList<>();

        LinkedHashMap<Integer, String> fieldsDictTxt = new LinkedHashMap<Integer, String>();
        LinkedHashMap<Integer, String> typesDictTxt = new LinkedHashMap<Integer, String>();
        
        Integer j = 0;
        for (String input : typeArrTxt) {
                    typesDictTxt.put(j, input);
                    ++j;
        }

        Integer k = 0;
        for (String input : fieldsArrTxt) {
                    fieldsDictTxt.put(k, input);
                    ++k;
        }

        for (String input : List_index){
            List_index_int.add(Integer.parseInt(input));
        }

        Blob blob = storage.get(bucket_url, bucket_schema_root_path +  bigquery_schema +".json");
        String bqSchema = new String(blob.getContent());
        //System.out.println(bqSchema);

        PCollection<String> readTxt = p.apply("Read from bucket txt", TextIO.read().from("gs://neat-drummer-366611/bigquery_schema/data.txt"));
        
        PCollection<String> Txtvalue=readTxt.apply("Split by index",ParDo.of(new DoFn<String, String>() {
            @ProcessElement
                public void process(ProcessContext c) {
                String value =c.element();
                String valuefinal = "";
                for(Integer i =0; i < List_index_int.size();i++){
                //String valuefinal = value.substring(0,11)+","+value.substring(11,13)+","+value.substring(13,17);    
                if (i != List_index_int.size() -1 ) {
                valuefinal += value.substring(List_index_int.get(i),List_index_int.get(i+1))+",";
                                                    }
                }
                //LOG.info("data:" + value+ " length:"+ c.element().length());
                //LOG.info("data:" + valuefinal.substring(0,valuefinal.length() -1));
            c.output(valuefinal);    
            }
                }));

            TableSchema tableSchema = new TableSchema().setFields(ImmutableList.of(
                new TableFieldSchema().setName("name")
                                      .setType("STRING")
                                      .setMode("NULLABLE"),
                new TableFieldSchema().setName("age")
                                      .setType("INTEGER")
                                      .setMode("NULLABLE"),
                new TableFieldSchema().setName("country")
                                      .setType("STRING")
                                      .setMode("NULLABLE")
            )
            );

            LOG.info("schema tablerow:"+tableSchema);

             Txtvalue.apply(ParDo.of( new ConvertStringtoTableRow(fieldsArrTxt,typeArrTxt)))
            .apply("write to bigquery",BigQueryIO.writeTableRows()
            .to(String.format("%s:%s.%s", projectId, bigquery_dataset, bigquery_table))
            .withSchema(tableSchema)
            //.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
            .withCustomGcsTempLocation(options.getTempGCSBQBucket())
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            //.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withoutValidation());
            /*
            Txtrow.apply(ParDo.of(new DoFn<TableRow,Void>() {
                @ProcessElement
                public void process(ProcessContext c) {
                LOG.info("table row:"+c.element());
                } 
            }));
             */

        p.run().waitUntilFinish();        
    }

}
