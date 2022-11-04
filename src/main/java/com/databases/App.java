package com.databases;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashMap;

import javax.print.event.PrintEvent;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import com.google.cloud.storage.*;
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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.json.JSONObject;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.beam.sdk.coders.AvroCoder;

import com.google.api.services.bigquery.model.Row;
import com.databases.Helpers.Functions;
import com.databases.Helpers.Functions.filterMessages;
import com.databases.Options.ParameterOptions;
import com.databases.Process.ConvertJSONtoGenericRecord;
import com.fasterxml.jackson.core.JsonParser;

public final class App {

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
        String bucket_url =objProperty.getPropValues("bucket_url", propFileName);
        String bucket_schema_root_path =objProperty.getPropValues("bucket_schema_root_path", propFileName);
        String schema_name =objProperty.getPropValues("schema_name", propFileName);
        Date date = new Date();
        Timestamp timestamp2 = new Timestamp(date.getTime());
        String appname = "Databases-"+timestamp2;
        
        String databaseInputInstance = objProperty.getPropValues("jdbc_instance_name",propFileName);
        String databaseLocation = objProperty.getPropValues("jdbc_location",propFileName);
        String databaseName = objProperty.getPropValues("jdbc_database",propFileName);
        String databaseUser = Functions.getSecretValueSM(projectId,objProperty.getPropValues("jdbc_user",propFileName),"latest");
        LOG.info("databaseUser is :" + databaseUser);
        String databasePassword = objProperty.getPropValues("jdbc_password",propFileName);
        String inputDatabaseConnectionString = String.format(objProperty.getPropValues("jdbc_database_url",propFileName),
                databaseName, projectId, databaseLocation, databaseInputInstance);
        String queryString = "select id,name,last_name,age,date from public.table_test";
        String output_prefix = objProperty.getPropValues("output_prefix",propFileName); 

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

        String fields_downstream = objProperty.getPropValues("fields_downstream",propFileName);
        String types_downstream = objProperty.getPropValues("types_downstream",propFileName);  
        String[] fieldsArrDwn = fields_downstream.split(",");
        String[] typeArrDwn = types_downstream.split(",");

        LinkedHashMap<Integer, String> fieldsDictDwn = new LinkedHashMap<Integer, String>();
        LinkedHashMap<Integer, String> typesDictDwn = new LinkedHashMap<Integer, String>();
        
        Integer j = 0;
        for (String input : typeArrDwn) {
                    typesDictDwn.put(j, input);
                    ++j;
        }

        Integer k = 0;
        for (String input : fieldsArrDwn) {
                    fieldsDictDwn.put(k, input);
                    ++k;
        }

        Blob blob = storage.get(bucket_url, bucket_schema_root_path +  schema_name +".avsc");
        String schemaContent = new String(blob.getContent());
        //System.out.println(schemaContent);

        PCollection<String> readPostgres = p.apply("Read from Postgres", JdbcIO.<String>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                .create("org.postgresql.Driver", inputDatabaseConnectionString).withUsername(databaseUser)
                .withPassword(databasePassword))
                .withQuery(queryString).withCoder(StringUtf8Coder.of())
                .withRowMapper(new JdbcIO.RowMapper<String>() {
            public String mapRow(ResultSet resultSet) throws Exception {
                JSONObject row =  new JSONObject();
                for (int i = 0; i < fieldsDictDwn.size(); i++) {    
                    row.put(fieldsDictDwn.get(i), resultSet.getString(fieldsDictDwn.get(i)));
                }
                System.out.println("row:"+row);
                return row.toString();
            }
        }));
        /*
        String schemaContent = "{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"postgres\",\n" +
        "     \"name\": \"table_test\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"id\", \"type\": \"string\" },\n" +
        "       { \"name\": \"name\", \"type\": \"string\" },\n" +
        "       { \"name\": \"last_name\", \"type\": \"string\" },\n" +
        "       { \"name\": \"age\", \"type\": \"string\" },\n" +
        "       { \"name\": \"date\", \"type\": \"string\" }\n" +
        "     ]\n" +
        "}"; */

        Schema schema=new org.apache.avro.Schema.Parser().parse(schemaContent);
        //convert Json to Avro
        PCollection <GenericRecord> avroRecords=readPostgres
        .apply(ParDo.of(new ConvertJSONtoGenericRecord(schemaContent,fieldsDictDwn)))
        .setCoder(AvroCoder.of(GenericRecord.class, schema));

        LOG.info("writing AVRO ");
  
        avroRecords.apply("WriteToAvro", AvroIO.writeGenericRecords(schema)
         .to(output_prefix)
         .withSuffix(".avro").withNumShards(1));        

        p.run().waitUntilFinish();        
    }

}
