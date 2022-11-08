package com.databases;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.*;
import com.databases.Helpers.Functions;
import org.apache.beam.sdk.io.AvroIO;

import java.util.*;

import org.apache.avro.Schema;

public class test {

    public static void main(String[] args) throws Exception {

    String json = "{\n" +
    "     \"type\": \"record\",\n" +
    "     \"namespace\": \"postgres\",\n" +
    "     \"name\": \"table_test\",\n" +
    "     \"fields\": [\n" +
    "       { \"name\": \"id\", \"type\": \"string\" },\n" +
    "       { \"name\": \"name\", \"type\": \"string\" },\n" +
    "       { \"name\": \"last_name\", \"type\": \"string\" },\n" +
    "       { \"name\": \"age\", \"type\": \"int\" },\n" +
    "       { \"name\": \"date\", \"type\": \"string\" }\n" +
    "     ]\n" +
    "}";
    //System.out.println(json);

    String propFileName = "configuration.properties";
    Functions.GetPropertyValues objProperty = new Functions.GetPropertyValues();

    String bucket_url =objProperty.getPropValues("bucket_url", propFileName);
        String bucket_schema_root_path =objProperty.getPropValues("bucket_schema_root_path", propFileName);
        String schema_name =objProperty.getPropValues("schema_name", propFileName);

    Storage storage = StorageOptions.newBuilder()
    .setProjectId("neat-drummer-366611")
    .build()
    .getService();
    
    //Blob blob = storage.get(bucket_url, bucket_schema_root_path +  schema_name +".avsc");
   // String schemaContent = new String(blob.getContent());
   String schemaContent = "{\n" +
   "     \"type\": \"record\",\n" +
   "     \"namespace\": \"postgres\",\n" +
   "     \"name\": \"table_test\",\n" +
   "     \"fields\": [\n" +
   "       { \"name\": \"id\", \"type\": \"string\" },\n" +
   "       { \"name\": \"name\", \"type\": \"string\" },\n" +
   "       { \"name\": \"last_name\", \"type\": \"string\" },\n" +
   "       { \"name\": \"age\", \"type\": \"int\" },\n" +
   "       { \"name\": \"date\", \"type\": \"string\" }\n" +
   "     ]\n" +
   "}"; 
    System.out.println(schemaContent);
    Schema schema=new org.apache.avro.Schema.Parser().parse(schemaContent);
    System.out.println("-------------");
    System.out.println(schema);
    List<String> vowels = new ArrayList<>();

	vowels.add("A"); // [A]
	vowels.add("E"); // [A, E]
	vowels.add("U"); // [A, E, U]
    System.out.println(vowels);
    System.out.println(vowels.get(1));
}
}
