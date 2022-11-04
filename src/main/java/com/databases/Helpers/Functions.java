package com.databases.Helpers;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.logging.Log;

public class Functions {
    
    public static class GetPropertyValues {

        String result = "";
        InputStream inputStream;

        public String getPropValues(String property, String propFileName) throws IOException {

            Properties prop = new Properties();
            //String propFileName = "configuration.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            String result = prop.getProperty(property);
            inputStream.close();

            return result;
        }
    }

    public static Timestamp convertStringToTimestamp(String strDate) {
        try {
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = formatter.parse(strDate);
            Timestamp timeStampDate = new Timestamp(date.getTime());

            return timeStampDate;
        } catch (ParseException e) {
            System.out.println("Exception :" + e);
            return null;
        }
    }


    public static Timestamp convertStringToTimestampTimeZone(String strDate,String ValueTimeZone) {
        
        
        if(ValueTimeZone.equalsIgnoreCase("AMEX") ){
            String TimeZone="America/Mexico_City";

            try {
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = formatter.parse(strDate);
            Timestamp timeStampDate = new Timestamp(date.getTime());
            String strDateUTC = timeStampDate.toInstant().atZone( ZoneId.of( TimeZone ) ).format( DateTimeFormatter.ISO_LOCAL_DATE_TIME).replace( "T" , " " );
            Date dateUTC = formatter.parse(strDateUTC);
            Timestamp timeStampDateUTC = new Timestamp(dateUTC.getTime());
            return timeStampDateUTC;
        } catch (ParseException e) {
            System.out.println("Exception :" + e);
            return null;
        }

        } else{

            String TimeZone = "Etc/GMT-5";
                            
        try {
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = formatter.parse(strDate);
            Timestamp timeStampDate = new Timestamp(date.getTime());
            String strDateUTC = timeStampDate.toInstant().atZone( ZoneId.of( TimeZone ) ).format( DateTimeFormatter.ISO_LOCAL_DATE_TIME).replace( "T" , " " );
            Date dateUTC = formatter.parse(strDateUTC);
            Timestamp timeStampDateUTC = new Timestamp(dateUTC.getTime());
            return timeStampDateUTC;
            
        } catch (ParseException e) {
            System.out.println("Exception :" + e);
            return null;
        }
            }
    }


    public static BigDecimal convertStringToBigDecimal(String value) {
        try {
            BigDecimal big = new BigDecimal(value);
            return big;
        } catch (Exception e) {
            System.out.println("Exception :" + e);
            return null;
        }
    }

    public static java.sql.Date convertStringToDate(String strDate) {
        try {
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = formatter.parse(strDate);
            java.sql.Date sDate = new java.sql.Date(date.getTime());
            return sDate;
        } catch (ParseException e) {
            System.out.println("Exception :" + e);
            return null;
        }
    }

    public static String getSecretValueSM(String projectId, String secretId, String versionId) throws Exception {

        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            SecretVersionName secretVersionName = SecretVersionName.of(projectId, secretId, versionId);
            AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
            return response.getPayload().getData().toStringUtf8();
        }
    }

    public static class MyMap extends SimpleFunction < Long, Long > {
        public Long apply(Long in ) {
                //System.out.println("Length is: " + in );
                return in;
            }
        }

    public static class filterMessages extends DoFn<String, String> {

            String resourceType; 

            public filterMessages (String wish_resourceType){
                resourceType = wish_resourceType;
              }
              @ProcessElement
              public void processElement(ProcessContext c) {
                  String message = c.element();
                  String resourceName = resourceType;

                  Boolean valid_message_bool = message.contains("\"resourceType\":\""+resourceName+"\"");

                  if(valid_message_bool == true){

                    c.output(message);

                    }
                
                }
            }

            
    
}
