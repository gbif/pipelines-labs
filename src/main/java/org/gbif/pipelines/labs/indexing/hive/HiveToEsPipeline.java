package org.gbif.pipelines.labs.indexing.hive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.thrift.TException;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.pipelines.config.base.EsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HiveToEsPipeline {

    private static final String GBIFID = "gbifid";

    interface HiveToEsOptions extends EsOptions {

        @Description("Uri to hive Metastore, e.g.: thrift://hivesever2:9083")
        String getMetastoreUris();
        void setMetastoreUris(String metastoreUris);


        @Description("Hive Database")
        String getHiveDB();
        void setHiveDB(String hiveDB);

        @Description("Hive Table")
        String getHiveTable();
        void setHiveTable(String hiveTable);
    }

    private static final Logger LOG = LoggerFactory.getLogger(HiveToEsPipeline.class);


    public static void main(String[] args) {
        HiveToEsOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(HiveToEsOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        Map<String, String> configProperties = new HashMap<>();
        configProperties.put(HiveConf.ConfVars.METASTOREURIS.varname, options.getMetastoreUris());
        HCatSchemaRecordDescriptor schemaRecordDescriptor = readHiveSchema(options.getHiveDB(),
                options.getHiveTable(), options.getMetastoreUris());
        pipeline
                 .apply(HCatalogIO.read()
                        .withConfigProperties(configProperties)
                        .withDatabase(options.getHiveDB()) //optional, assumes default if none specified
                        .withTable(options.getHiveTable()))
                .apply("Converting to JSON", ParDo.of(new RecordToEs(new HCatRecordToEsDoc(schemaRecordDescriptor))))
                .apply("Indexing to ElasticSearch", ElasticsearchIO.write()
                        .withConnectionConfiguration(ElasticsearchIO.ConnectionConfiguration.create(options.getESHosts(),
                                options.getESIndexName(),
                                "occurrence"))
                        .withMaxBatchSize(options.getESMaxBatchSize())
                        .withIdFn(node -> node.get(GBIFID).asText()));
        pipeline.run();
    }

    private static HCatSchemaRecordDescriptor readHiveSchema(String dataBase, String tableName, String metastoreUri) {
        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
            HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(hiveConf);
            List<FieldSchema> fieldSchemaList = metaStoreClient.getSchema(dataBase, tableName);
            return new HCatSchemaRecordDescriptor(HCatSchemaUtils.getHCatSchema(fieldSchemaList));
        } catch(TException | HCatException ex) {
            LOG.error("Error reading table schema", ex);
            throw Throwables.propagate(ex);
        }
    }

    static class LoggingFn extends DoFn<String,String> {
        // Instantiate Logger.
        // Suggestion: As shown, specify the class name of the containing class
        // (WordCount).
        private static final Logger LOG = LoggerFactory.getLogger(LoggingFn.class);

        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println(c.element());

        }
    }

    static class RecordToEs extends DoFn<HCatRecord,String> {

        private final ObjectMapper mapper = new ObjectMapper();
        private final HCatRecordToEsDoc hCatRecordToEsDoc;

        public RecordToEs(HCatRecordToEsDoc hCatRecordToEsDoc) {
            mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd"));
            this.hCatRecordToEsDoc = hCatRecordToEsDoc;
        }

        @ProcessElement
        public void processElement(ProcessContext context) {
            try {
                HCatRecord record = context.element();
                context.output(mapper.writeValueAsString(hCatRecordToEsDoc.convert(record)));
            } catch (JsonProcessingException ex) {
                LOG.error("Error converting record [{}] into JSON", context.element(), ex);
                throw Throwables.propagate(ex);
            }
        }
    }

    static class HCatRecordToEsDoc implements Serializable  {


        private final HCatSchemaRecordDescriptor schema;


        HCatRecordToEsDoc(HCatSchemaRecordDescriptor schema) {
            this.schema = schema;
        }


        private Map<String,Object> verbatimFields(HCatRecord record) {
            Map<String,Object> verbatimFields = new HashMap<>();
            schema.getVerbatimFields().forEach(field -> {
                try {
                    //substring(2) removes the prefix v_
                    verbatimFields.put(field.substring(2),record.getString(field, schema.getHCatSchema()));
                } catch (HCatException ex){
                    LOG.error("Error reading field {}", field, ex);
                    throw Throwables.propagate(ex);
                }
            });
            return verbatimFields;
        }

        private Map<String,Object> interpretedFields(HCatRecord record) {
            Map<String,Object> interpretedFields = new HashMap<>();
            schema.getInterpretedFields().forEach(field -> {
                try {
                    //substring(2) removes the prefix v_
                    if (field.equals("lastinterpreted") || field.equals("lastcrawled")
                            || field.equals("fragmentcreated") || field.equals("eventdate")) {
                        interpretedFields.put(field, Optional.ofNullable(record.getLong(field, schema.getHCatSchema()))
                                .map(value -> new SimpleDateFormat("yyyy-MM-dd").format(new Date(value)))
                                .orElse(null));
                    } else if (field.equals("mediatype") || field.equals("issue")) {
                        interpretedFields.put(field, record.getList(field, schema.getHCatSchema()));
                    } else {
                        interpretedFields.put(field, record.get(field, schema.getHCatSchema()));
                    }
                } catch (HCatException ex){
                    LOG.error("Error reading field {}", field, ex);
                    throw Throwables.propagate(ex);
                }
            });
            return interpretedFields;
        }

        private Map<String,Object> convert(HCatRecord record) {
            try {
                Map<String,Object> esDoc = new HashMap<>();
                esDoc.putAll(interpretedFields(record));
                if((Boolean)esDoc.get("hascoordinate")) {
                    esDoc.put("coordinate", esDoc.get("decimallatitude") + "," + esDoc.get("decimallongitude"));
                }
                esDoc.put("verbatim", verbatimFields(record));

                return esDoc;
            } catch (Exception ex) {
                LOG.error("Error building ES document", ex);
                throw Throwables.propagate(ex);
            }
        }
    }

    static class HCatSchemaRecordDescriptor implements Serializable {

        private final HCatSchema hCatSchema;

        private final List<String> verbatimFields;

        private final List<String> interpretedFields;

        HCatSchemaRecordDescriptor(HCatSchema hCatSchema) {
            this.hCatSchema = hCatSchema;

            Map<Boolean, List<String>> fields = hCatSchema.getFieldNames().stream()
                                .collect(Collectors.partitioningBy(field -> field.startsWith("v_")));
            this.verbatimFields = fields.get(true);
            this.interpretedFields = fields.get(false);
        }

        public HCatSchema getHCatSchema() {
            return hCatSchema;
        }

        public List<String> getVerbatimFields() {
            return verbatimFields;
        }

        public List<String> getInterpretedFields() {
            return interpretedFields;
        }
    }
}
