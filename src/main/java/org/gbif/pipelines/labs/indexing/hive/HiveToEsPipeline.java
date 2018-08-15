package org.gbif.pipelines.labs.indexing.hive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
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

        private static final Map<OccurrenceSearchParameter,String> SEARCH_PARAMETER_FIELD_MAP = Arrays.stream(OccurrenceSearchParameter.values())
                .filter(searchParameter -> searchParameter != OccurrenceSearchParameter.GEOMETRY) //It is not a stored field
                .collect(Collectors.toMap(Function.identity(), HCatRecordToEsDoc::hiveField));

        private static String hiveField(OccurrenceSearchParameter searchParameter) {
            if (searchParameter == OccurrenceSearchParameter.PUBLISHING_ORG) {
                return "publishingorgkey";
            }
            if (searchParameter == OccurrenceSearchParameter.COUNTRY) {
                return "countrycode";
            }
            if (searchParameter == OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE) {
                return "hasgeospatialissues";
            }
            return searchParameter.name().replace("_","").toLowerCase();
        }

        private final HCatSchemaRecordDescriptor schema;


        public HCatRecordToEsDoc(HCatSchemaRecordDescriptor schema) {
            this.schema = schema;
        }

        private Object searchFieldValue(OccurrenceSearchParameter searchParameter, HCatRecord record) {
            try {
                if (Date.class.isAssignableFrom(searchParameter.type())) {
                    return Optional.ofNullable(record.getLong(SEARCH_PARAMETER_FIELD_MAP.get(searchParameter), schema.getHCatSchema()))
                            .map(value -> new SimpleDateFormat("yyyy-MM-dd").format(new Date(value)))
                            .orElse(null);
                }
                if (OccurrenceSearchParameter.ISSUE == searchParameter || OccurrenceSearchParameter.MEDIA_TYPE == searchParameter) {
                   record.getList(SEARCH_PARAMETER_FIELD_MAP.get(searchParameter), schema.getHCatSchema());
                }
                return record.get(SEARCH_PARAMETER_FIELD_MAP.get(searchParameter), schema.getHCatSchema());
            } catch(Exception ex) {
                LOG.error("Error reading field {}", searchParameter, ex);
                throw Throwables.propagate(ex);
            }
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

        private Map<String,Object> convert(HCatRecord record) {
            try {
                Map<String,Object> esDoc = new HashMap<>();
                esDoc.put(GBIFID, record.get(GBIFID, schema.getHCatSchema()));
                SEARCH_PARAMETER_FIELD_MAP.forEach((key, value) -> esDoc.put(value, searchFieldValue(key, record)));
                if((Boolean)esDoc.get(SEARCH_PARAMETER_FIELD_MAP.get(OccurrenceSearchParameter.HAS_COORDINATE))) {
                    esDoc.put("coordinate", esDoc.get(SEARCH_PARAMETER_FIELD_MAP.get(OccurrenceSearchParameter.DECIMAL_LATITUDE)) + "," + esDoc.get(SEARCH_PARAMETER_FIELD_MAP.get(OccurrenceSearchParameter.DECIMAL_LONGITUDE)));
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

        HCatSchemaRecordDescriptor(HCatSchema hCatSchema) {
            this.hCatSchema = hCatSchema;
            verbatimFields = hCatSchema.getFieldNames().stream()
                                .filter(field -> field.startsWith("v_")).collect(Collectors.toList());
        }

        public HCatSchema getHCatSchema() {
            return hCatSchema;
        }

        public List<String> getVerbatimFields() {
            return verbatimFields;
        }
    }
}
