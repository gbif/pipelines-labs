package org.gbif.pipelines.labs.indexing.hive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
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
import org.gbif.pipelines.config.EsProcessingPipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HiveToEsPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(HiveToEsPipeline.class);


    public static void main(String[] args) {
        EsProcessingPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).create().as(EsProcessingPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        Map<String, String> configProperties = new HashMap<>();
        configProperties.put(HiveConf.ConfVars.METASTOREURIS.varname,"thrift://metastore-host:port");
        HCatSchemaRecordDescriptor schemaRecordDescriptor = readHiveSchema("dev", "occurrence_hds","");
        pipeline
                 .apply(HCatalogIO.read()
                        .withConfigProperties(configProperties)
                        .withDatabase("dev") //optional, assumes default if none specified
                        .withTable("occurrence_hdfs"))
                .apply("Converting to JSON", ParDo.of(new RecordToEs(new HCatRecordToEsDoc(schemaRecordDescriptor))))
                .apply(ElasticsearchIO.write()
                        .withConnectionConfiguration(ElasticsearchIO.ConnectionConfiguration.create(options.getESHosts(),
                                options.getESIndexName(),
                                options.getESIndexName()))
                        .withMaxBatchSize(options.getESMaxBatchSize()));
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

    static class HCatRecordToEsDoc {

        private static final Map<OccurrenceSearchParameter,String> SEARCH_PARAMETER_FIELD_MAP = Arrays.stream(OccurrenceSearchParameter.values()).collect(Collectors.toMap(Function.identity(), param -> param.name().replace("_","").toLowerCase()));

        private final HCatSchemaRecordDescriptor schema;


        public HCatRecordToEsDoc(HCatSchemaRecordDescriptor schema) {
            this.schema = schema;
        }

        private Object searchFieldValue(OccurrenceSearchParameter searchParameter, HCatRecord record) {
            try {
                if (Date.class.isAssignableFrom(searchParameter.type())) {
                    return Optional.ofNullable(record.getDecimal(SEARCH_PARAMETER_FIELD_MAP.get(searchParameter), schema.getHCatSchema()))
                            .map(value -> new SimpleDateFormat("yyyy-MM-dd").format(new Date(value.longValue())))
                            .orElse(null);
                }
                if (OccurrenceSearchParameter.ISSUE == searchParameter || OccurrenceSearchParameter.MEDIA_TYPE == searchParameter) {
                   record.getList(SEARCH_PARAMETER_FIELD_MAP.get(searchParameter), schema.getHCatSchema());
                }
                return record.get(SEARCH_PARAMETER_FIELD_MAP.get(searchParameter), schema.getHCatSchema());
            } catch(HCatException ex) {
                LOG.error("Error reading field {}", searchParameter, ex);
                throw Throwables.propagate(ex);
            }
        }

        private Map<String,Object> verbatimFields(HCatRecord record) {
            Map<String,Object> verbatimFields = new HashMap<>();
            schema.getVerbatimFields().forEach(field -> {
                try {
                    verbatimFields.put(field,record.getString(field, schema.getHCatSchema()));
                } catch (HCatException ex){
                    LOG.error("Error reading field {}", field, ex);
                    throw Throwables.propagate(ex);
                }
            });
            return verbatimFields;
        }

        private Map<String,Object> convert(HCatRecord record) {
            Map<String,Object> searchFields = new HashMap<>();
            SEARCH_PARAMETER_FIELD_MAP.forEach((key, value) -> searchFields.put(value, searchFieldValue(key, record)));
            searchFields.put("verbatim", verbatimFields(record));
            return searchFields;
        }
    }

    static class HCatSchemaRecordDescriptor {

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
