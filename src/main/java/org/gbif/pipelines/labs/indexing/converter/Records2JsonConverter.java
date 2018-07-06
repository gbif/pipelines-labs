package org.gbif.pipelines.labs.indexing.converter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Records2JsonConverter {

  private static final Logger LOG = LoggerFactory.getLogger(Records2JsonConverter.class);

  private final StringBuilder sb = new StringBuilder().append("{");
  private SpecificRecordBase[] bases;
  private Set<String> escapeKeys = Collections.emptySet();
  private Map<Class<? extends SpecificRecordBase>, BiConsumer<SpecificRecordBase, StringBuilder>>
      biConsumer = new HashMap<>();
  private String[] replaceKeys = {};

  Records2JsonConverter() {}

  public static Records2JsonConverter create() {
    return new Records2JsonConverter();
  }

  public static Records2JsonConverter create(SpecificRecordBase... bases) {
    return new Records2JsonConverter().setSpecificRecordBase(bases);
  }

  public Records2JsonConverter setSpecificRecordBase(SpecificRecordBase... bases) {
    this.bases = bases;
    return this;
  }

  public Records2JsonConverter setReplaceKeys(String... replaceKeys) {
    this.replaceKeys = replaceKeys;
    return this;
  }

  public Records2JsonConverter setEscapeKeys(String... escapeKeys) {
    if (this.escapeKeys.isEmpty()) {
      this.escapeKeys = new HashSet<>(Arrays.asList(escapeKeys));
    } else {
      this.escapeKeys.addAll(Arrays.asList(escapeKeys));
    }
    return this;
  }

  public Records2JsonConverter addSpecificConverter(
      Class<? extends SpecificRecordBase> type,
      BiConsumer<SpecificRecordBase, StringBuilder> consumer) {
    biConsumer.put(type, consumer);
    return this;
  }

  public String buildJson() {
    Arrays.stream(bases)
        .forEach(
            record -> {
              BiConsumer<SpecificRecordBase, StringBuilder> consumer =
                  biConsumer.get(record.getClass());
              if (Objects.nonNull(consumer)) {
                consumer.accept(record, sb);
              } else {
                commonConvert(record);
              }
            });
    String convert = convert();
    //LOG.info(convert);
    //isValidJSON(convert);
    return convert;
  }

  Records2JsonConverter commonConvert(SpecificRecordBase base) {
    base.getSchema()
        .getFields()
        .forEach(field -> addJsonField(field.name(), base.get(field.pos())));
    return this;
  }

  private String convert() {
    append("}");
    return sb.toString()
        .replaceAll("(\"\\{)|(\\{,)", "{")
        .replaceAll("(}\")|(,})", "}")
        .replaceAll("(\\[,)", "[")
        .replaceAll("(\"\\[\\{)", "[{")
        .replaceAll("(}]\")", "}]")
        .replaceAll("(,])", "]")
        .replaceAll("(}]\\[\\{)", "],[")
        .replaceAll("(\"\")", "\",\"")
        .replaceAll("(}\\{)", "},{");
  }

  Records2JsonConverter append(Object obj) {
    sb.append(obj);
    return this;
  }

  Records2JsonConverter addJsonField(String key, Object value) {
    if (escapeKeys.contains(key)) {
      return this;
    }
    return addJsonFieldNoCheck(key, value);
  }

  Records2JsonConverter addJsonFieldNoCheck(String key, Object value) {
    for (String rule : replaceKeys) {
      key = key.replaceAll(rule, "");
    }
    sb.append("\"").append(key).append("\":");
    if (Objects.isNull(value)) {
      return append("null").append(",");
    }
    if (value instanceof String) {
      value = ((String) value).replaceAll("(\\\\)", "\\\\\\\\").replaceAll("\"", "\\\\\"");
    }
    return append("\"").append(value).append("\",");
  }

//  public void isValidJSON(String json) {
//    try (JsonParser parser = new ObjectMapper().getJsonFactory().createJsonParser(json)) {
//      while (parser.nextToken() != null) {}
//    } catch (IOException ex) {
//      throw new IllegalArgumentException(json);
//    }
//  }
}
