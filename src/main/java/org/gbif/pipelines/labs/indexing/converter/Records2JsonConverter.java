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

public class Records2JsonConverter {

  private final StringBuilder sb = new StringBuilder().append("{");
  private SpecificRecordBase[] bases;
  private Set<String> escapeKeys = Collections.emptySet();
  private Map<Class<? extends SpecificRecordBase>, BiConsumer<SpecificRecordBase, StringBuilder>>
      biConsumer = new HashMap<>();
  private String replaceKey = "";

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

  public Records2JsonConverter setReplaceKey(String replaceKey) {
    this.replaceKey = replaceKey;
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
    return convert();
  }

  void commonConvert(SpecificRecordBase base) {
    base.getSchema()
        .getFields()
        .forEach(field -> addJsonField(field.name(), base.get(field.pos())));
  }

  private String convert() {
    return append("}")
        .toString()
        .replaceAll("(\"\\{)|(\\{,)", "{")
        .replaceAll("(}\")|(,})", "}")
        .replaceAll("(\"\\[)|(\\[,)", "[")
        .replaceAll("(]\")|(,])", "]")
        .replaceAll("(]\\[)", "],[")
        .replaceAll("(\"\")", "\",\"")
        .replaceAll("(}\\{)", "},{");
  }

  StringBuilder append(String str) {
    return sb.append(str);
  }

  StringBuilder addJsonField(String key, Object value) {
    if (escapeKeys.contains(key)) {
      return sb;
    }
    return addJsonFieldNoCheck(key, value);
  }

  StringBuilder addJsonFieldNoCheck(String key, Object value) {
    sb.append("\"").append(key.replaceAll(replaceKey, "")).append("\":");
    if (Objects.isNull(value)) {
      return sb.append("null").append(",");
    }
    return sb.append("\"").append(value).append("\",");
  }
}
