package org.gbif.pipelines.labs.indexing.converter;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.taxon.Rank;
import org.gbif.pipelines.io.avro.taxon.RankedName;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.avro.specific.SpecificRecordBase;

public class GbifRecords2JsonConverter extends Records2JsonConverter {

  private static final String[] ESCAPE_KEYS = {
    "decimalLatitude", "decimalLongitude", "diagnostics", "id"
  };
  private static final String[] REPLACE_KEYS = {
    "http://rs.tdwg.org/dwc/terms/", "http://purl.org/dc/terms/"
  };

  private GbifRecords2JsonConverter(SpecificRecordBase[] bases) {
    setSpecificRecordBase(bases);
    setEscapeKeys(ESCAPE_KEYS);
    setReplaceKeys(REPLACE_KEYS);
    addSpecificConverter(ExtendedRecord.class, getExtendedRecordConverter());
    addSpecificConverter(LocationRecord.class, getLocationRecordConverter());
    addSpecificConverter(TaxonRecord.class, getTaxonomyRecordConverter());
  }

  public static GbifRecords2JsonConverter create(SpecificRecordBase... bases) {
    return new GbifRecords2JsonConverter(bases);
  }

  private BiConsumer<SpecificRecordBase, StringBuilder> getExtendedRecordConverter() {
    return (record, sb) -> {
      Map<String, String> terms = ((ExtendedRecord) record).getCoreTerms();
      addJsonFieldNoCheck("id", record.get(0));
      //
      append("\"verbatim\":{");
      terms.forEach(this::addJsonField);
      append("},");
    };
  }

  private BiConsumer<SpecificRecordBase, StringBuilder> getLocationRecordConverter() {
    return (record, sb) -> {
      LocationRecord location = (LocationRecord) record;
      append("\"location\":");
      if (Objects.isNull(location.getDecimalLongitude())
          || Objects.isNull(location.getDecimalLatitude())) {
        append("null,");
      } else {
        append("{")
            .addJsonField("lon", location.getDecimalLongitude())
            .addJsonField("lat", location.getDecimalLatitude())
            .append("},");
      }
      //
      commonConvert(record);
    };
  }

  private BiConsumer<SpecificRecordBase, StringBuilder> getTaxonomyRecordConverter() {
    return (record, sb) -> {
      TaxonRecord taxon = (TaxonRecord) record;
      Map<Rank, String> map =
          taxon
              .getClassification()
              .stream()
              .collect(Collectors.toMap(RankedName::getRank, RankedName::getName));
      //
      addJsonField("gbifKingdom", map.get(Rank.KINGDOM));
      addJsonField("gbifPhylum", map.get(Rank.PHYLUM));
      addJsonField("gbifClass", map.get(Rank.CLASS));
      addJsonField("gbifOrder", map.get(Rank.ORDER));
      addJsonField("gbifFamily", map.get(Rank.FAMILY));
      addJsonField("gbifGenus", map.get(Rank.GENUS));
      addJsonField("gbifSubgenus", map.get(Rank.SUBGENUS));
      //
      addJsonField("gbifSpeciesKey", taxon.getUsage().getKey());
      addJsonField("gbifScientificName", taxon.getUsage().getName());
      //
      commonConvert(record);
    };
  }
}
