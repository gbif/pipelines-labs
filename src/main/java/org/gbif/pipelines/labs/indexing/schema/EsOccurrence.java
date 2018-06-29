package org.gbif.pipelines.labs.indexing.schema;

import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;

public class EsOccurrence {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private InterpretedExtendedRecord extendedRecord;
    private LocationRecord locationRecord;
    private MultimediaRecord multimediaRecord;
    private TemporalRecord temporalRecord;
    private TaxonRecord taxonRecord;

    private Builder() {}

    public void extendedRecord(InterpretedExtendedRecord extendedRecord) {
      this.extendedRecord = extendedRecord;
    }

    public void locationRecord(LocationRecord locationRecord) {
      this.locationRecord = locationRecord;
    }

    public void multimediaRecord(MultimediaRecord multimediaRecord) {
      this.multimediaRecord = multimediaRecord;
    }

    public void temporalRecord(TemporalRecord temporalRecord) {
      this.temporalRecord = temporalRecord;
    }

    public void taxonRecord(TaxonRecord taxonRecord) {
      this.taxonRecord = taxonRecord;
    }
  }

  private static String toJsonField(String value, String type) {
    return null;
  }

}
