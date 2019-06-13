package uk.ac.shef.ischool.wdcindex.deprecated.indexer;

public enum TripleIndexField {

    F_ID("id"),
    F_TLD("tld"),
    F_HOST("host"),
    //F_SUBJECT("subject"),
    //F_SUBJECT_NAMESPACE("subject_namespace"),
    //F_SUBJECT_TEXT("subject_text"),
    F_PREDICATE("predicate"),
    F_PREDICATE_NAMESPACE("predicate_namespace"),
    //F_PREDICATE_TEXT("predicate_text"),
    F_OBJECT("object"),
    F_OBJECT_NAMESPACE("object_namespace"),
    //F_OBJECT_TEXT("object_text"),
    F_OBJECT_TYPE("object_type"),
    F_PROVENANCE_WDC_FORMAT("provenance_wdc_format"),
    F_PROVENANCE_WDC_NQUAD_FILE("provenance_wdc_nquad_file"),
    F_PROVENANCE_WDC_NQUAD_LINE("provenance_wdc_nquad_line"),
    F_PROVENANCE_CC_WARC("provenance_cc_warc"),
    F_PROVENANCE_CC_WARC_OFFSET("provenance_cc_warc_offsets"),
    F_PROVENANCE_URL("provenance_url");

    private String fieldname;

    TripleIndexField(String fieldname) {
        this.fieldname = fieldname;
    }

    public String getFieldname() {
        return fieldname;
    }
}
