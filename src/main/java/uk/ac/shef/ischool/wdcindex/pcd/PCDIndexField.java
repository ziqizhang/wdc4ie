package uk.ac.shef.ischool.wdcindex.pcd;

public enum PCDIndexField {
    F_ID("id"),
    F_TLD("tld"),
    F_HOST("host"),
    //F_SUBJECT("subject"),
    //F_SUBJECT_NAMESPACE("subject_namespace"),
    //F_SUBJECT_TEXT("subject_text"),
    F_PREDICATE("predicate"),
    F_CLASS("class");

    private String fieldname;

    PCDIndexField(String fieldname) {
        this.fieldname = fieldname;
    }

    public String getFieldname() {
        return fieldname;
    }
}
