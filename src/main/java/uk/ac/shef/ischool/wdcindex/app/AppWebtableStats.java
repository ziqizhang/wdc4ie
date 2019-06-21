package uk.ac.shef.ischool.wdcindex.app;

import uk.ac.shef.ischool.wdcindex.table.WDCWebtableOverlapChecker_V1;

import java.io.IOException;

public class AppWebtableStats {
    public static void main(String[] args) throws IOException {
        WDCWebtableOverlapChecker_V1 app = new WDCWebtableOverlapChecker_V1();
        app.process(args[0],args[1],args[2], Integer.valueOf(args[3]), Integer.valueOf(args[4]));
    }
}
