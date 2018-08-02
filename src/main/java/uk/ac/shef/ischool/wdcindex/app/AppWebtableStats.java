package uk.ac.shef.ischool.wdcindex.app;

import uk.ac.shef.ischool.wdcindex.table.WDCWebtableOverlapChecker;

import java.io.IOException;

public class AppWebtableStats {
    public static void main(String[] args) throws IOException {
        WDCWebtableOverlapChecker app = new WDCWebtableOverlapChecker();
        app.process(args[0],args[1],args[2]);
    }
}
