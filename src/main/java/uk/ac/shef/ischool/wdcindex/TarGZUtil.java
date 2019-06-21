package uk.ac.shef.ischool.wdcindex;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class TarGZUtil {

    public static void main(String[] args) throws IOException {
        untargz(new File("/home/zz/Work/wdc4ie/resources/output/1438042981460.12.tar.gz"));
    }

    /**
     * Unzip a tar.gz file into a folder, and return that folder path
     *
     * @param targz
     * @return
     * @throws IOException
     */
    public static String untargz(File targz) throws IOException {
        String filename=targz.getName().substring(0, targz.getName().lastIndexOf(".gz"));
        String tarFileName = targz.getParent() + "/"+filename;
        FileInputStream instream = new FileInputStream(targz);
        GZIPInputStream ginstream = new GZIPInputStream(instream);
        FileOutputStream outstream = new FileOutputStream(tarFileName);
        byte[] buf = new byte[1024];
        int len;
        while ((len = ginstream.read(buf)) > 0) {
            outstream.write(buf, 0, len);
        }
        ginstream.close();
        outstream.close();

        //There should now be tar files in the directory
        //extract specific files from tar
        TarArchiveInputStream myTarFile = new TarArchiveInputStream(new FileInputStream(tarFileName));
        TarArchiveEntry entry = null;
        int offset;
        FileOutputStream outputFile = null;
        String fileName = targz.getName().substring(0, targz.getName().lastIndexOf('.'));
        fileName = fileName.substring(0, fileName.lastIndexOf('.'));
        //read every single entry in TAR file

        while ((entry = myTarFile.getNextTarEntry()) != null) {
            //the following two lines remove the .tar.gz extension for the folder name
            File outputDir = new File(targz.getParent() + "/" + fileName + "/" + entry.getName());
            if (!outputDir.getParentFile().exists()) {
                outputDir.getParentFile().mkdirs();
            }
            //if the entry in the tar is a directory, it needs to be created, only files can be extracted
            if (entry.isDirectory()) {
                outputDir.mkdirs();
            } else {
                byte[] content = new byte[(int) entry.getSize()];
                offset = 0;
                myTarFile.read(content, offset, content.length - offset);
                outputFile = new FileOutputStream(outputDir);
                IOUtils.write(content, outputFile);
                outputFile.close();
            }
        }
        //close and delete the tar files, leaving the original .tar.gz and the extracted folders
        myTarFile.close();
        File tarFile = new File(tarFileName);
        tarFile.delete();
        return targz.getParent() + "/" + fileName;
    }

}
