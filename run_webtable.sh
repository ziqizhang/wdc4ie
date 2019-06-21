#!/bin/bash
#$ -l h_rt=168:00:00 -l rmem=16G -m bea -M ziqi.zhang@sheffield.ac.uk
JAVA_HOME=/data/li1zz/jre1.8.0_171
export JAVA_HOME
echo $JAVA_HOME
webtable_list=/home/zz/Work/wdc4ie/resources/table/file_table.lst
wdc_list=/home/zz/Work/wdc4ie/resources/table/file_wdc.list
outfolder=/home/zz/Work/wdc4ie/resources/table/tmp
start=0
end=650
#-Djava.util.logging.config.file=/data/li1zz/wdc4ie/target/classes/log4j.properties

java -Xmx15000m -cp 'target/footballwhisper-1.0-SNAPSHOT-jar-with-dependencies.jar' uk.ac.shef.ischool.wdcindex.table.WDCWebtableOverlapChecker_V1 $webtable_list $wdc_list $outfolder

