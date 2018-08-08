package org.gbif.pipelines.labs.benchmark.datagen;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Script to convert all interpreted records to json (flat & nested). Sample script command for flat
 * nohup java -cp pipelines-labs-1.1-SNAPSHOT-shaded.jar
 * org.gbif.pipelines.labs.benchmark.datagen.ConvertToJSON outputFile=hdfs://ha-nn/gbif-data/flat/
 * inputFile=hdfs://ha-nn/data/ingest/ hdfsSite=hdfs-site.xml coreSite=coresite.xml type=flat
 * homedir=/home/rpathak > /var/lib/hadoop-hdfs/flat.out & for nested nohup java -cp
 * pipelines-labs-1.1-SNAPSHOT-shaded.jar org.gbif.pipelines.labs.benchmark.datagen.ConvertToJSON
 * outputFile=hdfs://ha-nn/gbif-data/nested/ inputFile=hdfs://ha-nn/data/ingest/
 * hdfsSite=hdfs-site.xml coreSite=coresite.xml type=nested homedir=/home/rpathak >
 * /var/lib/hadoop-hdfs/flat.out &
 */
public class ConvertToJSON {

  public static void main(String[] args) throws IOException, InterruptedException {
    Map<Options, String> cmdOpts = new HashMap<>();
    Options[] optionsAvailable = Options.values();
    for (Options opt : optionsAvailable) {
      cmdOpts.put(opt, null);
    }
    for (String arg : args) {
      String[] split = arg.split("=");
      cmdOpts.put(Options.valueOf(split[0]), split[1]);
    }
    Configuration config = new Configuration();
    config.addResource(new File(cmdOpts.get(Options.hdfsSite)).toURL());
    config.addResource(new File(cmdOpts.get(Options.coreSite)).toURL());
    FileSystem fileSystem = FileSystem.get(config);

    FileStatus[] fileStatuses = fileSystem.listStatus(new Path(cmdOpts.get(Options.inputFile)));
    int count = 0;
    for (FileStatus status : fileStatuses) {
      System.out.println("Converting: " + status.getPath().toString());
      count++;
      String datasetId = status.getPath().getName();
      String inputFile = cmdOpts.get(Options.inputFile);
      String attempt = fileSystem.listStatus(status.getPath())[0].getPath().getName();
      String hdfsSite = cmdOpts.get(Options.hdfsSite);
      String coreSite = cmdOpts.get(Options.coreSite);
      String defaultTargetDirectory = cmdOpts.get(Options.outputFile);
      String homeDir = cmdOpts.get(Options.homedir);

      String[] consArgsFlat = {
        "java",
        "-cp",
        "pipelines-labs-1.1-SNAPSHOT-shaded.jar",
        "org.gbif.pipelines.labs.benchmark.datagen.EsCoGroupFlatPipeline",
        "--datasetId=" + datasetId,
        "--attempt=" + attempt,
        "--hdfsSiteConfig=" + hdfsSite,
        "--coreSiteConfig=" + coreSite,
        "--defaultTargetDirectory=" + defaultTargetDirectory,
        "--inputFile=" + inputFile
      };
      String[] consArgsNested = {
        "java",
        "-cp",
        "pipelines-labs-1.1-SNAPSHOT-shaded.jar",
        "org.gbif.pipelines.labs.benchmark.datagen.EsCoGroupNestedPipeline",
        "--datasetId=" + datasetId,
        "--attempt=" + attempt,
        "--hdfsSiteConfig=" + hdfsSite,
        "--coreSiteConfig=" + coreSite,
        "--defaultTargetDirectory=" + defaultTargetDirectory,
        "--inputFile=" + inputFile
      };

      if (cmdOpts.get(Options.type).equals(Type.flat.name())) {
        ProcessBuilder pbuilder = new ProcessBuilder(consArgsFlat);
        pbuilder.directory(new File(homeDir));
        pbuilder
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .start()
            .waitFor(10, TimeUnit.MINUTES);
      } else {
        ProcessBuilder pbuilder = new ProcessBuilder(consArgsNested);
        pbuilder.directory(new File(homeDir));
        pbuilder
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .start()
            .waitFor(10, TimeUnit.MINUTES);
      }
    }
    System.out.println("total counts converted =" + count);
    fileSystem.close();
  }

  enum Type {
    flat,
    nested
  }

  enum Options {
    inputFile,
    outputFile,
    hdfsSite,
    coreSite,
    type,
    homedir
  }
}
