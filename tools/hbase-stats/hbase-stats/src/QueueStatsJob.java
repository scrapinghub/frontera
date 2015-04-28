import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class QueueStatsJob extends Configured implements Tool {
    public static class FingerpintsDumpMapper extends TableMapper<BytesWritable, NullWritable> {

        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            for (Cell c: value.rawCells()) {
                ByteArrayInputStream bis = new ByteArrayInputStream(c.getValueArray(), c.getValueOffset(), c.getValueLength());
                DataInputStream input = new DataInputStream(bis);
                byte[] fingerprint = new byte[20];
                int blobsCount = input.readInt();
                for (int i = 0; i < blobsCount; i++) {
                    if (input.read(fingerprint) == -1)
                        break;
                    if (input.readInt() == -1)
                        break;
                    BytesWritable key = new BytesWritable(fingerprint);
                    context.write(key, NullWritable.get());
                }
            }
        }
    }

    public static class MetadataFilteringMapper extends TableMapper<Text, Text> {
        private BloomFilter<BytesWritable> fingerprints = null;
        private HashMap<String, Integer> hostStats = null;

        public static enum Counters {MATCHED_RECORDS}

        protected void setup(Context context) throws IOException, InterruptedException {
            fingerprints = new BloomFilter<BytesWritable>(0.001, 100000000);
            hostStats = new HashMap<String, Integer>();

            Configuration config = new HdfsConfiguration();
            URI dfsUri = null;
            try {
                dfsUri = new URI(config.get("fs.defaultFS"));
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }

            FileSystem fs = new DistributedFileSystem();
            fs.initialize(dfsUri, config);

            Path filepath = new Path(context.getConfiguration().get("fingerprints.output"));
            for (FileStatus fileStatus: fs.listStatus(filepath)) {
                if (fileStatus.isFile() && fileStatus.getLen() > 0) {
                    SequenceFile.Reader reader = null;
                    System.err.format("Loading %s\n", fileStatus.getPath().toString());
                    try {
                        reader = new SequenceFile.Reader(config,
                                SequenceFile.Reader.file(fileStatus.getPath()));
                    } catch (IOException ioe) {
                        ioe.printStackTrace(System.err);
                        continue;
                    }
                    BytesWritable key = new BytesWritable();
                    while (reader.next(key))
                        fingerprints.add(key);
                    reader.close();
                }
            }
            fs.close();
        }

        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            String keyEncoded = new String(row.get(), row.getOffset(), row.getLength());
            byte[] decoded = null;
            try {
                decoded = Hex.decodeHex(keyEncoded.toCharArray());
            } catch (DecoderException e) {
                e.printStackTrace(System.err);
            }

            BytesWritable key = new BytesWritable(decoded, 20);
            if (fingerprints.contains(key)) {
                context.getCounter(Counters.MATCHED_RECORDS).increment(1);

                for (Cell c: value.rawCells()) {
                    String qualifier = new String(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
                    if (qualifier.equals("url")) {
                        String rawUrl = new String(c.getValueArray(), c.getValueOffset(), c.getValueLength());
                        try {
                            URL url = new URL(rawUrl);
                            int count = 0;
                            if (hostStats.containsKey(url.getHost()))
                                count = hostStats.get(url.getHost());
                            hostStats.put(url.getHost(), count + 1);
                        } catch (MalformedURLException murl) {
                            murl.printStackTrace(System.err);
                            continue;
                       }
                       break;
                    }
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration config = new HdfsConfiguration();
            URI dfsUri = null;
            try {
                dfsUri = new URI(config.get("fs.defaultFS"));
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }

            FileSystem fs = new DistributedFileSystem();
            fs.initialize(dfsUri, config);
            Configuration jobConf = context.getConfiguration();
            Path filepath = new Path(jobConf.get("metadata.output") + "/result_" +
                    context.getTaskAttemptID().getTaskID().toString() + ".txt");
            FSDataOutputStream outStream = fs.create(filepath, true);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outStream));
            for (Map.Entry<String, Integer> entry: hostStats.entrySet())
                writer.write(String.format("%s\t%d\n", entry.getKey(), entry.getValue()));
            writer.close();
            fs.close();
        }
    }

    public int run(String [] args) throws Exception {
        Configuration config = getConf();
        for (int partitionId = 0; partitionId < Integer.parseInt(args[2]); partitionId++) {
            config.set("frontera.hbase.namespace", args[0]);
            config.set("fingerprints.output", String.format("%s/fingerprints_%d", args[1], partitionId));
            config.set("metadata.output", String.format("%s_%d", args[1], partitionId));
            Job job = createFingerprintsDumpJob(partitionId, getConf());
            job.waitForCompletion(true);

            Job job2 = createMetadataFilteringJob(getConf());
            job2.waitForCompletion(true);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Tool tool = new QueueStatsJob();
        System.exit(ToolRunner.run(config, tool, args));
    }

    private static Job createMetadataFilteringJob(Configuration config) throws IOException {
        Job job = Job.getInstance(config, "Frontera queue stats, metadata filtering");
        job.setJarByClass(QueueStatsJob.class);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        String tableName = String.format("%s:metadata", config.get("frontera.hbase.namespace"));
        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                MetadataFilteringMapper.class,
                BytesWritable.class,
                NullWritable.class,
                job);
        Path outputPath = new Path(config.get("metadata.output"));
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setNumReduceTasks(0);
        return job;
    }

    private static Job createFingerprintsDumpJob(int partitionId, Configuration config) throws IOException {
        Job job = Job.getInstance(config, String.format("Frontera queue stats, dumping fingerprints partition %d", partitionId));
        job.setJarByClass(QueueStatsJob.class);
        String strPrefix = String.format("%d_", partitionId);
        byte[] prefix = ArrayUtils.subarray(strPrefix.getBytes(), 0, strPrefix.length());
        Filter filter = new PrefixFilter(prefix);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setFilter(filter);

        String tableName = String.format("%s:queue", config.get("frontera.hbase.namespace"));
        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                FingerpintsDumpMapper.class,
                BytesWritable.class,
                NullWritable.class,
                job);
        Path outputPath = new Path(config.get("fingerprints.output"));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputPath);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        return job;
    }
}
