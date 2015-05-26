import com.google.protobuf.ByteString;
import com.scrapinghub.frontera.hbasestats.HbaseQueue;
import com.scrapinghub.frontera.hbasestats.QueueRecordOuter;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;
import org.msgpack.packer.Packer;
import org.msgpack.template.Template;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.msgpack.template.Templates.tList;

public class ShuffleQueueJob extends Configured implements Tool {
    public static class HostsDumpMapper extends TableMapper<BytesWritable, BytesWritable> {
        public static enum Counters {FINGERPRINTS_COUNT}

        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            String rk = new String(row.get(), row.getOffset(), row.getLength(), "US-ASCII");
            String[] rkParts = rk.split("_");
            QueueRecordOuter.QueueRecord.Builder builder = QueueRecordOuter.QueueRecord.newBuilder();
            builder.setPartitionId(Integer.parseInt(rkParts[0]));
            builder.setTimestamp(Long.parseLong(rkParts[3]));

            for (Cell c: value.rawCells()) {
                String column = new String(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength(), "US-ASCII");
                String[] intervals = column.split("_");
                builder.setStartInterval(Float.parseFloat(intervals[0]));
                builder.setEndInterval(Float.parseFloat(intervals[1]));

                ByteArrayInputStream bis = new ByteArrayInputStream(c.getValueArray(), c.getValueOffset(), c.getValueLength());
                DataInputStream input = new DataInputStream(bis);
                byte[] fingerprint = new byte[20];
                byte[] hostCrc32 = new byte[4];
                int blobsCount = input.readInt();
                for (int i = 0; i < blobsCount; i++) {
                    if (input.read(fingerprint) == -1)
                        break;
                    builder.setFingerprint(ByteString.copyFrom(fingerprint));
                    if (input.read(hostCrc32) == -1)
                        break;
                    builder.setHostCrc32(ByteBuffer.wrap(hostCrc32).getInt());
                    BytesWritable key = new BytesWritable(hostCrc32);
                    BytesWritable val = new BytesWritable(builder.build().toByteArray());
                    context.write(key, val);
                    context.getCounter(Counters.FINGERPRINTS_COUNT).increment(1);
                }
            }
        }
    }

    public static class BuildQueueReducerCommon extends Reducer<BytesWritable, BytesWritable, Text, Text> {
        public class BuildQueueCommon extends BuildQueue {
            public void flushBuffer(Reducer.Context context, long timestamp) throws IOException, InterruptedException {
                for (Map.Entry<String, List<QueueRecordOuter.QueueRecord>> entry : buffer.entrySet()) {
                    List<QueueRecordOuter.QueueRecord> recordList = entry.getValue();
                    int count = recordList.size();
                    String rk = String.format("%s_%d", entry.getKey(), timestamp);
                    StringBuffer sBuffer = new StringBuffer();
                    sBuffer.append(count);
                    byte[] fingerprint = new byte[20];
                    for (QueueRecordOuter.QueueRecord record : entry.getValue()) {
                        record.getFingerprint().copyTo(fingerprint, 0);
                        sBuffer.append("\t");
                        sBuffer.append(Hex.encodeHexString(fingerprint) + "," + record.getHostCrc32());
                    }
                    context.write(new Text(rk), new Text(sBuffer.toString()));
                }
                buffer.clear();
            }
        }

        final BuildQueueCommon buildQueue = new BuildQueueCommon();

        public void setup(Context context) throws IOException, InterruptedException {
            buildQueue.setup(context);
        }

        public void reduce(BytesWritable hostCrc32, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            buildQueue.reduce(values, context);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            buildQueue.cleanup(context);
        }
    }

    public static class BuildQueueReducerHbase extends TableReducer<IntWritable, BytesWritable, ImmutableBytesWritable> {
        public static enum Counters {ITEMS_PRODUCED, ROWS_PUT}

        public class BuildQueueHbaseProtobuf extends BuildQueue {
            private final byte[] CF = "f".getBytes();
            private byte[] salt = new byte[4];
            private HbaseQueue.QueueItem.Builder item = HbaseQueue.QueueItem.newBuilder();
            private HbaseQueue.QueueCell.Builder cell = HbaseQueue.QueueCell.newBuilder();

            public void flushBuffer(Reducer.Context context, long timestamp) throws IOException, InterruptedException {
                for (Map.Entry<String, List<QueueRecordOuter.QueueRecord>> entry : buffer.entrySet()) {
                    List<QueueRecordOuter.QueueRecord> recordList = entry.getValue();
                    cell.clear();
                    RND.nextBytes(salt);
                    String saltStr = new String(Hex.encodeHex(salt, true));
                    String rk = String.format("%s_%d_%s", entry.getKey(), timestamp, saltStr);

                    String column = null;
                    for (QueueRecordOuter.QueueRecord record : recordList) {
                        if (column == null)
                            column = String.format("%.3f_%.3f", record.getStartInterval(), record.getEndInterval());
                        item.clear();
                        item.setFingerprint(record.getFingerprint());
                        item.setHostCrc32(record.getHostCrc32());
                        item.setScore(record.getScore());
                        item.setUrl(record.getUrl());

                        cell.addItems(item.build());
                        context.getCounter(Counters.ITEMS_PRODUCED).increment(1);
                    }
                    Put put = new Put(Bytes.toBytes(rk));
                    put.add(CF, column.getBytes(), cell.build().toByteArray());
                    context.write(null, put);
                    context.getCounter(Counters.ROWS_PUT).increment(1);
                }
                buffer.clear();
            }
        }

        @Message
        public static class QueueItem {
            public byte[] fingerprint;
            public int hostCrc32;
            public String url;
            public float score;
        }

        public class BuildQueueHbaseMsgPack extends BuildQueue {
            private final byte[] CF = "f".getBytes();
            private byte[] salt = new byte[4];
            private MessagePack messagePack = new MessagePack();

            public void flushBuffer(Reducer.Context context, long timestamp) throws IOException, InterruptedException {
                for (Map.Entry<String, List<QueueRecordOuter.QueueRecord>> entry : buffer.entrySet()) {
                    List<QueueRecordOuter.QueueRecord> recordList = entry.getValue();
                    RND.nextBytes(salt);
                    String saltStr = new String(Hex.encodeHex(salt, true));
                    String rk = String.format("%s_%d_%s", entry.getKey(), timestamp, saltStr);

                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    Packer packer = messagePack.createPacker(bos);
                    String column = null;
                    QueueItem item = new QueueItem();
                    for (QueueRecordOuter.QueueRecord record : recordList) {
                        if (column == null)
                            column = String.format("%.3f_%.3f", record.getStartInterval(), record.getEndInterval());
                        item.fingerprint = record.getFingerprint().toByteArray();
                        item.hostCrc32 = record.getHostCrc32();
                        item.url = record.getUrl();
                        item.score = record.getScore();

                        packer.write(item);
                        context.getCounter(Counters.ITEMS_PRODUCED).increment(1);
                    }

                    Put put = new Put(Bytes.toBytes(rk));
                    put.add(CF, column.getBytes(), bos.toByteArray());
                    context.write(null, put);
                    context.getCounter(Counters.ROWS_PUT).increment(1);
                }
                buffer.clear();
            }
        }

        final BuildQueueHbaseMsgPack buildQueue = new BuildQueueHbaseMsgPack();
        public void setup(Context context) throws IOException, InterruptedException {
            buildQueue.setup(context);
        }

        public void reduce(IntWritable hostCrc32, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            buildQueue.reduce(values, context);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            buildQueue.cleanup(context);
        }
    }

    public int run(String [] args) throws Exception {
        Configuration config = getConf();
        config.set("frontera.hbase.namespace", args[0]);

        boolean rJob = runBuildQueueFromSequenceFile(args);
        if (!rJob)
            throw new Exception("Error during queue building.");
        return 0;
    }

    private boolean runBuildQueueHbase(String[] args) throws Exception {
        Configuration config = getConf();
        Job job = Job.getInstance(config, "BuildQueue Job");
        job.setJarByClass(ShuffleQueueJob.class);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        String sourceTable = String.format("%s:%s", config.get("frontera.hbase.namespace"), args[1]);
        String targetTable = String.format("%s:%s", config.get("frontera.hbase.namespace"), args[2]);
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,
                scan,
                HostsDumpMapper.class,
                BytesWritable.class,
                BytesWritable.class,
                job);

        TableMapReduceUtil.initTableReducerJob(
                targetTable,
                BuildQueueReducerHbase.class,
                job);
        return job.waitForCompletion(true);
    }

    private boolean runBuildQueueFromSequenceFile(String[] args) throws Exception {
        Configuration config = getConf();
        Job job = Job.getInstance(config, "BuildQueue Job");
        job.setJarByClass(ShuffleQueueJob.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));

        String targetTable = String.format("%s:%s", config.get("frontera.hbase.namespace"), args[2]);
        TableMapReduceUtil.initTableReducerJob(
                targetTable,
                BuildQueueReducerHbase.class,
                job);

        return job.waitForCompletion(true);
    }

    private boolean runBuildQueueDebug(String[] args) throws Exception {
        Configuration config = getConf();
        Job job = Job.getInstance(config, "BuildQueue Job (debug)");
        job.setJarByClass(ShuffleQueueJob.class);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        String tableName = String.format("%s:queue", config.get("frontera.hbase.namespace"));
        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                HostsDumpMapper.class,
                BytesWritable.class,
                BytesWritable.class,
                job);

        job.setReducerClass(BuildQueueReducerCommon.class);
        Path outputPath = new Path(String.format("%s/hostsize", args[1]));
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Tool tool = new ShuffleQueueJob();
        System.exit(ToolRunner.run(config, tool, args));
    }
}