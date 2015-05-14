import com.google.protobuf.ByteString;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

public class ShuffleQueueJob extends Configured implements Tool {
    public static class HostsDumpMapper extends TableMapper<BytesWritable, BytesWritable> {
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
            buildQueue.reduce(hostCrc32, values, context);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            buildQueue.cleanup(context);
        }
    }

    class BuildQueueReducerHbase extends TableReducer<BytesWritable, BytesWritable, ImmutableBytesWritable> {
        public class BuildQueueHbase extends BuildQueue {
            private final byte[] CF = "f".getBytes();

            public void flushBuffer(Reducer.Context context, long timestamp) throws IOException, InterruptedException {
                for (Map.Entry<String, List<QueueRecordOuter.QueueRecord>> entry : buffer.entrySet()) {
                    List<QueueRecordOuter.QueueRecord> recordList = entry.getValue();
                    int size = 4 + recordList.size() * (20 + 4);
                    ByteBuffer bb = ByteBuffer.allocate(size);
                    bb.order(ByteOrder.BIG_ENDIAN);
                    String rk = String.format("%s_%d", entry.getKey(), timestamp);

                    bb.putInt(recordList.size());
                    String column = null;
                    for (QueueRecordOuter.QueueRecord record : recordList) {
                        if (column == null)
                            column = String.format("%.3f_%.3f", record.getStartInterval(), record.getEndInterval());
                        ByteString fingerprint = record.getFingerprint();
                        bb.put(fingerprint.asReadOnlyByteBuffer());
                        bb.putInt(record.getHostCrc32());
                    }
                    Put put = new Put(Bytes.toBytes(rk));
                    put.add(CF, column.getBytes(), bb.array());
                    context.write(null, put);
                }
                buffer.clear();
            }
        }

        final BuildQueueHbase buildQueue = new BuildQueueHbase();
        public void setup(Context context) throws IOException, InterruptedException {
            buildQueue.setup(context);
        }

        public void reduce(BytesWritable hostCrc32, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            buildQueue.reduce(hostCrc32, values, context);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            buildQueue.cleanup(context);
        }
    }

    public int run(String [] args) throws Exception {
        Configuration config = getConf();
        config.set("frontera.hbase.namespace", args[0]);
        config.set("shufflequeue.workdir", String.format("%s/hostsize", args[1]));

        // Get host sizes from queue
        boolean rHostSize = runBuildQueue();
        if (!rHostSize)
            throw new Exception("Error during host sizes calculation.");

        return 0;
    }

    private boolean runBuildQueue2() throws Exception {
        Configuration config = getConf();
        Job job = Job.getInstance(config, "BuildQueue Job");
        job.setJarByClass(ShuffleQueueJob.class);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        String sourceTable = String.format("%s:queue", config.get("frontera.hbase.namespace"));
        String targetTable = String.format("%s:new_queue", config.get("frontera.hbase.namespace"));
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
        job.setNumReduceTasks(0);
        return job.waitForCompletion(true);
    }

    private boolean runBuildQueue() throws Exception {
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
        Path outputPath = new Path(config.get("shufflequeue.workdir"));
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //SequenceFileOutputFormat.setOutputPath(job, outputPath);
        //SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Tool tool = new ShuffleQueueJob();
        System.exit(ToolRunner.run(config, tool, args));
    }
}
