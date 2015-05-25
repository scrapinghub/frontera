import com.google.protobuf.ByteString;
import com.scrapinghub.frontera.hbasestats.HbaseQueue;
import com.scrapinghub.frontera.hbasestats.QueueRecordOuter;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MergeAndBuildQueue extends Configured implements Tool {
    public static class QueueDumpMapper extends TableMapper<Text, BytesWritable> {
        public static enum Counters {QUEUE_FINGERPRINTS_COUNT, METADATA_FINGERPRINTS_COUNT, URL_ABSENT_COUNT}
        HbaseQueue.QueueItem.Builder itemBuilder = HbaseQueue.QueueItem.newBuilder();

        private void queueMap(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            String rk = new String(row.get(), row.getOffset(), row.getLength(), "US-ASCII");
            String[] rkParts = rk.split("_");
            QueueRecordOuter.QueueRecord.Builder builder = QueueRecordOuter.QueueRecord.newBuilder();
            builder.setPartitionId(Integer.parseInt(rkParts[0]));
            builder.setTimestamp(Long.parseLong(rkParts[3]));

            for (Cell c : value.rawCells()) {
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
                    Text key = new Text(Hex.encodeHexString(fingerprint));
                    byte[] msg = builder.build().toByteArray();
                    ByteBuffer bb2 = ByteBuffer.allocate(msg.length + 1);
                    bb2.put((byte)1);
                    bb2.put(msg);
                    bb2.flip();

                    BytesWritable val = new BytesWritable(bb2.array());
                    context.write(key, val);
                    context.getCounter(Counters.QUEUE_FINGERPRINTS_COUNT).increment(1);
                }
            }
        }

        private void metadataMap(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            String rk = new String(row.get(), row.getOffset(), row.getLength(), "US-ASCII");
            Text key = new Text(rk);
            itemBuilder.clear();
            try {
                itemBuilder.setFingerprint(ByteString.copyFrom(Hex.decodeHex(rk.toCharArray())));
            } catch (DecoderException e) {
                throw new InterruptedException("Decoder exception.");
            }

            Cell cell = value.getColumnLatestCell("s".getBytes(), "score".getBytes());
            if (cell != null) {
                ByteBuffer bb = ByteBuffer.allocate(8);
                bb.order(ByteOrder.BIG_ENDIAN);
                bb.put(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                bb.flip();
                itemBuilder.setScore((float) bb.getDouble());
            }

            itemBuilder.setHostCrc32(0);

            cell = value.getColumnLatestCell("m".getBytes(), "url".getBytes());
            if (cell != null)
                itemBuilder.setUrlBytes(ByteString.copyFrom(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            else {
                context.getCounter(Counters.URL_ABSENT_COUNT).increment(1);
                return;
            }

            byte[] msg = itemBuilder.build().toByteArray();
            ByteBuffer bb2 = ByteBuffer.allocate(msg.length + 1);
            bb2.put((byte)2);
            bb2.put(msg);
            bb2.flip();

            BytesWritable val = new BytesWritable(bb2.array());
            context.write(key, val);
            context.getCounter(Counters.METADATA_FINGERPRINTS_COUNT).increment(1);
        }

        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            TableSplit split = (TableSplit)context.getInputSplit();
            String qualifier = split.getTable().getQualifierAsString();
            if (qualifier.equals("metadata")) {
                metadataMap(row, value, context);
                return;
            }

            if (qualifier.equals("new_queue")) {
                queueMap(row, value, context);
                return;
            }
            throw new InterruptedException("Unknown table name");
        }
    }

    public static class MergingReducer extends Reducer<Text, BytesWritable, IntWritable, BytesWritable> {
        public static enum Counters {EMPTY_RECORDS, NO_METADATA}
        Random RND = new Random();

        protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            HbaseQueue.QueueItem item = null;
            byte[] salt = new byte[4];
            List<QueueRecordOuter.QueueRecord> records = new ArrayList<QueueRecordOuter.QueueRecord>();
            for (BytesWritable blob: values) {
                ByteBuffer bb = ByteBuffer.wrap(blob.getBytes(), 0, blob.getLength());
                byte b = bb.get();
                byte[] buf = new byte[bb.remaining()];
                bb.get(buf);
                switch (b) {
                    case 1:
                        QueueRecordOuter.QueueRecord record = QueueRecordOuter.QueueRecord.parseFrom(buf);
                        records.add(record);
                        break;
                    case 2:
                        if (item != null)
                            throw new InterruptedException("We have second item, which shouldn't be possible.");
                        item = HbaseQueue.QueueItem.parseFrom(buf);
                        break;
                    default:
                        throw new InterruptedException("Wrong magic mark.");
                }
            }

            if (records.isEmpty()) {
                context.getCounter(Counters.EMPTY_RECORDS).increment(1);
                return;
            }

            if (item == null) {
                context.getCounter(Counters.NO_METADATA).increment(1);
                return;
            }

            QueueRecordOuter.QueueRecord record = records.get(0);
            QueueRecordOuter.QueueRecord.Builder builder = QueueRecordOuter.QueueRecord.newBuilder();
            builder.setFingerprint(item.getFingerprint());
            builder.setHostCrc32(record.getHostCrc32());
            builder.setPartitionId(record.getPartitionId());
            builder.setStartInterval(record.getStartInterval());
            builder.setEndInterval(record.getEndInterval());
            builder.setTimestamp(record.getTimestamp());
            builder.setUrl(item.getUrl());
            builder.setScore(item.getScore());
            BytesWritable val = new BytesWritable(builder.build().toByteArray());

            //RND.nextBytes(salt);
            //String saltStr = new String(Hex.encodeHex(salt, true));
            //Text outKey = new Text(String.format("%.3f_%s", item.getScore(), saltStr));
            IntWritable outKey = new IntWritable(record.getHostCrc32());
            context.write(outKey, val);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration config = getConf();
        config.set("frontera.hbase.namespace", "crawler");
        return runDump() ? 0 : -1;
    }

    private boolean runDump() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = getConf();
        Job job = Job.getInstance(config, "Dump queue Job");
        job.setJarByClass(MergeAndBuildQueue.class);
        List<Scan> scans = new ArrayList<Scan>();

        Scan scan_queue = new Scan();
        scan_queue.setCaching(2000);
        scan_queue.setCacheBlocks(false);
        scan_queue.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "crawler:new_queue".getBytes());
        scans.add(scan_queue);

        Scan scan_metadata = new Scan();
        scan_metadata.setCaching(2000);
        scan_metadata.setCacheBlocks(false);
        scan_metadata.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "crawler:metadata".getBytes());
        scans.add(scan_metadata);

        TableMapReduceUtil.initTableMapperJob(
                scans,
                QueueDumpMapper.class,
                Text.class,
                BytesWritable.class,
                job);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path("/user/sibiryakov/mergebuild/dumpqueue"));
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        job.setReducerClass(MergingReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Tool tool = new MergeAndBuildQueue();
        System.exit(ToolRunner.run(config, tool, args));
    }
}
