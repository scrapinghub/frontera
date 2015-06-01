import com.google.protobuf.ByteString;
import com.scrapinghub.frontera.hbasestats.MetadataOuter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class MetadataUpdateJob extends Configured implements Tool {
    public enum Counters {URL_ABSENT, MALFORMED_URL}
    public static class MetadataMapper extends TableMapper<BytesWritable, BytesWritable> {
        MessageDigest MD5 = null;
        PureJavaCrc32 crc32calc = new PureJavaCrc32();
        MetadataOuter.HBaseCell.Builder gpbCell = MetadataOuter.HBaseCell.newBuilder();
        MetadataOuter.HBaseRow.Builder gpbRow = MetadataOuter.HBaseRow.newBuilder();
        final byte[] scoringCF = "s".getBytes();

        public void setup(Context context) throws IOException, InterruptedException {
            try {
                MD5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String url = getUrl(value);
            if (url == null) {
                context.getCounter(Counters.URL_ABSENT).increment(1);
                return;
            }

            URL urlParsed = null;
            try {
                urlParsed = new URL(url);
            } catch (MalformedURLException exc) {
                context.getCounter(Counters.MALFORMED_URL).increment(1);
                return;
            }
            byte[] hostname = urlParsed.getHost().getBytes();
            crc32calc.reset();
            crc32calc.update(hostname, 0, hostname.length);

            int hostChecksum = (int)crc32calc.getValue();
            String documentFingerprint = urlParsed.getPath() + urlParsed.getQuery() + urlParsed.getRef();

            MD5.update(documentFingerprint.getBytes());
            ByteBuffer fingerprint = ByteBuffer.allocate(20);
            fingerprint.putInt(hostChecksum);
            fingerprint.put(MD5.digest());
            fingerprint.flip();

            gpbRow.clear();
            for (Cell c: value.rawCells()) {
                gpbCell.clear();
                String qualifier = new String(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
                ByteBuffer val = ByteBuffer.wrap(c.getValueArray(), c.getValueOffset(), c.getValueLength());
                long revertedTimestamp = Long.MAX_VALUE - c.getTimestamp();

                gpbCell.setColumn(ByteString.copyFrom(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength()));
                gpbCell.setFamily(ByteString.copyFrom(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength()));
                gpbCell.setTimestamp(revertedTimestamp);
                gpbCell.setValue(ByteString.copyFrom(val));

                if (qualifier.equals("state")) {
                    gpbCell.setFamily(ByteString.copyFrom(scoringCF));
                    gpbCell.setColumn(ByteString.copyFrom("state", "US-ASCII"));
                }

                if (qualifier.equals("score")) {
                    val.rewind();
                    float score = (float) val.getDouble();
                    ByteBuffer bb = ByteBuffer.allocate(4);
                    bb.putFloat(score);
                    gpbCell.setFamily(ByteString.copyFrom(scoringCF));
                    gpbCell.setColumn(ByteString.copyFrom("score", "US-ASCII"));
                    gpbCell.setValue(ByteString.copyFrom(bb));
                }
                gpbRow.addCells(gpbCell);
            }
            context.write(new BytesWritable(fingerprint.array()), new BytesWritable(gpbRow.build().toByteArray()));
        }

        private String getUrl(Result value) {
            String url = null;
            for (Cell c: value.rawCells()) {
                String qualifier = new String(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
                if (qualifier.equals("url")) {
                    url = new String(c.getValueArray(), c.getValueOffset(), c.getValueLength());
                    break;
                }
            }
            return url;
        }
    }

    public static class MetadataReducer extends TableReducer<BytesWritable, BytesWritable, ImmutableBytesWritable> {

        public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            ImmutableBytesWritable hbKey = new ImmutableBytesWritable(key.getBytes(), 0, key.getLength());
            Put put = new Put(key.getBytes(), 0, key.getLength());
            for (BytesWritable val: values) {
                ByteArrayInputStream bis = new ByteArrayInputStream(val.getBytes(), 0, val.getLength());
                MetadataOuter.HBaseRow row = MetadataOuter.HBaseRow.parseFrom(bis);

                for (MetadataOuter.HBaseCell cell: row.getCellsList()) {
                    put.add(cell.getFamily().toByteArray(), cell.getColumn().toByteArray(),
                            cell.getTimestamp(), cell.getValue().toByteArray());
                }
                context.write(hbKey, put);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Tool tool = new MetadataUpdateJob();
        System.exit(ToolRunner.run(config, tool, args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration config = getConf();

        Job job = Job.getInstance(config, "Frontera metadata update");
        job.setJarByClass(MetadataUpdateJob.class);
        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.setCacheBlocks(false);

        String tableName = String.format("%s:metadata", "crawler");
        String targetTable = "crawler:metadata2";
        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                MetadataMapper.class,
                BytesWritable.class,
                BytesWritable.class,
                job);

        TableMapReduceUtil.initTableReducerJob(
                targetTable,
                MetadataReducer.class,
                job);
        return job.waitForCompletion(true) ? 0 : -1;
    }
}
