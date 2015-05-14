import com.google.protobuf.ByteString;
import com.scrapinghub.frontera.hbasestats.QueueRecordOuter;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public abstract class BuildQueue {
    long jobStartTimestamp = 0;
    Random RND = null;
    Map<String, List<QueueRecordOuter.QueueRecord>> buffer = null;

    public void setup(Reducer.Context context) throws IOException, InterruptedException {
        jobStartTimestamp = System.currentTimeMillis() * 1000 - 864000; // 10 days before now
        buffer = new HashMap<String, List<QueueRecordOuter.QueueRecord>>();
        RND = new Random();
    }

    public void reduce(BytesWritable hostCrc32, Iterable<BytesWritable> values, Reducer.Context context) throws IOException, InterruptedException {
        int count = 0;
        int flushCounter = 0;
        long timestamp = jobStartTimestamp;

        for (BytesWritable v : values) {
            QueueRecordOuter.QueueRecord record = QueueRecordOuter.QueueRecord.parseFrom(ByteString.copyFrom(v.getBytes(), 0, v.getLength()));
            String key = String.format("%d_%.2f_%.2f",
                    record.getPartitionId(),
                    record.getStartInterval(),
                    record.getEndInterval());

            List<QueueRecordOuter.QueueRecord> list = buffer.get(key);
            if (list == null)
                list = new ArrayList<QueueRecordOuter.QueueRecord>();

            list.add(record);
            buffer.put(key, list);
            count++;
            flushCounter++;

            if (flushCounter == 64) {
                flushBuffer(context, timestamp);
                flushCounter = 0;
            }

            if (count % 1024 == 0)
                timestamp += 15 * 60;
        }
    }

    public void cleanup(Reducer.Context context) throws IOException, InterruptedException {
        flushBuffer(context, jobStartTimestamp);
    }

    protected void flushBuffer(Reducer.Context context, long timestamp) throws IOException, InterruptedException {

    }
}