import com.google.protobuf.ByteString;
import com.scrapinghub.frontera.hbasestats.QueueRecordOuter;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public abstract class BuildQueue {
    Random RND = null;
    Map<String, List<QueueRecordOuter.QueueRecord>> buffer = null;

    public void setup(Reducer.Context context) throws IOException, InterruptedException {
        buffer = new HashMap<String, List<QueueRecordOuter.QueueRecord>>();
        RND = new Random();
    }

    private long getTimestamp() {
        return (System.currentTimeMillis() - 864000*1000 - RND.nextInt(1000)) * 1000;
    }

    public void reduce(BytesWritable hostCrc32, Iterable<BytesWritable> values, Reducer.Context context) throws IOException, InterruptedException {
        int count = 0;
        int flushCounter = 0;
        long timestamp = getTimestamp();

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
                timestamp += 15 * 60 * 1000 * 1000;
        }
    }

    public void cleanup(Reducer.Context context) throws IOException, InterruptedException {
        flushBuffer(context, getTimestamp());
    }

    protected void flushBuffer(Reducer.Context context, long timestamp) throws IOException, InterruptedException {

    }
}