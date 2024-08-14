package com.cachakka.streaming.connectors;

import com.google.protobuf.Message;
import com.cachakka.streaming.core.utils.FlinkReflectionUtils;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ParquetProtobufSink<T extends Message> extends BucketingSink<T>{

    public ParquetProtobufSink(String basePath, long inactiveThresholdMillis){
        super(basePath);
        this.setWriter(new ParquetProtobufWriter<>(new FlinkReflectionUtils.ReflectionTypeExtractor<T>()
                .extractTypeInformation(this, ParquetProtobufSink.class,0).get().getTypeClass()
                , 50*1024*1024, 1*1024*1024))
                // 50 MB max row group size (which is also equal to 1 HDFS block), 1 MB page size (Parquet units)
                // note that the row groups will actually be smaller because a row group is finalized once flush() is called (on every checkpoint)
                .setBucketer(new DateTimeBucketer("yyyy-MM-dd-HH", "hr="))
                .setInactiveBucketCheckInterval(60 * 1000)
                .setInactiveBucketThreshold(inactiveThresholdMillis)
                .setPartPrefix("part")
                .setBatchSize(500*1024*1024)
                 //350MB max bucket part file size (HDFS units)
                 // note that the batches will actually be smaller because batch file may be closed earlier (when bucket is finished and is detected to be inactive)
                .setPendingSuffix(".pending")
                .setInProgressSuffix(".in-progress")
                .setValidLengthSuffix(".valid");
    }

    /**
     * Partitions data into hourly buckets
     * @param <T>
     */
    public static class DateTimeBucketer<T> implements Bucketer<T> {
        private static final long serialVersionUID = -3758499756501574129L;
        private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";
        private final String formatString;
        private transient SimpleDateFormat dateFormatter;
        private static final String DEFAULT_PREIFX = "";
        private String prefix = "";

        /**
         * Creates a new {@code DateTimeBucketer} with format string {@code "yyyy-MM-dd--HH"}.
         */
        public DateTimeBucketer() {
            this(DEFAULT_FORMAT_STRING);
        }

        public DateTimeBucketer(String formatString, String prefix) {
            this.prefix = prefix;
            this.formatString = formatString;
            this.dateFormatter = new SimpleDateFormat(formatString);
        }

        /**
         * Creates a new {@code DateTimeBucketer} with the given date/time format string.
         *
         * @param formatString The format string that will be given to {@code SimpleDateFormat} to determine
         *                     the bucket path.
         */
        public DateTimeBucketer(String formatString) {
            this(formatString, DEFAULT_PREIFX);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();

            this.dateFormatter = new SimpleDateFormat(formatString);
        }

        @Override
        public Path getBucketPath(Clock clock, Path basePath, T element) {
            String newDateTimeString = dateFormatter.format(new Date(clock.currentTimeMillis()));
            return new Path(basePath + "/" + prefix + newDateTimeString);
        }

        @Override
        public String toString() {
            return "DateTimeBucketer{" +
                    "formatString='" + formatString + '\'' +
                    '}';
        }
    }
}
