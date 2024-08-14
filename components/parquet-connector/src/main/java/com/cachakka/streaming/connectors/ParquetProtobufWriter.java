package com.cachakka.streaming.connectors;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import io.vavr.Tuple;
import io.vavr.Tuple5;
import io.vavr.control.Try;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopPositionOutputStream;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Created by a0b010p on 3/5/18.
 */
public class ParquetProtobufWriter<T extends Message> implements Writer<T> {
    private static final long serialVersionUID = 1L;
    private final int blockSize;
    private final int pageSize;
    private transient ProtoParquetWriter protoParquetWriter;
    private final Class<T> clazz;
    private transient Tuple5<Field,Field,Field,Method,Object> internalParquetWriterReflectionSignature;
    private transient HadoopPositionOutputStream internalFileStream;


    public ParquetProtobufWriter(Class<T> clazz, int blockSize, int pageSize) {
       this.clazz=clazz;
       this.blockSize = blockSize;
       this.pageSize = pageSize;
    }

    @Override
    public void open(FileSystem fs, Path file) throws IOException {
        //super.open(fs, file);
        protoParquetWriter = new ProtoParquetWriter<T>(
                file,
                this.clazz,
                CompressionCodecName.SNAPPY,
                blockSize, pageSize);
        // use reflection trick to access private fields
        // assert and fail fast if reflection failed
        internalParquetWriterReflectionSignature = Try.of(() -> {
            Class<?> clazz = TypeToken.of(protoParquetWriter.getClass()).getSupertype(ParquetWriter.class).getRawType();
            Field f = clazz.getDeclaredField("writer");
            f.setAccessible(true);
            Object writer = f.get(protoParquetWriter);
            Field f1 = writer.getClass().getDeclaredField("nextRowGroupSize");
            f1.setAccessible(true);
            Field f2 = writer.getClass().getDeclaredField("recordCountForNextMemCheck");
            f2.setAccessible(true);
            Field f3 = writer.getClass().getDeclaredField("recordCount");
            f3.setAccessible(true);
            Method m1 = writer.getClass().getDeclaredMethod("checkBlockSizeReached");
            m1.setAccessible(true);
            if (f1.get(writer).equals((long) blockSize)
                    && f2.get(writer).equals(100L)
                    && f3.get(writer).equals(0L)
                    && m1.invoke(writer) == null){
                return Tuple.of(f1,f2,f3,m1,writer);
            }
            throw new IllegalStateException("Failed to instantiate internal Parquet writer reflection signature");
        }).get();
        internalFileStream = Try.of(() -> {
            Class<?> clazz = internalParquetWriterReflectionSignature._5.getClass();
            Field f = clazz.getDeclaredField("parquetFileWriter");
            f.setAccessible(true);
            Object writer = f.get(internalParquetWriterReflectionSignature._5);
            Field f1 = writer.getClass().getDeclaredField("out");
            f1.setAccessible(true);
            HadoopPositionOutputStream stream = (HadoopPositionOutputStream) f1.get(writer);
            stream.sync();
            return stream;
        }).get();
    }

    @Override
    public void write(T obj) throws IOException {
        protoParquetWriter.write(obj);
    }

    @Override
    public void close() throws IOException {
        protoParquetWriter.close();
    }

    @Override
    public ParquetProtobufWriter<T> duplicate() {
        return new ParquetProtobufWriter(clazz, blockSize, pageSize);
    }

    @Override
    public long getPos() {
       return protoParquetWriter.getDataSize();
    }

    @Override
    public long flush() {
        return Try.of(() ->{
            internalParquetWriterReflectionSignature._1.set(internalParquetWriterReflectionSignature._5,0L);
            internalParquetWriterReflectionSignature._2.set(internalParquetWriterReflectionSignature._5,1L);
            internalParquetWriterReflectionSignature._4.invoke(internalParquetWriterReflectionSignature._5);
            if (internalParquetWriterReflectionSignature._3.get(internalParquetWriterReflectionSignature._5).equals(0L)) {
                internalFileStream.sync();
                return getPos();
            }
            throw new IllegalStateException("Failed to flush internal Parquet writer via its reflection signature");
        }).get();
    }

}
