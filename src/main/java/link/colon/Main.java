/**
 * COPYRIGHT 2020, PEERAPAT ASOKTUMMARUNGSRI
 */
package link.colon;

import lombok.val;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;

public final class Main {

    public static void main(String[] args) throws Exception {
        val schema = new AvroSchemaConverter().convert(UserRank.getClassSchema());

        val writer = AvroParquetWriter
                .<UserRank>builder(new Path("./example.parquet"))
                .withSchema(UserRank.getClassSchema())
                .withPageSize(65535)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();

        List.of(new UserRank(1, 3),
                new UserRank(2, 0),
                new UserRank(3, 100))
                .forEach(o -> write(writer, o));

    }

    public static void write(final ParquetWriter<UserRank> writer, final UserRank o) {
        try {
            writer.write(o);
        } catch (final IOException i) {
            i.printStackTrace();
        }
    }

}
