package fr.xebia.ldi;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Created by loicmdivad.
 */
public abstract class ActionTestProvider {

    public static File fileFromResource(String name) {
        return new File(getPathFromResources(name));
    }

    public static Schema schemaFromResource(String fileName) {
        try {
            return new Schema.Parser().parse(fileFromResource(fileName));
        } catch (IOException e) {
            e.printStackTrace();
            return  Schema.create(Schema.Type.NULL);
        }
    }

    public static String getPathFromResources(String filename) {
        return Objects.requireNonNull(
                ActionTestProvider
                        .class
                        .getClassLoader()
                        .getResource(filename)
        )
                .getFile();
    }

    public static String byteToUTF8(ByteBuffer buffer) {
        return new String(buffer.array(), StandardCharsets.UTF_8);
    }
}
