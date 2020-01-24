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
public abstract class ActionProviderTest {

    protected File fileFromResource(String name) {
        return new File(Objects.requireNonNull(getClass().getClassLoader().getResource(name)).getPath());
    }

    protected Schema schemaFromResource(String fileName) {
        try {
            return new Schema.Parser().parse(fileFromResource(fileName));
        } catch (IOException e) {
            e.printStackTrace();
            return  Schema.create(Schema.Type.NULL);
        }
    }

    protected String byteToUTF8(ByteBuffer buffer) {
        return new String(buffer.array(), StandardCharsets.UTF_8);
    }

}
