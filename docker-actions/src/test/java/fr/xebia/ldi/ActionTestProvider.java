/*
 *    Copyright 2020 Loïc DIVAD
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package fr.xebia.ldi;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

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
