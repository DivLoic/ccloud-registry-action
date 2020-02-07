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

import com.jasongoodwin.monads.Try;
import com.typesafe.config.ConfigFactory;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static fr.xebia.ldi.ActionTestProvider.fileFromResource;
import static fr.xebia.ldi.KeyValuePair.pair;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.*;

public class ParseAllAvroFilesTest {

    RegistryActionFunction service;

    @BeforeEach
    void setUp() {
        service = new RegistryActionFunction(ConfigFactory.parseResources("application-test.conf"));
    }

    @Test
    void parseAllShouldParseSchemasFromAFileList() {
        List<KeyValuePair<String, File>> subjectFilePairs = Arrays.asList(
                pair("subject1-key", fileFromResource("avro/schema0.avsc")),
                pair("subject2-key", fileFromResource("avro/schema0.avsc")),
                pair("subject1-value", fileFromResource("avro/schema1.avsc")),
                pair("subject2-value", fileFromResource("avro/schema2.avsc"))
        );

        Try<List<KeyValuePair<String, Schema>>> triedSubjectSchemaPairs = service.parseAll(subjectFilePairs);

        assertTrue(triedSubjectSchemaPairs.isSuccess());
        assertEquals(triedSubjectSchemaPairs.getUnchecked().get(1).value.getName(), "TaxiTripDropoff");
        assertEquals(triedSubjectSchemaPairs.getUnchecked().get(1).value.getNamespace(), "fr.xebia.gbildi");
    }

    @Test
    void parseAllShouldHandleEmptyList() {
        List<KeyValuePair<String, File>> emptySubjectFilePairs = Collections.emptyList();

        Try<List<KeyValuePair<String, Schema>>> triedSubjectSchemaPairs = service.parseAll(emptySubjectFilePairs);

        assertTrue(triedSubjectSchemaPairs.isSuccess());
        assertTrue(triedSubjectSchemaPairs.getUnchecked().isEmpty());
    }

    @Test
    void parseAllShouldFailOnUnExistingFiles() {
        List<KeyValuePair<String, File>> keyValuePairs = Collections.singletonList(
                pair("fake-subject-key", new File("fake/path/to/file.avsc"))
        );

        Try<List<KeyValuePair<String, Schema>>> triedSubjectSchemaPairs = service.parseAll(keyValuePairs);

        assertFalse(triedSubjectSchemaPairs.isSuccess());
        triedSubjectSchemaPairs
                .onFailure((throwable) -> assertThat(throwable, instanceOf(FileNotFoundException.class)));
    }

    @Test
    void parseAllShouldFailOnASingleCorruptedFile() {
        List<KeyValuePair<String, File>> subjectFilePairs = Arrays.asList(
                pair("subject1-key", fileFromResource("avro/schema0.avsc")),
                pair("subject1-value", fileFromResource("avro/schema1.avsc")),
                pair("corrupted-subject", fileFromResource("avro/corrupted.xml"))
        );

        Try<List<KeyValuePair<String, Schema>>> triedSubjectSchemaPairs = service.parseAll(subjectFilePairs);

        assertFalse(triedSubjectSchemaPairs.isSuccess());
        triedSubjectSchemaPairs
                .onFailure((throwable) -> assertThat(throwable, instanceOf(SchemaParseException.class)));
    }
}