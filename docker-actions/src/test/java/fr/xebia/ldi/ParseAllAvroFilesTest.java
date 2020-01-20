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

import static fr.xebia.ldi.KeyValuePair.pair;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by loicmdivad.
 */
public class ParseAllAvroFilesTest extends ActionProviderTest {

    SchemaActionService service;

    @BeforeEach
    void setUp() {
        service = new SchemaActionService(ConfigFactory.parseResources("application-test.conf"));
    }

    @Test
    void parseAllTest() {
        List<KeyValuePair<String, File>> keyValuePairs = Arrays.asList(
                pair("subject1-key", fileResource("avro/schema0.avsc")),
                pair("subject2-key", fileResource("avro/schema0.avsc")),
                pair("subject1-value", fileResource("avro/schema1.avsc")),
                pair("subject2-value", fileResource("avro/schema2.avsc"))
        );

        Try<List<KeyValuePair<String, Schema>>> triedSubjectsSchemasPair = service.parseAll(keyValuePairs);

        assertTrue(triedSubjectsSchemasPair.isSuccess());
        assertEquals(triedSubjectsSchemasPair.getUnchecked().get(1).value.getName(), "TaxiTripDropoff");
        assertEquals(triedSubjectsSchemasPair.getUnchecked().get(1).value.getNamespace(), "fr.xebia.gbildi");
    }

    @Test
    void parseAllShouldHandleEmptyList() {
        List<KeyValuePair<String, File>> keyValuePairs = Collections.emptyList();

        Try<List<KeyValuePair<String, Schema>>> triedSubjectsSchemasPair = service.parseAll(keyValuePairs);

        assertTrue(triedSubjectsSchemasPair.isSuccess());
        assertTrue(triedSubjectsSchemasPair.getUnchecked().isEmpty());
    }

    @Test
    void parseAllShouldFailOnUnExistingFiles() {
        List<KeyValuePair<String, File>> keyValuePairs = Collections.singletonList(
                pair("fake-subject-key", new File("fake/path/to/file.avsc"))
        );

        Try<List<KeyValuePair<String, Schema>>> triedSubjectsSchemasPair = service.parseAll(keyValuePairs);

        assertFalse(triedSubjectsSchemasPair.isSuccess());
        triedSubjectsSchemasPair
                .onFailure((throwable) -> assertThat(throwable, instanceOf(FileNotFoundException.class)));
    }

    @Test
    void parseAllShouldFailOnASingleCorruptedFile() {
        List<KeyValuePair<String, File>> keyValuePairs = Arrays.asList(
                pair("subject1-key", fileResource("avro/schema0.avsc")),
                pair("subject1-value", fileResource("avro/schema1.avsc")),
                pair("corrupted-subject", fileResource("avro/corrupted.xml"))
        );

        Try<List<KeyValuePair<String, Schema>>> triedSubjectsSchemasPair = service.parseAll(keyValuePairs);

        assertFalse(triedSubjectsSchemasPair.isSuccess());
        triedSubjectsSchemasPair
                .onFailure((throwable) -> assertThat(throwable, instanceOf(SchemaParseException.class)));
    }
}