package fr.xebia.ldi;

import com.jasongoodwin.monads.Try;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
public class CheckAllAvroSchemaTest extends ActionProviderTest {

    SchemaActionService service;

    @BeforeEach
    void setUp() throws IOException, RestClientException {
        Config config = ConfigFactory.parseResources("application-test.conf");
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

        schemaRegistryClient.register("subject1-key", schemaFromResource("avro/schema0.avsc"));
        schemaRegistryClient.register("subject2-key", schemaFromResource("avro/schema0.avsc"));
        schemaRegistryClient.register("subject1-value", schemaFromResource("avro/schema1.avsc"));
        schemaRegistryClient.register("subject2-value", schemaFromResource("avro/schema2.avsc"));

        service = new SchemaActionService(config, schemaRegistryClient);
    }

    @Test
    void testAllShouldCheckAllSchema() {
        List<KeyValuePair<String, Schema>> keyValuePairs = Arrays.asList(
                pair("subject1-key", schemaFromResource("avro/schema0.avsc")),
                pair("subject2-key", schemaFromResource("avro/schema0.avsc")),
                pair("subject1-value", schemaFromResource("avro/schema1.avsc")),
                pair("subject2-value", schemaFromResource("avro/schema2.avsc"))
        );

        Try<List<KeyValuePair<String, Boolean>>> triedSubjectsSchemasPair =
                service.testAllCompatibilities(keyValuePairs);

        assertTrue(triedSubjectsSchemasPair.isSuccess());
        assertTrue(triedSubjectsSchemasPair.getUnchecked().get(0).value);
        assertTrue(triedSubjectsSchemasPair.getUnchecked().get(1).value);
        assertTrue(triedSubjectsSchemasPair.getUnchecked().get(2).value);
        assertTrue(triedSubjectsSchemasPair.getUnchecked().get(3).value);
    }

    @Test
    void testAllShouldHandleEmptyList() {
        List<KeyValuePair<String, Schema>> keyValuePairs = Collections.emptyList();

        Try<List<KeyValuePair<String, Boolean>>> triedSubjectsSchemasPair =
                service.testAllCompatibilities(keyValuePairs);

        assertTrue(triedSubjectsSchemasPair.isSuccess());
        assertTrue(triedSubjectsSchemasPair.getUnchecked().isEmpty());
    }

    @Test
    void testAllShouldNotFailUnCompatibleSchema() {
        List<KeyValuePair<String, Schema>> keyValuePairs = Arrays.asList(
                pair("subject1-key", schemaFromResource("avro/schema0.avsc")),
                pair("subject1-value", schemaFromResource("avro/schema1.avsc")),

                pair("subject2-value", SchemaBuilder
                        .record("NotExistingSchema")
                        .namespace("fr.xebia.ldi.testfailure")
                                .fields()
                                .name("fake_filed_1").type().fixed("MD5").size(16).noDefault()
                                .name("fake_filed_2").type().nullable().stringType().noDefault()
                                .endRecord()
                )
        );

        Try<List<KeyValuePair<String, Boolean>>> triedSubjectsSchemasPair =
                service.testAllCompatibilities(keyValuePairs);

        assertTrue(triedSubjectsSchemasPair.isSuccess());
        assertTrue(triedSubjectsSchemasPair.getUnchecked().get(0).value);
        assertTrue(triedSubjectsSchemasPair.getUnchecked().get(1).value);

        assertFalse(triedSubjectsSchemasPair.getUnchecked().get(2).value);
    }
}