package fr.xebia.ldi;

import com.jasongoodwin.monads.Try;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static fr.xebia.ldi.KeyValuePair.pair;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Created by loicmdivad.
 */
public class CheckAllAvroSchemaTestProvider extends ActionTestProvider {

    RegistryActionFunction service;

    @BeforeEach
    void setUp() throws IOException, RestClientException {
        Config config = ConfigFactory.parseResources("application-test.conf");
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

        schemaRegistryClient.register("subject1-key", schemaFromResource("avro/schema0.avsc"));
        schemaRegistryClient.register("subject2-key", schemaFromResource("avro/schema0.avsc"));
        schemaRegistryClient.register("subject1-value", schemaFromResource("avro/schema1.avsc"));
        schemaRegistryClient.register("subject2-value", schemaFromResource("avro/schema2.avsc"));

        service = new RegistryActionFunction(config, schemaRegistryClient);
    }

    @Test
    void testAllShouldCheckAllSchemas() {
        List<KeyValuePair<String, Schema>> subjectSchemaPairs = Arrays.asList(
                pair("subject1-key", schemaFromResource("avro/schema0.avsc")),
                pair("subject2-key", schemaFromResource("avro/schema0.avsc")),
                pair("subject1-value", schemaFromResource("avro/schema1.avsc")),
                pair("subject2-value", schemaFromResource("avro/schema2.avsc"))
        );

        Try<List<KeyValuePair<String, Boolean>>> triedSubjectValidationPairs =
                service.testAllCompatibilities(subjectSchemaPairs);

        assertTrue(triedSubjectValidationPairs.isSuccess());
        assertTrue(triedSubjectValidationPairs.getUnchecked().get(0).value);
        assertTrue(triedSubjectValidationPairs.getUnchecked().get(1).value);
        assertTrue(triedSubjectValidationPairs.getUnchecked().get(2).value);
        assertTrue(triedSubjectValidationPairs.getUnchecked().get(3).value);
    }

    @Test
    void testAllShouldHandleEmptyList() {
        List<KeyValuePair<String, Schema>> emptySubjectSchemaPairs = Collections.emptyList();

        Try<List<KeyValuePair<String, Boolean>>> triedSubjectValidationPairs =
                service.testAllCompatibilities(emptySubjectSchemaPairs);

        assertTrue(triedSubjectValidationPairs.isSuccess());
        assertTrue(triedSubjectValidationPairs.getUnchecked().isEmpty());
    }

    @Test
    void testAllShouldNotFailUnCompatibleSchema() {
        List<KeyValuePair<String, Schema>> subjectSchemaPairs = Arrays.asList(
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

        Try<List<KeyValuePair<String, Boolean>>> triedSubjectValidationPairs =
                service.testAllCompatibilities(subjectSchemaPairs);

        assertTrue(triedSubjectValidationPairs.isSuccess());
        assertTrue(triedSubjectValidationPairs.getUnchecked().get(0).value);
        assertTrue(triedSubjectValidationPairs.getUnchecked().get(1).value);

        assertFalse(triedSubjectValidationPairs.getUnchecked().get(2).value);
    }
}