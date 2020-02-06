package fr.xebia.ldi;

import fr.xebia.ldi.error.CompatibilityCheckException;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;

import static fr.xebia.ldi.ActionTestProvider.fileFromResource;
import static fr.xebia.ldi.ActionTestProvider.getPathFromResources;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Created by loicmdivad.
 */
public class ValidationIt {

    public static String address;
    public static Integer port;
    public static SchemaRegistryClient client;

    @ClassRule
    public static DockerComposeContainer environment =
            new DockerComposeContainer(new File(getPathFromResources("docker-compose-it.yml")))
                    .withExposedService("schema-registry_1", 8081, Wait.forHttp("/subjects").forStatusCode(200));

    public static void copyDirectory(String source, String destination) {
        try {
            FileUtils.forceMkdir(new File(fileFromResource(source).getParent(), destination));
            FileUtils.copyDirectory(fileFromResource(source), fileFromResource(destination));
        } catch (IOException throwable) {
            fail("Fail to move the required files for the test", throwable);
        }
    }

    @BeforeAll
    public static void setUp() throws IOException, RestClientException {
        environment.start();
        address = environment.getServiceHost("schema-registry_1", 8081);
        port = environment.getServicePort("schema-registry_1", 8081);

        client = new CachedSchemaRegistryClient(String.format("http://%s:%s", address, port), 20);

        client.register("subject1-key", ActionTestProvider.schemaFromResource("avro-0/schema0.avsc"));
        client.register("subject2-key", ActionTestProvider.schemaFromResource("avro-0/schema0.avsc"));
        client.register("subject1-value", ActionTestProvider.schemaFromResource("avro-0/schema1.avsc"));
        client.register("subject2-value", ActionTestProvider.schemaFromResource("avro-0/schema2.avsc"));

        System.setProperty("logback.configurationFile", getPathFromResources("logback-it.xml"));
        System.setProperty("config.file", getPathFromResources("application-it.conf"));
        System.setProperty("SCHEMA_REGISTRY_URL", String.format("http://%s:%s", address, port));
        System.setProperty("AVRO_SUBJECT_YAML", getPathFromResources(".") + "workspace/schema-it.yml");
        System.setProperty("AVRO_FILES_PATH", getPathFromResources(".") + "workspace");
    }


    @AfterEach
    public void tearDownEach() {
        try {

            FileUtils.deleteDirectory(fileFromResource("workspace"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    public static void tearDown() {
        environment.stop();
    }

    @Test
    public void validationShouldVerifyEveryElementInSubjectList() {
        String[] args = {};
        copyDirectory("avro-0", "workspace");

        try {
            Validation.main(args);
        } catch (Throwable throwable) {
            fail(throwable);
        }
    }

     @Test
    public void validationShouldPassOnEmptySubjectList() {
        String[] args = {};
        copyDirectory("avro-1", "workspace");

        try {
            Validation.main(args);
        } catch (Throwable throwable) {
            fail(throwable);
        }
    }

    @Test
    public void validationShouldFailOnNotCompatibleElementInSubjectList() {
        String[] args = {};
        copyDirectory("avro-2", "workspace");

        try {
            Validation.main(args);
            fail("The test should fail on a CompatibilityCheckException");
        } catch (Throwable throwable) {
            assertThat(throwable, instanceOf(CompatibilityCheckException.class));
        }
    }

    @Test
    @SetSystemProperty(key = "SCHEMA_REGISTRY_URL", value = "http://localhost:8081")
    public void validationShouldFailOnUnreachableRegistry() {
        String[] args = {};
        copyDirectory("avro-3", "workspace");

        try {
            Validation.main(args);
            fail("The test should fail on a ConnectException");
        } catch (Throwable throwable) {
            assertThat(throwable, instanceOf(ConnectException.class));
        }
    }
}