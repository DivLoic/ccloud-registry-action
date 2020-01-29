package fr.xebia.ldi;

import com.jasongoodwin.monads.Try;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.scanner.ScannerException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;

import static fr.xebia.ldi.ActionTestProvider.byteToUTF8;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by loicmdivad.
 */
class RegistryActionFunctionProviderTest {

    RegistryActionFunction service;

    @BeforeEach
    void setUp() {
        URL schemaUrl = getClass().getClassLoader().getResource("schemas.yaml");

        Config config = ConfigFactory
                .parseResources("application-test.conf")
                .withValue("avro.subjects.yaml", ConfigValueFactory.fromAnyRef(requireNonNull(schemaUrl).getPath()));

        service = new RegistryActionFunction(config);
    }

    @Test
    void tryStreamFileSubjectsShouldLoadExistingFile() throws IOException {
        Try<FileInputStream> triedInputStream = service.tryStreamFileSubjects();

        assertTrue(triedInputStream.isSuccess());

        ByteBuffer buffer = ByteBuffer.wrap(new byte[64]);
        triedInputStream.getUnchecked().getChannel().read(buffer);
        assertEquals(byteToUTF8(buffer).split("\n")[0], "---");
        assertEquals(byteToUTF8(buffer).split("\n")[1], "schemas:");
    }

    @Test
    void tryStreamFileSubjectsShouldFailOnMissingFile() {
        Config config = ConfigFactory
                .parseResources("application-test.conf")
                .withValue("avro.subjects.yaml", ConfigValueFactory.fromAnyRef("/totally/fake/path/to.yaml"));

        service = new RegistryActionFunction(config);

        Try<FileInputStream> triedInputStream = service.tryStreamFileSubjects();

        assertFalse(triedInputStream.isSuccess());
        triedInputStream
                .onFailure((throwable) -> assertThat(throwable, instanceOf(FileNotFoundException.class)));
    }

    @Test
    void parseYamlShouldReturnASubjectSchemaEntry() throws FileNotFoundException {
        FileInputStream inputStream = new FileInputStream(
                new File(
                        requireNonNull(getClass().getClassLoader().getResource("schemas.yaml")).getPath()
                )
        );

        Try<List<SchemaList.SubjectEntry>> triedSubjectSchemaEntries = service.parseYaml(inputStream);

        assertTrue(triedSubjectSchemaEntries.isSuccess());

        assertEquals(triedSubjectSchemaEntries.getUnchecked().get(0).getFile(), "schema0.avsc");
        assertEquals(triedSubjectSchemaEntries.getUnchecked().get(0).getSubject(), "subject1-key");

        assertEquals(triedSubjectSchemaEntries.getUnchecked().get(1).getFile(), "schema0.avsc");
        assertEquals(triedSubjectSchemaEntries.getUnchecked().get(1).getSubject(), "subject2-key");
    }

    @Test
    void parseYamlShouldFailOnCorruptedFiles() throws FileNotFoundException {
        FileInputStream inputStream = new FileInputStream(
                new File(
                        requireNonNull(getClass().getClassLoader().getResource("schemas-yaml-corrupted.xml")).getPath()
                )
        );

        Try<List<SchemaList.SubjectEntry>> triedSubjectSchemaEntries = service.parseYaml(inputStream);

        assertFalse(triedSubjectSchemaEntries.isSuccess());
        triedSubjectSchemaEntries
                .onFailure((throwable) -> assertThat(throwable, instanceOf(ScannerException.class)));
    }
}