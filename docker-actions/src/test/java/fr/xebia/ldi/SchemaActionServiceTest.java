package fr.xebia.ldi;

import com.jasongoodwin.monads.Try;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.avro.SchemaParseException;
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
import java.util.Objects;


import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by loicmdivad.
 */
class SchemaActionServiceTest extends ActionProviderTest {

    SchemaActionService service;

    @BeforeEach
    void setUp() {
        URL schemaUrl = getClass().getClassLoader().getResource("schemas.yaml");

        Config config = ConfigFactory
                .parseResources("application-test.conf")
                .withValue("avro.subjects.yaml", ConfigValueFactory.fromAnyRef(requireNonNull(schemaUrl).getPath()));

        service = new SchemaActionService(config);
    }

    @Test
    void tryLoadingSubjectsShouldLoadExistingFile() throws IOException {
        Try<FileInputStream> triedSubjects = service.tryLoadingSubjects();

        assertTrue(triedSubjects.isSuccess());

        ByteBuffer buffer = ByteBuffer.wrap(new byte[64]);
        triedSubjects.getUnchecked().getChannel().read(buffer);
        assertEquals(byteToUTF8(buffer).split("\n")[0], "---");
        assertEquals(byteToUTF8(buffer).split("\n")[1], "schemas:");
    }

    @Test
    void tryLoadingSubjectsShouldFailOnMissingFile() {
        Config config = ConfigFactory
                .parseResources("application-test.conf")
                .withValue("avro.subjects.yaml", ConfigValueFactory.fromAnyRef("/totally/fake/path/to.yaml"));

        service = new SchemaActionService(config);

        Try<FileInputStream> triedSubjects = service.tryLoadingSubjects();

        assertFalse(triedSubjects.isSuccess());
        triedSubjects
                .onFailure((throwable) -> assertThat(throwable, instanceOf(FileNotFoundException.class)));
    }


    @Test
    void parseYamlShouldFailOnCorruptedFiles() throws FileNotFoundException {
        FileInputStream stream = new FileInputStream(
                new File(
                        requireNonNull(getClass().getClassLoader().getResource("schemas-yaml-corrupted.xml")).getPath()
                )
        );

        Try<List<SchemaList.SubjectEntry>> listTry = service.parseYaml(stream);

        assertFalse(listTry.isSuccess());
        listTry
                .onFailure((throwable) -> assertThat(throwable, instanceOf(ScannerException.class)));
    }

    @Test
    void parseYamlShouldReturnASubjectSchemaPair() throws FileNotFoundException {
        FileInputStream stream = new FileInputStream(
                new File(
                        requireNonNull(getClass().getClassLoader().getResource("schemas.yaml")).getPath()
                )
        );

        Try<List<SchemaList.SubjectEntry>> listTry = service.parseYaml(stream);

        assertTrue(listTry.isSuccess());

        assertEquals(listTry.getUnchecked().get(0).getFile(), "schema0.avsc");
        assertEquals(listTry.getUnchecked().get(0).getSubject(), "subject1-key");

        assertEquals(listTry.getUnchecked().get(1).getFile(), "schema0.avsc");
        assertEquals(listTry.getUnchecked().get(1).getSubject(), "subject2-key");
    }
}