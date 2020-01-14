package fr.xebia.ldi;

import com.jasongoodwin.monads.Try;
import com.jasongoodwin.monads.TrySupplier;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import jdk.internal.org.objectweb.asm.commons.TryCatchBlockSorter;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by loicmdivad.
 */
public class SchemaActionService {

    private Yaml yaml;
    private Config config;
    private Schema.Parser parser;
    private SchemaRegistryClient client;

    private Logger logger = LoggerFactory.getLogger(SchemaActionService.class);

    public SchemaActionService(Config config) {
        this.config = config;
        this.setClient();
        this.setParse();
    }

    public Yaml getYaml() {
        return Optional
                .ofNullable(this.yaml)
                .orElseGet(() -> {
                    this.setYaml();
                    return this.yaml;
                });
    }

    public Schema.Parser getParser() {
        return Optional
                .ofNullable(this.parser)
                .orElseGet(() -> {
                    this.setParse();
                    return this.parser;
                });
    }

    public SchemaRegistryClient getClient() {
        return Optional
                .ofNullable(this.client)
                .orElseGet(() -> {
                    this.setClient();
                    return this.client;
                });
    }

    public void setYaml() {
        this.yaml = new Yaml(new Constructor(SchemaList.class));
    }

    public void setParse() {
        this.parser = new Schema.Parser();
    }

    public void setClient() {
        this.client = new CachedSchemaRegistryClient(
                this.config.getString("schema.registry.url"),
                this.config.getInt("schema.registry.capacity"),
                this.config.getConfig("schema.registry.config")
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                (Map.Entry<String, ConfigValue> pair) -> this.config
                                        .getString(String.format("schema.registry.config.%s", pair.getKey()))
                                )
                        )
        );
    }

    public void setParser(Schema.Parser parser) {
        this.parser = parser;
    }

    public void setClient(SchemaRegistryClient client) {
        this.client = client;
    }

    public static <R> Predicate<R> not(Predicate<R> predicate) {
        return predicate.negate();
    }

    public Try<Stream<Schema>> parseAll(Stream<File> files) {
        Stream<Try<Schema>> tryStream = files.map((file) -> Try.ofFailable(() -> getParser().parse(file)));

        return tryStream.allMatch(Try::isSuccess)
                ? Try.ofFailable(() -> tryStream.map(Try::getUnchecked))
                : Try.failure(new Exception("Fail to parse some schema files"));
    }

    public Map.Entry<String, File> mapSubjectToFile(SchemaList.SubjectEntry subject) {
        return new AbstractMap.SimpleEntry<>(
                subject.getSubject(),
                new File(config.getString("avro.files.path") , subject.getFile())
        );
    }

    public Try<List<SchemaList.SubjectEntry>> triedSubjectList() {
        File file = new File("/tmp/app/avro/schema.yml");

        return Try
                .ofFailable(() ->  new FileInputStream(file))
                .onFailure((t) -> logger.error("Fail to load the schema.yml file in a FileInputStream", t))
                .map((fis) -> (SchemaList) getYaml().load(fis))
                .onFailure((t) -> logger.error("Fail to parse the schema.yml file to SchemaList class", t))
                .map(SchemaList::getSchemas);

    }

    public static File[] listAvscFiles(Config config) {
        return new File(config.getString("avro.files.path"))
                .listFiles(pathname -> pathname.getName().endsWith(".avsc"));
    }

    public static Map<String, File> mapSubjectToFiles(File[] files, Config config) {
        return Arrays
                .stream(Objects.requireNonNull(files))
                .collect(Collectors.toMap((File file) -> file.getName().replace(".avsc", ""), (File file) -> file));
    }
}
