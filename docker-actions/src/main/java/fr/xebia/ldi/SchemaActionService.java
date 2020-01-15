package fr.xebia.ldi;

import com.jasongoodwin.monads.Try;
import com.jasongoodwin.monads.TrySupplier;
import com.sun.tools.javac.util.Pair;
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
import java.util.Map.Entry;
import java.util.function.Function;
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
    private SchemaRegistryClient client;

    private Logger logger = LoggerFactory.getLogger(SchemaActionService.class);

    public SchemaActionService(Config config) {
        this.config = config;
        this.setClient();
    }

    public Yaml getYaml() {
        return Optional
                .ofNullable(this.yaml)
                .orElseGet(() -> {
                    this.setYaml();
                    return this.yaml;
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

    public Schema.Parser getParser() {
        return new Schema.Parser();
    }

    public void setYaml() {
        this.yaml = new Yaml(new Constructor(SchemaList.class));
    }


    public void setClient() {
        this.client = new CachedSchemaRegistryClient(
                this.config.getString("schema.registry.url"),
                this.config.getInt("schema.registry.capacity"),
                this.config.getConfig("schema.registry.config")
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Entry::getKey,
                                (Entry<String, ConfigValue> pair) -> this.config
                                        .getString(String.format("schema.registry.config.%s", pair.getKey()))
                                )
                        )
        );
    }

    public void setClient(SchemaRegistryClient client) {
        this.client = client;
    }

    public static <R> Predicate<R> not(Predicate<R> predicate) {
        return predicate.negate();
    }

    public List<Pair<String, File>> mapSubjectToFile(List<SchemaList.SubjectEntry> subjects) {
        return subjects.stream().map((subject) ->
                Pair.of(
                        subject.getSubject(),
                        new File(config.getString("avro.files.path"), subject.getFile())
                )
        ).collect(Collectors.toList());
    }

    private <T> Supplier<Stream<T>> supply(List<T> list) {
        return list::stream;
    }


    public Try<Pair<String, Schema>> parseOne(Pair<String, File> file) {
        return Try.ofFailable(() -> Pair.of(file.fst, getParser().parse(file.snd)));
    }

    public Try<List<Pair<String, Schema>>> parseAll(List<Pair<String, File>> files) {
        List<Try<Pair<String, Schema>>> collected = files
                .stream()
                .map(this::parseOne)
                .collect(Collectors.toList());

        Try<Stream<Pair<String, Schema>>> t = collected.stream().allMatch(Try::isSuccess)
                ? Try.ofFailable(() -> collected.stream().map(Try::getUnchecked))
                : Try.failure(new Exception("Fail to parse some schema files"));

        return t.map((stream) -> stream.collect(Collectors.toList()));
    }

    public Try<Pair<String, Boolean>> testOne(Pair<String, Schema> schema) {
        return Try.ofFailable(() -> Pair.of(schema.fst, getClient().testCompatibility(schema.fst, schema.snd)));
    }

    public Try<List<Pair<String, Boolean>>> testAllCompatibilities(List<Pair<String, Schema>> schemas) {
        List<Try<Pair<String, Boolean>>> collected = schemas
                .stream()
                .map(this::testOne)
                .collect(Collectors.toList());

        Try<Stream<Pair<String, Boolean>>> t = collected.stream().allMatch(Try::isSuccess)
                ? Try.ofFailable(() -> collected.stream().map(Try::getUnchecked))
                : Try.failure(new Exception("Fail to test schema files compatibility"));

        return t.map((stream) -> stream.collect(Collectors.toList()));
    }

    public <T, U> Try<Stream<T>> travers(List<Try<T>> tries) {

        Try<Stream<T>> t = tries.stream().allMatch(Try::isSuccess)
                ? Try.ofFailable(() -> tries.stream().map(Try::getUnchecked))
                : Try.failure(new Exception("Fail to test schema files compatibility"));

        return null;

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
