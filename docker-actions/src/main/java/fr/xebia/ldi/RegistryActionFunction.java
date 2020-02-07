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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static fr.xebia.ldi.KeyValuePair.pair;

public class RegistryActionFunction {

    private Yaml yaml;
    private Config config;
    private SchemaRegistryClient client;

    private Logger logger = LoggerFactory.getLogger(RegistryActionFunction.class);

    public RegistryActionFunction(Config config) {
        this.config = config;
        this.setClient();
    }

    public RegistryActionFunction(Config config, SchemaRegistryClient schemaRegistryClient) {
        this.config = config;
        this.client = schemaRegistryClient;
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

    private void setYaml() {
        this.yaml = new Yaml(new Constructor(SchemaList.class));
    }

    private void setClient() {
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

    public Try<FileInputStream> tryStreamFileSubjects() {
        File file = new File(this.config.getString("avro.subjects.yaml"));

        return Try
                .ofFailable(() ->  new FileInputStream(file))
                .onFailure((t) -> logger.error("Fail to load the yaml file, it may not exist.", t));
    }

    public Try<List<SchemaList.SubjectEntry>> parseYaml(FileInputStream input) {
        return Try
                .ofFailable(() -> (SchemaList) getYaml().load(input))
                .onFailure((t) -> logger.error("Fail to parse the yaml file, it may be corrupted", t))
                .map(SchemaList::getSchemas);
    }

    public List<KeyValuePair<String, File>> mapSubjectToFile(List<SchemaList.SubjectEntry> subjects) {
        return subjects
                .stream()
                .map((subject) -> pair(
                        subject.getSubject(),
                        new File(config.getString("avro.files.path"), subject.getFile())
                ))
                .collect(Collectors.toList());
    }

    public Try<KeyValuePair<String, Schema>> parseOne(KeyValuePair<String, File> subjectFilePair) {
        return Try.ofFailable(() -> pair(subjectFilePair.key, getParser().parse(subjectFilePair.value)));
    }

    public Try<List<KeyValuePair<String, Schema>>> parseAll(List<KeyValuePair<String, File>> subjectFilePairs) {
        List<Try<KeyValuePair<String, Schema>>> collected = subjectFilePairs
                .stream()
                .map(this::parseOne)
                .collect(Collectors.toList());

        return travers(collected).map((stream) -> stream.collect(Collectors.toList()));
    }

    public Try<KeyValuePair<String, Boolean>> testOne(KeyValuePair<String, Schema> subjectSchemaPair) {
        return Try.ofFailable(() -> KeyValuePair.pair(
                subjectSchemaPair.key,
                getClient().testCompatibility(subjectSchemaPair.key, subjectSchemaPair.value))
        );
    }

    public Try<List<KeyValuePair<String, Boolean>>> testAll(List<KeyValuePair<String, Schema>> subjectSchemaPairs) {
        List<Try<KeyValuePair<String, Boolean>>> collected = subjectSchemaPairs
                .stream()
                .map(this::testOne)
                .collect(Collectors.toList());

        return travers(collected).map((stream) -> stream.collect(Collectors.toList()));
    }

    public <T> Try<Stream<T>> travers(List<Try<T>> tries) {

        return tries.stream().allMatch(Try::isSuccess)
                ? Try.ofFailable(() -> tries.stream().map(Try::getUnchecked))
                : tries.stream().filter(not(Try::isSuccess)).findFirst().orElse(extractionFailure()).map(Stream::of);
    }

    public static <R> Predicate<R> not(Predicate<R> predicate) {
        return predicate.negate();
    }

    private static <T> Try<T> extractionFailure() {
        return Try.failure(new IllegalStateException("Fail to extract the corrupted element. This should not occur."));
    }
}
