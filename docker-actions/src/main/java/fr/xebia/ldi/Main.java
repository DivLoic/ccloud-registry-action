package fr.xebia.ldi;

import com.jasongoodwin.monads.Try;
import com.jasongoodwin.monads.TryConsumer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Created by loicmdivad.
 */
public class Main {

    public static void main(String[] args) throws Throwable {

        Config config = ConfigFactory.load();
        Logger logger = LoggerFactory.getLogger(Main.class);

        File[] schemaFiles = new File(config.getString("avro.files.path"))
                .listFiles(pathname -> pathname.getName().endsWith(".avsc"));

        Map<String, File> subjectSchemaMap = Arrays
                .stream(Objects.requireNonNull(schemaFiles))
                .collect(Collectors.toMap((File file) -> file.getName().replace(".avsc", ""), (File file) -> file));

        subjectSchemaMap
                .forEach((subject, file) -> logger.warn(String.format("subject: %s / file: %s", subject, file.getPath())));

        CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(
                config.getString("schema.registry.url"),
                config.getInt("schema.registry.capacity"),
                config.getConfig("schema.registry.config")
                        .entrySet()
                        .stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        (Map.Entry<String, ConfigValue> pair) -> config
                                                .getString(String.format("schema.registry.config.%s", pair.getKey()))
                                )
                        )

        );


        Try
                .ofFailable(schemaRegistryClient::getAllSubjects)

                .onSuccess(
                        (TryConsumer<Collection<String>, Throwable>) subjects ->
                                new ArrayList<>(subjects).forEach(logger::warn)
                )
                .onFailure(
                        (TryConsumer<Throwable, Throwable>) throwable ->
                                logger.error("Something went wrong", throwable)
                );
    }
}
