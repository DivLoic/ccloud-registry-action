package fr.xebia.ldi;


import com.jasongoodwin.monads.Try;
import com.jasongoodwin.monads.TryConsumer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by loicmdivad.
 */
public class Main {

    public static void main(String[] args) throws Throwable {

        Config config = ConfigFactory.load();
        Logger logger = LoggerFactory.getLogger(Main.class);

        CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(
                config.getString("schema.registry.url"),
                config.getInt("schema.registry.capacity")
        );

        Try
                .ofFailable(schemaRegistryClient::getAllSubjects)

                .onSuccess(
                        (TryConsumer<Collection<String>, Throwable>) subjects ->
                                new ArrayList<String>(subjects).forEach(logger::warn)
                )
                .onFailure(
                        (TryConsumer<Throwable, Throwable>) throwable ->
                                logger.error("Something went wrong", throwable.getCause())
                );
    }
}
