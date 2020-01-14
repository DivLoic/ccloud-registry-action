package fr.xebia.ldi;

import com.jasongoodwin.monads.Try;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by loicmdivad.
 */
public class Validation {

    public static void main(String[] args) throws Throwable {

        Config config = ConfigFactory.load();
        Logger logger = LoggerFactory.getLogger(Validation.class);

        SchemaActionService actionService = new SchemaActionService(config);

        actionService

                .triedSubjectList()

                .onSuccess((l) -> logger.info("Successfully load the yaml file."))

                .map((subjects) -> subjects.stream().map(actionService::mapSubjectToFile))

                .onSuccess((l) -> logger.info("Successfully parse the yaml into SchemaList instance."))

                .onSuccess((l) -> l.forEach((entry) -> logger.info(
                        String.format(
                                "Subject loaded - subject: %s / file: %s",
                                entry.getKey(),
                                entry.getValue().getPath()
                        )
                )))

        ;


        System.exit(1);


        Map<String, File> subjectSchemaMap = SchemaActionService.mapSubjectToFiles(null, config);

        subjectSchemaMap.forEach((subject, file) ->
                logger.debug(String.format("Loading - subject: %s / file: %s", subject, file.getPath()))
        );

        Map<String, Try<Boolean>> compatibilities = subjectSchemaMap.entrySet().stream().map(entry -> {

            Try<Boolean> triedCompatibility = Try.ofFailable(() -> actionService.getParser().parse(entry.getValue()))

                    .onFailure((t) -> logger.error(String.format("Fail to parse schema from subject %s", entry.getKey()), t))

                    .map((schema) -> actionService.getClient().testCompatibility(entry.getKey(), schema))

                    .onFailure((t) -> logger.error(String.format("Compatibility check failed for %s", entry.getKey()), t));

            return new AbstractMap.SimpleEntry<>(entry.getKey(), triedCompatibility);

        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Optional<Try<Boolean>> result = compatibilities.values().stream().reduce(
                (booleanTry, booleanTry2) ->
                        booleanTry.flatMap(compatibility ->
                                booleanTry2.map(compatibility2 ->
                                        compatibility && compatibility2)
                        )
        );

        if(result.get().get()) {
            logger.info("Successfully validate the compatibility of all schemas");
        } else {
            throw new Exception("Nope!");
        }
    }
}
