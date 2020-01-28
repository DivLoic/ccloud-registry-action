package fr.xebia.ldi;

import com.jasongoodwin.monads.Try;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static java.lang.System.exit;

/**
 * Created by loicmdivad.
 */
public class Validation {

    static class CompatibilityCheckException extends Exception {
        CompatibilityCheckException(String message){
            super(message);
        }
    }

    public static void main(String[] args) throws Throwable {

        Config config = ConfigFactory.load();
        Logger logger = LoggerFactory.getLogger(Validation.class);

        RegistryActionFunction actionService = new RegistryActionFunction(config);

        Instant start = Instant.now();
        Runtime
                .getRuntime()
                .addShutdownHook(new Thread(
                        () -> logger.info(
                                String.format(
                                        "time elapsed : %s sec.",
                                        Duration.between(start, Instant.now()).toMillis() / 1000.d)
                        )
                ));

        Try<List<KeyValuePair<String, Boolean>>> triedCompatibilityResults = actionService

                .tryStreamFileSubjects()

                .flatMap(actionService::parseYaml)

                .onSuccess((l) -> logger.info("Successfully load the yaml file."))

                .map(actionService::mapSubjectToFile)

                .onSuccess((l) -> logger.info("Successfully convert the yaml into a SchemaList instance."))

                .onSuccess((l) -> l.forEach((entry) -> logger.info(
                        String.format(
                                "Subject loaded - subject: %s <> file: %s",
                                entry.key,
                                entry.value.getPath()
                        ))
                ))

                .flatMap(actionService::parseAll)

                .onSuccess((l) -> logger.debug("Successfully parse all the avro schemas - now testing compatibilities"))

                .flatMap(actionService::testAll);

        triedCompatibilityResults
                .onFailure(throwable -> {
                    throw throwable;
                })

                .onSuccess((validations) -> {

                            if (validations.isEmpty()) {
                                logger.warn("Not a single schema to validate was found ... ");
                                exit(0);
                            }

                            validations.forEach((validation) -> {
                                if (!validation.value) logger.error(
                                        String.format("Compatibility test failed for subject: %s.", validation.key)
                                );
                            });

                            if (validations.stream().allMatch((validation) -> validation.value)) logger.info(
                                    "🎉 Successfully validate all schema compatibilities."
                            );
                            else throw new CompatibilityCheckException("Fail due to incompatible schemas.");
                        }
                );
    }
}
