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
import com.typesafe.config.ConfigFactory;
import fr.xebia.ldi.error.CompatibilityCheckException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class Validation {

    public static void main(String[] args) throws Throwable {

        ConfigFactory.invalidateCaches();
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
                                return;
                            }

                            validations.forEach((validation) -> {
                                if (!validation.value) logger.error(
                                        String.format("Compatibility test failed for subject: %s.", validation.key)
                                );
                            });

                            if (validations.stream().allMatch((validation) -> validation.value)) logger.info(
                                    "\\(ᵔᵕᵔ)/ Successfully validate all schema compatibilities."
                            );
                            else throw new CompatibilityCheckException("Fail due to incompatible schemas.");
                        }
                );
    }
}
