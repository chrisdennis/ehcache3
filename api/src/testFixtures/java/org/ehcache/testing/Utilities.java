/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehcache.testing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static java.util.Collections.singletonMap;

public class Utilities {

  public static URL substitute(URL input, String variable, String substitution) throws IOException {
    return substitute(input, singletonMap(variable, substitution));
  }

  public static URL substitute(URL input, Map<String, String> variables) throws IOException {
    Function<String, String> substitution = variables.entrySet().stream()
      .map(e -> (Function<String, String>) line -> line.replace("${" + e.getKey() + "}", e.getValue()))
      .reduce(Function.identity(), Function::andThen);

    File output = File.createTempFile(input.getFile(), ".substituted", new File("build"));
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(output));
         BufferedReader reader = new BufferedReader(new InputStreamReader(input.openStream(), StandardCharsets.UTF_8))) {
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        } else {
          writer.write(substitution.apply(line));
          writer.newLine();
        }
      }
    }
    return output.toURI().toURL();
  }
}
