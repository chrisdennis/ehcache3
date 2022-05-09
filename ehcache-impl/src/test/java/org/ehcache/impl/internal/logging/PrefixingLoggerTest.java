package org.ehcache.impl.internal.logging;

import org.ehcache.core.spi.service.LoggingService;
import org.junit.Test;
import org.slf4j.Logger;

public class PrefixingLoggerTest {

  @Test @SuppressWarnings("try")
  public void testSimplePrefix() {
    DefaultLoggingService defaultLoggingService = new DefaultLoggingService();


    try (LoggingService.Context context = defaultLoggingService.withContext("foo", "bar")) {
      Logger logger = defaultLoggingService.getLogger(PrefixingLoggerTest.class);

      logger.info("Message {}", "Message");
    }
  }

  @Test @SuppressWarnings("try")
  public void testEscapedPrefix() {
    DefaultLoggingService defaultLoggingService = new DefaultLoggingService();


    try (LoggingService.Context context = defaultLoggingService.withContext("foo", "{}")) {
      Logger logger = defaultLoggingService.getLogger(PrefixingLoggerTest.class);

      logger.info("Message {}", "Message");
    }
  }

  @Test @SuppressWarnings("try")
  public void testDoubleEscapedPrefix() {
    DefaultLoggingService defaultLoggingService = new DefaultLoggingService();


    try (LoggingService.Context context = defaultLoggingService.withContext("foo", "\\{}")) {
      Logger logger = defaultLoggingService.getLogger(PrefixingLoggerTest.class);

      logger.info("Message {}", "Message");
    }
  }

  @Test @SuppressWarnings("try")
  public void testTripleEscapedPrefix() {
    DefaultLoggingService defaultLoggingService = new DefaultLoggingService();


    try (LoggingService.Context context = defaultLoggingService.withContext("foo", "\\\\{}")) {
      Logger logger = defaultLoggingService.getLogger(PrefixingLoggerTest.class);

      logger.info("Message {}", "Message");
    }
  }
}
