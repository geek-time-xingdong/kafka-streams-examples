package io.confluent.examples.streams.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author ceyhunuzunoglu
 */
@SpringBootApplication(scanBasePackages = "io.confluent.examples.streams.processor")
public class KafkaProducerApiExampleApplication {

  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(KafkaProducerApiExampleApplication.class);
    app.setRegisterShutdownHook(false);
    app.run(args);
  }
}
