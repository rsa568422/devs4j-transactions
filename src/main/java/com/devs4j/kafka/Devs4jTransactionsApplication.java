package com.devs4j.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

@SpringBootApplication
@EnableScheduling
public class Devs4jTransactionsApplication {

	@Autowired
	private KafkaTemplate<String, String> producer;

	private static final Logger log = LoggerFactory.getLogger(Devs4jTransactionsApplication.class);

	@KafkaListener(topics = "devs4j-transactions", groupId = "devs4jGroup", containerFactory = "kafkaListenerContainerFactory")
	public void listen(List<String> messages) {
		messages.forEach(log::info);
	}

	@Scheduled(fixedRate = 10000)
	public void sendMessages() {
		for (int i = 0; i < 10000; i++) {
			producer.send("devs4j-transactions", "key", String.format("Mensaje %d", i));
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(Devs4jTransactionsApplication.class, args);
	}

}
