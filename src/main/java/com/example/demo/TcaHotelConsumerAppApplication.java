package com.example.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TcaHotelConsumerAppApplication {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TcaHotelConsumerAppApplication.class);

	public static void main(String[] args) {
		LOGGER.info("Before Starting application name: "+TcaHotelConsumerAppApplication.class.getName());
		SpringApplication.run(TcaHotelConsumerAppApplication.class, args);
		LOGGER.debug("Starting TcaHotelConsumerAppApplication in debug with {} args", args.length);
		LOGGER.info("Starting TcaHotelConsumerAppApplication with {} args.", args.length); 
	}

}
