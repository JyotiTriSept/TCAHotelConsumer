package com.example.demo.controller;

import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.consumer.TCAHotelDataConsumer;
import com.example.demo.exception.HotelDataUploadException;
import com.example.demo.upload.UploadToADLS;

@RestController
public class ConsumerController {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerController.class);
	@Autowired
	TCAHotelDataConsumer consumer;

	@Autowired
	UploadToADLS upload;

	@GetMapping("/tca/hotel/consumer")
	public String consumeHotelData(@RequestParam(name = "totalEvent") long totalEvents, @RequestParam(name = "uuid") UUID uuid) {

		LOGGER.info("Request id: "+uuid+" Consume Hotel data Controller method: START");
		long before = System.currentTimeMillis();
		try {
			String eventsConsumed = consumer.consumerFromEventHub(totalEvents, uuid);
			LOGGER.info("Request id: "+uuid+" : "+eventsConsumed);
			String uploadToADLS = upload.uploadToADLS(uuid);
			LOGGER.info("Request id: "+uuid+" : "+uploadToADLS);
			long after = System.currentTimeMillis();
			String totalTimeTaken = "Time it took for all Hotel Data to be stored in RAW layer from EventHub is: "
					+ (after - before) / 1000.0 + " seconds.\n";
			LOGGER.info("Request id: "+uuid+" : "+totalTimeTaken);
			String responseString = eventsConsumed + totalTimeTaken;
			LOGGER.info("Request id: "+uuid+" Consume Hotel data Controller method: END");
			return responseString;

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			return null;
		} catch (HotelDataUploadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

	}

}
