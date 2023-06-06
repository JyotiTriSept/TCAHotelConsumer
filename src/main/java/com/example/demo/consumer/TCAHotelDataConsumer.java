package com.example.demo.consumer;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.azure.messaging.eventhubs.*;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.*;
import com.azure.messaging.eventhubs.models.EventBatchContext;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

@Component
public class TCAHotelDataConsumer {
	private static final Logger LOGGER = LoggerFactory.getLogger(TCAHotelDataConsumer.class);
			
	private static final String connectionString = "Endpoint=sb://alg-ehub-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=+fcEzuI7B9sFRFuxR0wtK4IxYU00RmH0hfFga9K+msA=";
    private static final String eventHubName = "hotel-tca";
	private static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=wohalgpmsdldevsa;AccountKey=iSh4zGYXanmJdEhCSv/Qg1h+GF37rsfZAwIzzo0nByAgg6itXlDVQHFVe2gf5vK+3l4eFvtPWaVNj2P4f0wQow==;EndpointSuffix=core.windows.net";
	private static final String checkpointStorageContainerName = "ehub-hotel-checkpoint";
	
	
	public static Set<String> dataList = new HashSet<>();
	
	public String consumerFromEventHub(long totalEvents, UUID uuid) throws IOException, InterruptedException {
	    
		LOGGER.info("Request id: "+uuid+" consume hotel from event hub method: START");
		int retry = 2;
		
	    // Create a blob container client that you use later to build an event processor client to receive and process events
		   BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
		        .connectionString(storageConnectionString)
		        .containerName(checkpointStorageContainerName)
		        .buildAsyncClient();

		    // Create a builder object that you will use later to build an event processor client to receive and process events and errors.
		    EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
		        .connectionString(connectionString, eventHubName)
		        .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
		        .processEvent(PARTITION_PROCESSOR)
		        .processError(ERROR_HANDLER)
		        .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient));

		    // Use the builder object to create an event processor client
		    EventProcessorClient eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();

		    LOGGER.info("Request id: "+uuid+" : Starting event processor");
		    eventProcessorClient.start();
		    System.out.println(ZonedDateTime.now());

		    Thread.sleep(TimeUnit.MINUTES.toMillis(1));
		    System.out.println(ZonedDateTime.now());
		    
		    System.out.println("Datalist: "+dataList.size());
			if (dataList.size() == 0 ||dataList.size() < totalEvents) {
				
				for(int i=0;i<=retry;i++) {
					LOGGER.info("Request id: "+uuid+" : Datalist: "+dataList.size());
					LOGGER.info("Request id: "+uuid+" : Stopping event processor");
					eventProcessorClient.stop();
					Thread.sleep(4000);
					LOGGER.info("Request id: "+uuid+" : Restarting event hub processor");
					eventProcessorClient.start();
					Thread.sleep(5000);
					if(dataList.size() == 0 || dataList.size() < totalEvents) {
						continue;
					} else {
						LOGGER.info("Request id: "+uuid+" : Stopping event processor");
						eventProcessorClient.stop();
						Thread.sleep(2000);
						LOGGER.info("Request id: "+uuid+" : Event processor stopped.");
						break;
					}
				}
				
			}  else {
				LOGGER.info("Request id: "+uuid+" : Stopping event processor");
				eventProcessorClient.stop();
				Thread.sleep(2000);
				LOGGER.info("Request id: "+uuid+" : Event processor stopped.");
			}
			LOGGER.info("Request id: "+uuid+" consume hotel from event hub method: END");
		 return "No of Events Consumed: "+dataList.size()+"\n";   
	}
	


	Consumer<ErrorContext> ERROR_HANDLER = errorContext -> {
	    System.out.printf("Error occurred in partition processor for partition %s, %s.%n",
	        errorContext.getPartitionContext().getPartitionId(),
	        errorContext.getThrowable());
	};
	
	 Consumer<EventBatchContext> PROCESS_EVENT_BATCH = eventBatchContext -> {
         eventBatchContext.getEvents().forEach(eventData -> {
        	 String data = eventData.getBodyAsString();
     	    dataList.add(data);
             System.out.printf("Partition id = %s and sequence number of event = %s%n",
                 eventBatchContext.getPartitionContext().getPartitionId(),
                 eventData.getSequenceNumber());
         });
         eventBatchContext.updateCheckpoint();
     };
     
      Consumer<EventContext> PARTITION_PROCESSOR = eventContext -> {
 	    PartitionContext partitionContext = eventContext.getPartitionContext();
 	    EventData eventData = eventContext.getEventData();
 	    String data = eventData.getBodyAsString();
 	    dataList.add(data);
 	    System.out.printf("Processing event from partition %s with sequence number %d with body: %s%n",
 	        partitionContext.getPartitionId(), eventData.getSequenceNumber(), data);
        
 	    // Every 10 events received, it will update the checkpoint stored in Azure Blob Storage.
 			eventContext.updateCheckpoint();
 		
 		 
 	};
}
