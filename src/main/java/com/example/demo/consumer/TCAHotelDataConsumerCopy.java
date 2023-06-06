package com.example.demo.consumer;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.springframework.stereotype.Component;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventBatchContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

@Component
public class TCAHotelDataConsumerCopy {
	private static final String EVENTHUBNAME="hotel-tca";
	private static final String EVENTHUBCONNECTIONSTRING="Endpoint=sb://alg-ehub-ns.servicebus.windows.net/;SharedAccessKeyName=readWriteAccess;SharedAccessKey=JpB0AZsti7jUjMF/jQNqsLiPm5+Iq7+Lc+AEhC2jfME=;EntityPath=hotel-tca";
	
	
	
	public static Set<String> dataList = new HashSet<>();
	
	public String consumerFromEventHub(long totalEvents) throws IOException, InterruptedException {
	    int retry = 2;
		
		// Create a blob container client that you use later to build an event processor client to receive and process events
		   BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
				   .connectionString("DefaultEndpointsProtocol=https;AccountName=wohalgpmsdldevsa;AccountKey=iSh4zGYXanmJdEhCSv/Qg1h+GF37rsfZAwIzzo0nByAgg6itXlDVQHFVe2gf5vK+3l4eFvtPWaVNj2P4f0wQow==;EndpointSuffix=core.windows.net")
		        .containerName("ehub-hotel-checkpoint")
		        .buildAsyncClient();

		    // Create a builder object that you will use later to build an event processor client to receive and process events and errors.
		    EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
		        .connectionString(EVENTHUBCONNECTIONSTRING, EVENTHUBNAME)
		        .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
		        .processEventBatch(PROCESS_EVENT_BATCH, 50, Duration.ofSeconds(30))
		        .processError(ERROR_HANDLER)
		        .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient));

		    // Use the builder object to create an event processor client
		    EventProcessorClient eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();

		    System.out.println("Starting event processor");
		    eventProcessorClient.start();
		    System.out.println(ZonedDateTime.now());

		    Thread.sleep(TimeUnit.MINUTES.toMillis(1));
		    System.out.println(ZonedDateTime.now());
		    
		    System.out.println("Datalist: "+dataList.size());
			if (dataList.size() == 0 ||dataList.size() < totalEvents) {
				
				for(int i=0;i<=retry;i++) {
					System.out.println("Datalist: "+dataList.size());
					System.out.println("Stopping event processor");
					eventProcessorClient.stop();
					Thread.sleep(4000);
					System.out.println("Restarting event hub processor");
					eventProcessorClient.start();
					Thread.sleep(5000);
					if(dataList.size() == 0 || dataList.size() < totalEvents) {
						continue;
					} else {
						System.out.println("Stopping event processor");
						eventProcessorClient.stop();
						Thread.sleep(2000);
						System.out.println("Event processor stopped.");
						break;
					}
				}
				
			}  else {
				System.out.println("Stopping event processor");
				eventProcessorClient.stop();
				Thread.sleep(2000);
				System.out.println("Event processor stopped.");
			}
		    
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
}
