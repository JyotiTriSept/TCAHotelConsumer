package com.example.demo.upload;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.azure.core.util.BinaryData;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.example.demo.consumer.TCAHotelDataConsumer;
import com.example.demo.exception.HotelDataUploadException;

@Component
public class UploadToADLS {
	private static final Logger LOGGER = LoggerFactory.getLogger(UploadToADLS.class);
	
	public String uploadToADLS( UUID uuid) throws HotelDataUploadException {
		LOGGER.info("Request id: "+uuid+" upload to ADLS method: START");
		String uploadData = "";

		for (String data : TCAHotelDataConsumer.dataList) {
			uploadData = uploadData.concat(data + "\n");
		}
		DataLakeServiceClient getDataLakeServiceClient = GetDataLakeServiceClient();

		DataLakeFileSystemClient fileSystemClient = getDataLakeServiceClient
				.getFileSystemClient("woh-alg-pms-datalake-dev");

		//upload to archive storage
		String uploadToArchiveLocation = uploadToArchiveLocation(uploadData,fileSystemClient, uuid);
		LOGGER.info("Request id: "+uuid+" : "+uploadToArchiveLocation);
		
		DataLakeDirectoryClient subdirectoryClient = fileSystemClient.getDirectoryClient("RAW")
				                                     .getSubdirectoryClient("TCA-API")
				                                     .getSubdirectoryClient("Hotels")
				                                     .getSubdirectoryClient("LatestData");

		DataLakeFileClient fileClient = subdirectoryClient.createFileIfNotExists("AMR_Hotels_Latest.json");
		try {

			fileClient.upload(BinaryData.fromString(uploadData), true);
		} catch (Exception e) {
			String issue ="Issue occured while storing hotel data to adls with error: " + e.getMessage();
			throw new HotelDataUploadException(issue);
		}
		LOGGER.info("Request id: "+uuid+" upload to ADLS method: END");
		return "Flush of hotel data completed on path: " + fileClient.getFilePath();
	}

	private String uploadToArchiveLocation(String hotelData, DataLakeFileSystemClient fileSystemClient, UUID uuid) throws HotelDataUploadException {
		LOGGER.info("Request id: "+uuid+" upload to Archive location method: START");
		
		ZonedDateTime now = ZonedDateTime.now();
		
		DataLakeDirectoryClient subdirectoryClient = fileSystemClient.getDirectoryClient("RAW")
				                                     .getSubdirectoryClient("TCA-API")
				                                     .getSubdirectoryClient("Hotels")
				                                     .getSubdirectoryClient(String.valueOf(now.getYear()))
				                                     .getSubdirectoryClient(String.valueOf(now.getMonthValue()))
				                                     .getSubdirectoryClient(String.valueOf(now.getDayOfMonth()));
        String filename =  "AMR__Hotels_"+now.getYear()+"-"+now.getMonthValue()+"-"+now.getDayOfMonth()+".json";
		DataLakeFileClient fileClient = subdirectoryClient.createFileIfNotExists(filename);
		try {

			fileClient.upload(BinaryData.fromString(hotelData), true);
		} catch (Exception e) {
			System.out.println("Issue occured while storing hotel data to adls with error: " + e.getMessage());
			throw new HotelDataUploadException(e.getMessage());
		}
		LOGGER.info("Request id: "+uuid+" upload to Archive location method: START");
		return "Flush of hotel data completed on archive path: " + fileClient.getFilePath();
	}

	private static DataLakeServiceClient GetDataLakeServiceClient() {

		StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(
				"wohalgpmsdldevsa", "iSh4zGYXanmJdEhCSv/Qg1h+GF37rsfZAwIzzo0nByAgg6itXlDVQHFVe2gf5vK+3l4eFvtPWaVNj2P4f0wQow==");
		

		DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();

		builder.credential(sharedKeyCredential);
		
		builder.endpoint("https://" + "wohalgpmsdldevsa" + ".dfs.core.windows.net");

		return builder.buildClient();
	}
}

