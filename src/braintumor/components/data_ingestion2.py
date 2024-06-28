import os
import sys
import os
import urllib.request as request
import zipfile
import gdown
from braintumor.logger import logging
from braintumor.utils.common import get_size
from braintumor.entity.config_entity import DataIngestionConfig
from braintumor.entity.config_entity import DataIngestionArtifact
from pathlib import Path



class S3Operation:

    def sync_folder_from_s3(
        self, folder: str, bucket_name: str, bucket_folder_name: str
    ) -> None:
            
            command: str = (
                f"aws s3 sync s3://{bucket_name}/{bucket_folder_name}/ {folder} "
            )

            os.system(command)



class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        self.data_ingestion_config = data_ingestion_config

        self.s3 = S3Operation()

    def get_data_from_s3(self) -> None:
            
            logging.info("Entered the get_data_from_s3 method of Data ingestion class")

            self.s3.sync_folder_from_s3(
                folder=self.data_ingestion_config.local_data_file,
                bucket_name=self.data_ingestion_config.bucket_name,
                bucket_folder_name=self.data_ingestion_config.s3_data_folder,
            )

            logging.info("Exited the get_data_from_s3 method of Data ingestion class")

        
        

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        logging.info(
            "Entered the initiate_data_ingestion method of Data ingestion class"
        )
        self.get_data_from_s3()

        data_ingestion_artifact: DataIngestionArtifact = DataIngestionArtifact(
            train_file_path=self.data_ingestion_config.train_data_path,
            test_file_path=self.data_ingestion_config.test_data_path,
            )

        logging.info(
                "Exited the initiate_data_ingestion method of Data ingestion class"
            )

        return data_ingestion_artifact