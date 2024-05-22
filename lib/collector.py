import pandas as pd
import requests
import json
import gzip
import os
import shutil

class Raw:
    def __init__(self, api_url, spark, metadata_table):
        self.api_url = api_url
        self.spark = spark
        self.metadata_table = metadata_table
        self.metadata = self.load_metadata()

    def load_metadata(self):
        if self.spark._jsparkSession.catalog().tableExists(self.metadata_table):
            df = self.spark.table(self.metadata_table)
            return {row.url: row.last_modified for row in df.collect()}
        return {}

    def save_metadata(self):
        metadata_df = self.spark.createDataFrame(
            [(url, last_modified) for url, last_modified in self.metadata.items()],
            ['url', 'last_modified']
        )
        metadata_df.write.mode('overwrite').saveAsTable(self.metadata_table)

    def fetch_urls(self):
        response = requests.get(self.api_url)
        
        if response.status_code == 200:
            data = response.json()
            resources = data.get('result', {}).get('resources', [])
            urls = [(resource.get('url'), resource.get('last_modified')) for resource in resources if 'url' in resource]
            return urls
        else:
            print(f"Falha na requisição. Status code: {response.status_code}")
            return []

    def download_file(self, url):
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            return response.content
        else:
            print(f"Falha no download. Status code: {response.status_code}")
            return None
    
    def save_file(self, content, output_path):
        with open(output_path, 'wb') as f:
            f.write(content)

    def unzip_file_content(self, content):
        return gzip.decompress(content)

    def get_and_save(self, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        urls = self.fetch_urls()

        for url, last_modified in urls:
            file_name = os.path.basename(url)
            output_path = os.path.join(output_dir, file_name.replace('.gz', ''))
            if (url not in self.metadata or self.metadata[url] != last_modified or not os.path.exists(output_path)):
                file_content = self.download_file(url)
                if file_content:
                    if url.endswith('.gz'):
                        file_content = self.unzip_file_content(file_content)
                    self.save_file(file_content, output_path)
                    self.metadata[url] = last_modified

        self.save_metadata()