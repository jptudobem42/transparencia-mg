# Databricks notebook source
import pandas as pd
import requests
import json
import gzip
import os
import shutil
from tqdm import tqdm

# COMMAND ----------

class IngestionBronze:
    def __init__(self, api_url, metadata_file='metadata.json'):
        self.api_url = api_url
        self.metadata_file = metadata_file
        self.metadata = self.load_metadata()

    def load_metadata(self):
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        return {}

    def save_metadata(self):
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f)

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
            temp_path = os.path.join('temp', os.path.basename(url))
            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return temp_path
        else:
            print(f"Falha no download. Status code: {response.status_code}")
            return None
    
    def unzip_file(self, input_path, output_path):
        with gzip.open(input_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    
    def get_and_save(self, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        if not os.path.exists('temp'):
            os.makedirs('temp')

        urls = self.fetch_urls()

        for url, last_modified in tqdm(urls):
            file_name = os.path.basename(url)
            output_path = os.path.join(output_dir, file_name.replace('.gz', ''))
            if (url not in self.metadata or self.metadata[url] != last_modified or not os.path.exists(output_path)):
                temp_path = self.download_file(url)
                if temp_path:
                    if temp_path.endswith('.gz'):
                        self.unzip_file(temp_path, output_path)
                        os.remove(temp_path)
                    else:
                        shutil.move(temp_path, output_path)
                    self.metadata[url] = last_modified

        self.save_metadata()
