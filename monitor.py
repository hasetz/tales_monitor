import logging
import sys
import time
import yaml
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, PatternMatchingEventHandler
from PyPDF2 import PdfWriter, PdfReader
from pathlib import Path

config = yaml.safe_load(open("config.yaml"))
Path(config["preprocess_folder"]).mkdir(parents=True, exist_ok=True)

def preprocess_pdf(file_path):
    reader = PdfReader(file_path)
    writer = PdfWriter()
    pages_to_keep = reader.pages[1:-1] # Delete first and last page
    for page in pages_to_keep:
        writer.add_page(page)
    return writer

class pdf_handler(PatternMatchingEventHandler):
    def __init__(self, config):
        super().__init__(patterns=["*.pdf"], case_sensitive=False)
        self.config = config

    def on_created(self, event):
        reducted_pdf = preprocess_pdf(event.src_path)
        filename = event.src_path.split("/")[-1]
        filepath = Path(self.config["preprocess_folder"]) / ('preprocessed_' + filename)
        with open(filepath, 'wb') as f:
            reducted_pdf.write(f)  


if __name__ == "__main__":
    logging.basicConfig(filename='monitor.log', level=logging.INFO)
    path = config["workload_folder"]
    logging_handler = LoggingEventHandler()
    pdf_handler = pdf_handler(config=config)
    observer = Observer()
    observer.schedule(logging_handler, path, recursive=True)
    observer.schedule(pdf_handler, path, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()