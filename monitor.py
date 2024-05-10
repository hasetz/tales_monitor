import logging
import shutil
import subprocess
import tempfile
import time
import yaml
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, PatternMatchingEventHandler
from PyPDF2 import PdfWriter, PdfReader
from pathlib import Path


config = yaml.safe_load(open("config.yaml"))

class pdf_handler(PatternMatchingEventHandler):
    """
    Handler for pdf files. Does preprocessing of pdf files when they arrive in the workload folder and saves results in the 
    preprocess folder.
    """
    def __init__(self, config):
        super().__init__(patterns=["*.pdf"], case_sensitive=False)
        self.config = config

    def on_created(self, event):
        """Triggered when pdf file is created in the workload folder. Preprocesses the pdf file and saves it in the preprocess folder."""
        filename = event.src_path.split("/")[-1]
        output_path = Path(self.config["preprocess_folder"]) / filename
        preprocess_pdf(event.src_path, output_path)

def get_ghostscript_path():
    gs_names = ["gs", "gswin32", "gswin64"]
    for name in gs_names:
        if shutil.which(name):
            return shutil.which(name)
    raise FileNotFoundError(
        f"No GhostScript executable was found on path ({'/'.join(gs_names)})"
    )

def compress_pdf_file(input_path, output_path):
    """Compresses the pdf file using ghostscript."""
    gs = get_ghostscript_path()
    tmp = tempfile.TemporaryDirectory()
    logging.info(f"Started compressing {input_path} to {output_path}")
    subprocess.call(
        [
            gs,
            "-q"
            "-dNOPAUSE",
            "-dBATCH",
            "-dSAFER",
            "-dQUIET",
            "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.5",
            "-dPDFSETTINGS=/screen",
            "-dEmbedAllFonts=true", 
            "-dSubsetFonts=true",  
            "-dColorImageDownsampleType=/Bicubic", 
            "-dColorImageResolution=144",                #PDF downsample color image resolution`
            "-dGrayImageDownsampleType=/Bicubic", 
            "-dGrayImageResolution=144",                 #PDF downsample gray image resolution`
            "-dMonoImageDownsampleType=/Bicubic",
            "-dMonoImageResolution=144",                  
            "-sOutputFile=" + tmp.name + "/output.pdf",
            "-dNOPAUSE",
            str(input_path),
        ]
    )
    shutil.move(tmp.name + "/output.pdf", output_path)
    tmp.cleanup()
    logging.info(f"Compressed {input_path} to {output_path}")

def preprocess_pdf(input_path, output_path):
    """Preprocesses the pdf file by removing the first and last page."""
    reader = PdfReader(input_path)
    writer = PdfWriter()
    pages_to_keep = reader.pages[1:-1] # Delete first and last page
    for page in pages_to_keep:
        writer.add_page(page)
    with open(output_path, 'wb') as f:
        writer.write(f) 
    compress_pdf_file(output_path, Path(config['final_folder']) / output_path.name)
    

def check_new_pdfs(workload_folder, preprocess_folder):
    """When application is started, checks for any pdf files in the workload folder and preprocesses them."""
    for file in Path(workload_folder).glob('*.pdf'):
        if not (Path(preprocess_folder) /  file.name).exists():
            output_path = Path(preprocess_folder) / file.name
            preprocess_pdf(file, output_path)


if __name__ == "__main__":
    logging.basicConfig(filename='monitor.log', level=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())
    workload_folder = config["workload_folder"]
    for folder in [config["preprocess_folder"], config['final_folder']]:
        Path(folder).mkdir(parents=True, exist_ok=True)

    check_new_pdfs(workload_folder, config["preprocess_folder"])

    # Handler for logging all events in the workload folder
    logging_handler = LoggingEventHandler()

    # Handler for pdf files preprocessing
    pdf_handler = pdf_handler(config=config)

    observer = Observer()
    observer.schedule(logging_handler, workload_folder, recursive=True)
    observer.schedule(pdf_handler, workload_folder, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
