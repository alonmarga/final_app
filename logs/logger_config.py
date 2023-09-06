import logging

def setup_logger(log_file_path):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename=log_file_path,
        filemode="w"  # Append mode, so logs are added to the file
    )
    return logging.getLogger("my_logger")
