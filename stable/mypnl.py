
from loguru import logger

def do_it_now():
    try:
        print("Hello Old Mypnl file")
    except Exception as e:
        logger.error(f"Yo Mikey - An error occurred while printing: {e}")

if __name__ == "__do_it_now__":
    logger.add("error_log.log", level="ERROR")  # Log errors to a file
    do_it_now()
