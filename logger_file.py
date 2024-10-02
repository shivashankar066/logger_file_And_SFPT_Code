import logging
from datetime import datetime

log_file = 'E:/application.log'
logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def my_function():
    logging.info("Starting the function")

    try:
        # Write your code here
        print("This is a print statement")
        logging.info("This is a print statement logged")
        x = 1 / 0

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.error("Error details:", exc_info=True)


def another_fun(a, b):
    try:
        logging.info("Multplication of two numbers")
        c = a * b
        logging.info("Result", c)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.error("Error details:", exc_info=True)


if __name__ == "__main__":
    my_function()
    another_fun(6, 4)