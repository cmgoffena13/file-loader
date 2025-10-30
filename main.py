import logging

from src.settings import config
from src.utils import process_directory

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s",
)

# Suppress noisy package loggers
logging.getLogger("pyexcel").setLevel(logging.WARNING)
logging.getLogger("pyexcel_io").setLevel(logging.WARNING)
logging.getLogger("pyexcel.internal").setLevel(logging.WARNING)


def main():
    try:
        results = process_directory()

        for result in results:
            print(result)

    except Exception as e:
        # TODO: Setup Notification Alert here
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
