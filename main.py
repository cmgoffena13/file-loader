import logging

from src.readers.utils import process_directory
from src.settings import config

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s",
)


def main():
    try:
        results = process_directory()

        for result in results:
            print(result.model_dump())

    except Exception as e:
        # TODO: Setup Notification Alert here
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
