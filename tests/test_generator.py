import logging

from src.ingestion.generator import generate_mock_events
from src.ingestion.bronze_writer import write_bronze_events

from src.common.constants import TEST_ROOT
from src.common.storage_factory import get_storage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

def main():

    logger.info("Starting mock event generation")

    storage = get_storage()
    events = generate_mock_events(storage=storage)

    logger.info("Generated %s events", len(events))

    if events:
        logger.info("First event sample:")
        logger.info(events[0])

    output_path = write_bronze_events(events=events, storage=storage)

    logger.info("Events written to %s", output_path)


if __name__ == "__main__":
    main()