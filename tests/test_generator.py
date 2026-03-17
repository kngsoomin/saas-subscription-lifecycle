import logging

from src.ingestion.generator import generate_mock_events
from src.ingestion.bronze_writer import write_bronze_events

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

def main():

    logger.info("Starting mock event generation")

    events = generate_mock_events(
        state_path="data/state/generator/subscription_state_current.json",
        seq_path="data/state/generator/last_subscription_seq.txt",
    )

    logger.info("Generated %s events", len(events))

    if events:
        logger.info("First event sample:")
        logger.info(events[0])

    output_path = write_bronze_events(events=events, base_dir="data/bronze/subscription_events")

    logger.info("Events written to %s", output_path)


if __name__ == "__main__":
    main()