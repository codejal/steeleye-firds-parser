import logging

logger = logging.getLogger(__name__)


def main():
    logger.info("steeleye-firds-parser started")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    main()
