import logging
import os

from dotenv import load_dotenv
from jinja2 import Template
from pathlib import Path

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s [%(levelname)s] %(message)s")
repo_root = Path(__file__).parents[2]


def main():
    with open(repo_root / "templates" / 'databricks.yaml.j2') as f:
        template = Template(f.read()).render(env=os.environ)
    logger.info("Databricks template file read and rendered successfully.")

    with open(repo_root / 'databricks.yaml', 'w') as f:
        f.write(template)
    logger.info("Databricks YAML file created successfully.")


if __name__ == '__main__':
    main()
