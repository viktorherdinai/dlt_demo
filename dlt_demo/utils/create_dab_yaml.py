import os

from dotenv import load_dotenv
from jinja2 import Template
from pathlib import Path

load_dotenv()

repo_root = Path(__file__).parents[2]


def main():
    with open(repo_root / "templates" / 'databricks.yaml.j2') as f:
        template = Template(f.read()).render(env=os.environ)

    with open(repo_root / 'databricks.yaml', 'w') as f:
        f.write(template)


if __name__ == '__main__':
    main()