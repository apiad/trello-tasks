import typer
import yaml

from . import TaskManager


def main(config:str="config.yml"):
    with open(config) as fp:
        config = yaml.safe_load(fp)

    manager = TaskManager(config)
    manager.run()


def run():
    typer.run(main)


if __name__ == "__main__":
    run()
