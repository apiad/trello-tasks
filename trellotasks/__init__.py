import shlex, subprocess
import asyncio
import datetime
import psutil
import collections

from trello import TrelloClient, Card, List, Label


class TaskManager:
    def __init__(self, config):
        self.config = config

    def run(self):
        try:
            asyncio.run(self._run_async())
        except KeyboardInterrupt:
            print("Main loop cancelled!")

    async def _run_async(self):
        self.client = TrelloClient(
            api_key=self.config["auth"]["api_key"],
            api_secret=self.config["auth"]["api_secret"],
        )

        coroutines = []

        for board_config in self.config["boards"]:
            coroutines.append(self._setup_board(board_config))

        await asyncio.gather(*coroutines)

    async def _setup_board(self, board_config):
        board = self.client.get_board(board_config["id"])

        command = board_config["command"]
        poll_time = board_config.get("poll_time", 30)
        queue_list = None
        ongoing_list = None
        done_list = None

        for lst in board.get_lists("open"):
            if lst.name == board_config.get("queue_list", "Queue"):
                queue_list = lst
            elif lst.name == board_config.get("ongoing_list", "Ongoing"):
                ongoing_list = lst
            elif lst.name == board_config.get("done_list", "Done"):
                done_list = lst

        if queue_list is None:
            raise ValueError(f"Queue list not found")
        if ongoing_list is None:
            raise ValueError(f"Ongoing list not found")
        if done_list is None:
            raise ValueError(f"Done list not found")

        used_resources = collections.defaultdict(lambda: 0)

        while True:
            print(f"Checking cards from {board.name}")

            for card in queue_list.list_cards():
                self._schedule_card(card, board_config, ongoing_list, used_resources)

            for card in ongoing_list.list_cards():
                self._check_card(card, done_list, used_resources)

            await asyncio.sleep(poll_time)

    def _schedule_card(
        self, card: Card, board_config: dict, ongoing_list: List, used_resources: dict,
    ):
        resources = board_config.get("resources", {})
        uses_resources = []

        for label in card.labels:
            if label.name in resources:
                uses_resources.append(label.name)

        for label in uses_resources:
            if used_resources[label] >= resources[label]:
                return

        for label in uses_resources:
            used_resources[label] += 1

        cmd = shlex.split(board_config["command"].format(msg=card.description))

        print(f"Scheduling card {card.name}")
        process = subprocess.Popen(cmd, close_fds=True)

        card.change_list(ongoing_list.id)
        card.comment(f"⏲ Started: {datetime.datetime.now()}")
        card.comment(f"💻 PID: {process.pid}")

    def _check_card(self, card: Card, done_list: List, used_resources: dict):
        print(f"Checking card {card.name}")
        pid = None

        for comment in card.fetch_comments():
            comment_text = comment["data"]["text"]
            if "PID:" in comment_text:
                pid = int(comment_text.split("PID:")[1].strip())

        if pid is None:
            raise ValueError(f"PID not found in card {card.name}")

        if not psutil.pid_exists(pid):
            card.change_list(done_list.id)
            card.comment(f"❌ Error: Could not find the process")

            for label in card.labels:
                if label.name in used_resources:
                    used_resources[label.name] -= 1

            return

        process = psutil.Process(int(pid))

        if process.status() in [psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING]:
            return

        print(f"Finished card {card.name}")

        card.change_list(done_list.id)
        card.comment(f"✔️ Finished: {datetime.datetime.now()}")

        for label in card.labels:
            if label.name in used_resources:
                used_resources[label.name] -= 1
