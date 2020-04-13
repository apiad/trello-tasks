import shlex, subprocess
import asyncio
import datetime
import psutil
import collections
import uuid
from pathlib import Path

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
        labels = card.labels or []

        for label in labels:
            if label.name in resources:
                uses_resources.append(label.name)

        for label in uses_resources:
            if used_resources[label] >= resources[label]:
                return

        for label in uses_resources:
            used_resources[label] += 1

        uid = str(uuid.uuid4())
        cmd = board_config["command"].format(msg=card.description, uid=uid)

        print(f"Scheduling card {card.name}")
        process = subprocess.Popen(cmd, shell=True, start_new_session=True)

        card.change_list(ongoing_list.id)
        card.comment(f"⏲ Started: {datetime.datetime.now()}")
        card.comment(f"💻 PID: {process.pid}")
        card.comment(f"🔑 UID: {uid}")

    def _check_card(self, card: Card, done_list: List, used_resources: dict):
        print(f"Checking card {card.name}")
        pid = None
        uid = None

        for comment in card.fetch_comments():
            comment_text = comment["data"]["text"]
            if "PID:" in comment_text:
                pid = int(comment_text.split("PID:")[1].strip())
            if "UID:" in comment_text:
                uid = comment_text.split("UID:")[1].strip()

        if pid is None:
            raise ValueError(f"PID not found in card {card.name}")

        if uid is None:
            raise ValueError(f"UID not found in card {card.name}")

        if not psutil.pid_exists(pid):
            card.comment(f"⚠️ Warning: Could not find the process, assuming it finised.")
            return self._finish_card(card, done_list, used_resources, uid)

        process = psutil.Process(int(pid))

        if process.status() in [psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING]:
            return

        self._finish_card(card, done_list, used_resources, uid)

    def _finish_card(self, card: Card, done_list:List, used_resources:dict, uid: str):
        print(f"Finished card {card.name}")

        card.comment(f"✔️ Finished: {datetime.datetime.now()}")
        card.change_list(done_list.id)
        labels = card.labels or []

        for label in labels:
            if label.name in used_resources:
                used_resources[label.name] -= 1

        log_file = Path(f"{uid}.log")

        if log_file.exists():
            with open(log_file) as fp:
                card.attach(name=f"{uid}.log", file=fp)
