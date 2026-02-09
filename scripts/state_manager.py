import json
import os


class StateManager:
    def __init__(self, channel_handle) -> None:
        scripts = os.path.dirname(__file__)
        file_path = os.path.join(
            scripts, "..", "data", "raw", f"{channel_handle}", "state.json"
        )
        self.file_path = file_path
        self.state = self._load_state()

    def _load_state(self):
        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError:
            print(f"{self.file_path} is corrupted. Starting with empty state.")
            return {}

    def update_channel_state(self, **kwargs):
        """**kwargs could include fetch_date, status, nextPageToken, current_date"""
        self.state.update(kwargs)

    def save_state(self):
        with open(self.file_path, "w") as f:
            json.dump(self.state, f, indent=4)
