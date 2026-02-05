import json
import os


class StateManager:
    def __init__(self, file_path=None) -> None:
        if file_path is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(script_dir, "state.json")
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

    def get_channel_state(self, channel_handle):
        return self.state.get(channel_handle, {})

    def update_channel_state(self, channel_handle, **kwargs):
        """**kwargs could include fetch_date, status, nextPageToken, current_date"""
        if channel_handle not in self.state:
            self.state[channel_handle] = {}

        self.state[channel_handle].update(kwargs)

    def save_state(self):
        with open(self.file_path, "w") as f:
            json.dump(self.state, f, indent=4)
