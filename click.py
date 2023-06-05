from clickhouse_driver import Client
from json import loads
import csv

MAX_LINES = 1000  # number of lines in the buffer


class Clickhouse:
    def __init__(self):
        self.config = Clickhouse.get_config_settings()
        self.client = self.get_db_client()

    @staticmethod
    def get_config_settings() -> dict:
        with open("config.json", "r") as config_file:
            config = loads(config_file.read())
        return config

    def get_db_client(self) -> Client:
        client = Client(host=self.config["db"]["host"],
                        user=self.config["db"]["user"],
                        password=self.config["db"]["password"],
                        port=self.config["db"]["port"])
        return client

    @staticmethod
    def create_buffer() -> None:
        with open("buffers/data.csv", "a", newline="", encoding="utf-8") as _:
            ...  # create empty file or if file exists do nothing

    @staticmethod
    def get_count_lines() -> int:
        with open("buffers/data.csv", "r", encoding="utf-8") as file:
            return len(file.readlines())

    def add_csv(self, date: str, url: str, ocl: str, oid: str,
                user_id: str) -> None:
        if url != "":
            with open("buffers/data.csv", "a", newline="",
                      encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow([date, url, ocl, oid, user_id])
            if self.get_count_lines() == MAX_LINES:
                Clickhouse.eject_to_db()

    @staticmethod
    def eject_to_db() -> None:
        with open("buffers/data.csv", "r", encoding="utf-8",
                  newline="") as file:
            reader = csv.DictReader(file,
                                    fieldnames=[
                                        "ViewDate",
                                        "URL",
                                        "ObjectClass",
                                        "ObjectID",
                                        "UserID"])
            client = click_hs.get_db_client()
            for line in reader:
                client.execute(
                    f"INSERT INTO views2 VALUES "
                    f"(CAST('{line['ViewDate']}' AS DATETIME), "
                    f"CAST('{line['URL']}' as String), "
                    f"CAST({line['ObjectID']} AS Int32), "
                    f"CAST('{line['ObjectClass']}' AS String), "
                    f"CAST('{line['UserID']}' AS UUID))")
        with open("buffers/data.csv", "w", encoding="utf-8") as _:
            ...  # clear file


click_hs = Clickhouse()
