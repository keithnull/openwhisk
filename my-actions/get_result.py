import requests
from requests.auth import HTTPBasicAuth
import json
import re
import time

from data_store import Activation, ActivationStore
from wsk_config import USER, PASSWORD, ACTIVATIONS_ENDPOINT, ACTIVATIONS_PARAMS, DB_FILENAME

POLL_INTERVAL = 5.0  # s
SUCCESSIVE_FAILURES = 6

RESULT_PATTERN = re.compile('Sleeping\\s+(\\d+)ms\\.')

seen = set()
all_activations = list()


def fetch():

    def process_record(record):
        activation_id = record["activationId"]
        if activation_id in seen:
            return False
        seen.add(activation_id)
        name = record["name"]
        priority = name.split("-")[-1]
        start, end = record["start"], record["end"]
        duration = record["duration"]
        execution_time = 0
        try:
            result_body = record["response"]["result"]["body"]
            execution_time = int(RESULT_PATTERN.findall(result_body)[0])
        except Exception as e:
            print(e, record)
        res = Activation(
            activation_id, priority, name, start, end, duration, execution_time
        )
        print(res)
        all_activations.append(res)
        return True

    req = requests.get(ACTIVATIONS_ENDPOINT, params=ACTIVATIONS_PARAMS,
                       auth=HTTPBasicAuth(USER, PASSWORD))
    print(req.status_code)
    if req.status_code != 200:
        return False
    activations = json.loads(req.content)
    # reverse it to process old records first
    return any([process_record(r) for r in activations[::-1]])


if __name__ == "__main__":
    print("Hi, let's begin")
    failures = 0
    while failures < SUCCESSIVE_FAILURES:
        failures = 0 if fetch() else failures + 1
        time.sleep(POLL_INTERVAL)
    print("Okay, it's time to say goodbye")
    print("-" * 40)
    for record in all_activations:
        print(record)
    db = ActivationStore(DB_FILENAME)
    db.update_activation(all_activations)
