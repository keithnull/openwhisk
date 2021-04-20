import random
import argparse
import time
from datetime import datetime
import json
import requests
from data_store import ActivationStore
from wsk_config import USER, PASSWORD, INVOKE_ENDPOINT, INVOKE_PARAMS, DB_FILENAME


db = ActivationStore(DB_FILENAME)


def invoke_action(name, priority, params=None):
    resp = requests.post(INVOKE_ENDPOINT + f"{name}-{priority}",
                         json=params, params=INVOKE_PARAMS,
                         auth=(USER, PASSWORD))
    if resp.status_code not in (202, 200):
        print(resp.url, resp.status_code)
        return False
    res = json.loads(resp.content)
    created_at = int(time.time() * 1000)
    print(datetime.now(), name, priority, res["activationId"])
    db.create_activation([res["activationId"]], [created_at, ])
    return True


def invoke(name, priority_dist, interval, shuffle, min_duration, max_duration):
    invocations = []
    for p, cnt in priority_dist.items():
        invocations.extend([p, ] * cnt)
    if shuffle:
        random.shuffle(invocations)
    for i, p in enumerate(invocations):
        duration = random.randint(min_duration, max_duration)
        print(f"{i:03d}/{len(invocations):03d}:", end=" ")
        invoke_action(name, p, {"ms": duration})
        if i != len(invocations) - 1:
            time.sleep(interval)


if __name__ == "__main__":
    parse = argparse.ArgumentParser(
        "Run multiple OpenWhisk actions via wsk-cli")
    parse.add_argument("--low", type=int, default=0, action="store",
                       help="The number of low priority actions (default: 0)")
    parse.add_argument("--normal", type=int, default=0, action="store",
                       help="The number of normal priority actions (default: 0)")
    parse.add_argument("--high", type=int, default=0, action="store",
                       help="The number of high priority actions (default: 0)")
    parse.add_argument("--shuffle", action="store_true",
                       help="If set, the invocation of actions will be shuffled")
    parse.add_argument("--interval", type=float, default=0.0, action="store",
                       help="The interval (in second) between activations (default: 0.0)")
    parse.add_argument("--min", type=int, default=1000, action="store",
                       help="The minimum duration (in ms) of an action (default: 1000)")
    parse.add_argument("--max", type=int, default=1000, action="store",
                       help="The maximum duration (in ms) of an action (default: min)")
    args = parse.parse_args()
    args.max = max(args.max, args.min)
    print(f"""{"-"*50}
     Low: {args.low}
  Normal: {args.normal}
    High: {args.high}
 Shuffle: {args.shuffle}
Interval: {args.interval} second(s)
     Min: {args.min} ms
     Max: {args.max} ms
{"-" * 50} """)

    invoke("sleep", {
        "low": args.low,
        "normal": args.normal,
        "high": args.high,
    }, args.interval, args.shuffle, args.min, args.max)
