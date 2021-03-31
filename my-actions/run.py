import os
import random


def invoke_action(name, priority, params=None):
    params_str = " ".join(" ".join((k, v)
                                   for k, v in params)) if params else ""
    os.system(f"wsk action invoke {name}-{priority} {params_str}")


def invoke(name, priority_dist, shuffle=False):
    invocations = []
    for p, cnt in priority_dist.items():
        invocations.extend([p, ] * cnt)
    if shuffle:
        random.shuffle(invocations)
    for p in invocations:
        invoke_action(name, p)


if __name__ == "__main__":
    invoke("sleep", {
        "low": 0,
        "normal": 200,
        "high": 0,
    }, True)
