import os


def create_action(name, filepath, priority):
    os.system(
        f"wsk action create {name}-{priority} {filepath} -a priority {priority}")


if __name__ == "__main__":
    for p in ["high", "low", "normal"]:
        create_action("sleep", "./sleep.js", p)
