#!/usr/bin/env python3
"""Example consumer that pulls messages from PyQueue."""

import sys
import argparse

sys.path.insert(0, str(__file__).rsplit("/", 2)[0])

from pyqueue.client import PyQueueClient


def main():
    parser = argparse.ArgumentParser(description="Consume messages from PyQueue")
    parser.add_argument(
        "--consumer-id",
        default="example-consumer",
        help="Consumer ID for offset tracking (default: example-consumer)",
    )
    parser.add_argument(
        "--poll",
        action="store_true",
        help="Keep polling for new messages",
    )
    args = parser.parse_args()

    print(f"Connecting to PyQueue broker as '{args.consumer_id}'...")

    with PyQueueClient() as client:
        print("Connected! Pulling messages...")

        message_count = 0
        while True:
            message = client.pull(args.consumer_id)
            if message is None:
                if args.poll:
                    import time
                    time.sleep(1)
                    continue
                else:
                    print("No more messages available.")
                    break
            message_count += 1
            print(f"  Received: {message}")

        print(f"Done! Received {message_count} messages.")


if __name__ == "__main__":
    main()
