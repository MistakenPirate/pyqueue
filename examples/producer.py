#!/usr/bin/env python3
"""Example producer that pushes messages to PyQueue."""

import sys
import time

sys.path.insert(0, str(__file__).rsplit("/", 2)[0])

from pyqueue.client import PyQueueClient


def main():
    print("Connecting to PyQueue broker...")

    with PyQueueClient() as client:
        print("Connected! Pushing 10 messages...")

        for i in range(10):
            message = f"Message {i + 1} at {time.time()}"
            client.push(message)
            print(f"  Pushed: {message}")

        print("Done!")


if __name__ == "__main__":
    main()
