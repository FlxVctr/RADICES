import argparse
import datetime
import multiprocessing.dummy as mp
import os
import time
import traceback
from shutil import copyfile
from sys import stderr, stdout

import pandas as pd

from collector import Coordinator
from setup import Config


def main_loop(coordinator, select=[], lang=None, test_fail=False, restart=False):

    collectors = coordinator.start_collectors(select=select,
                                              lang=lang, fail=test_fail, restart=restart)

    stdout.write("\nstarting {} collectors\n".format(len(collectors)))
    stdout.flush()

    i = 0

    for instance in collectors:
        instance.join(timeout=3600)
        if instance.is_alive():
            raise mp.TimeoutError
        if instance.err is not None:
            raise instance.err
        i += 1
        stdout.write("{} collector(s) finished\n".format(i))
        stdout.flush()


if __name__ == "__main__":

    if os.path.isfile("latest_seeds.csv"):
        copyfile("latest_seeds.csv", f"{datetime.datetime.now().isoformat()}_latest_seeds.csv")

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--seeds', type=int, help="specify number of seeds", default=10)
    parser.add_argument('-l', '--language', help="specify language code of users to gather")
    parser.add_argument('-r', '--restart',
                        help="restart with latest seeds in latest_seeds.csv", action="store_true")
    parser.add_argument('-t', '--test', help="dev only: test for 2 loops only",
                        action="store_true")
    parser.add_argument(
        '-f', '--fail', help="dev only: test unexpected exception", action="store_true")
    parser.add_argument('-p', '--following_pages_limit', type=int,
                        help='''Define limit for maximum number of recent followings to retrieve per \
account to determine most followed friend.
1 page has a maximum of 5000 folllowings.
Lower values speed up collection. Default: 0 (unlimited)''', default=0)

    args = parser.parse_args()

    config = Config()

    user_details_list = []
    for detail, sqldatatype in config.config["twitter_user_details"].items():
        if sqldatatype is not None:
            user_details_list.append(detail)

    if args.restart:
        latest_seeds_df = pd.read_csv('latest_seeds.csv', header=None)[0]
        latest_seeds = list(latest_seeds_df.values)
        coordinator = Coordinator(seed_list=latest_seeds,
                                  following_pages_limit=args.following_pages_limit)
        print("Restarting with latest seeds:\n")
        print(latest_seeds_df)
    else:
        coordinator = Coordinator(seeds=args.seeds,
                                  following_pages_limit=args.following_pages_limit)

    k = 0
    restart_counter = 0

    while True:

        if args.test:
            k += 1
            if k == 2:
                args.fail = False
            if k == 3:
                break
            stdout.write("\nTEST RUN {}\n".format(k))
            stdout.flush()

        try:
            if args.restart is True and restart_counter == 0:
                main_loop(coordinator, select=user_details_list,
                          lang=args.language, test_fail=args.fail, restart=True)
                restart_counter += 1
            else:
                main_loop(coordinator, select=user_details_list,
                          lang=args.language, test_fail=args.fail)
        except Exception:
            stdout.write("Encountered unexpected exception:\n")
            traceback.print_exc()
            try:
                if config.use_notifications is True:
                    response = config.send_mail({
                        "subject": "Unexpected Error",
                        "text":
                            f"Unexpected Error encountered.\n{traceback.format_exc()}"
                    }
                    )
                    assert '200' in str(response)
                    stdout.write(f"Sent notification to {config.notif_config['email_to_notify']}")
                    stdout.flush()
            except Exception:
                stderr.write('Could not send error-mail: \n')
                traceback.print_exc(file=stderr)
            stdout.write("Retrying in 5 seconds.")
            stdout.flush()
            latest_seeds = list(pd.read_csv('latest_seeds.csv', header=None)[0].values)
            coordinator = Coordinator(seed_list=latest_seeds,
                                      following_pages_limit=args.following_pages_limit)
            args.restart = True
            restart_counter = 0
            time.sleep(5)
