import argparse
from datetime import datetime
import os
import time
import traceback
from shutil import copyfile
from sys import stderr, stdout

import pandas as pd

from collector import Coordinator
from setup import Config


def main_loop(coordinator, select=[], status_lang=None, test_fail=False, restart=False,
              bootstrap=False, language_threshold=0, keywords=[]):

    try:
        latest_start_time = pd.read_sql_table('timetable', coordinator.dbh.engine)
        latest_start_time = latest_start_time['latest_start_time'][0]
    except ValueError:
        latest_start_time = 0

    if restart is True:

        update_query = f"""
                        UPDATE friends
                        SET burned=0
                        WHERE UNIX_TIMESTAMP(timestamp) > {latest_start_time}
                       """
        coordinator.dbh.engine.execute(update_query)

    start_time = time.time()

    pd.DataFrame({'latest_start_time': [start_time]}).to_sql('timetable', coordinator.dbh.engine,
                                                             if_exists='replace')

    collectors = coordinator.start_collectors(select=select,
                                              status_lang=status_lang,
                                              fail=test_fail,
                                              restart=restart,
                                              retries=4,
                                              latest_start_time=latest_start_time,
                                              bootstrap=bootstrap,
                                              language_threshold=language_threshold,
                                              keywords=keywords)

    stdout.write("\nstarting {} collectors\n".format(len(collectors)))
    stdout.write(f"\nKeywords: {keywords}\n")
    stdout.flush()

    i = 0
    timeout = 3600

    for instance in collectors:
        instance.join(timeout=timeout)
        if instance.is_alive():
            raise RuntimeError(f"Thread {instance.name} took longer than {timeout} seconds \
to finish.")
        if instance.err is not None:
            raise instance.err
        i += 1
        stdout.write(f"Thread {instance.name} joined. {i} collector(s) finished\n")
        stdout.flush()


if __name__ == "__main__":

    # Backup latest_seeds.csv if exists
    if os.path.isfile("latest_seeds.csv"):
        copyfile("latest_seeds.csv",
                 "{}_latest_seeds.csv".format(datetime.now().isoformat().replace(":", "-")))

    # Get arguments from commandline
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--seeds', type=int, help="specify number of seeds", default=10)
    parser.add_argument('-l', '--language', nargs="+",
                        help="specify language codes of last status by users to gather")
    parser.add_argument('-lt', '--lthreshold', type=float,
                        help="fraction threshold (0 to 1) of last 200 tweets by an account that \
must have chosen languages detected (leads to less false positives but \
also more false negatives)", default=0)
    parser.add_argument('-k', '--keywords', nargs="+",
                        help="specify keywords contained in last 200 tweets by users to gather")
    parser.add_argument('-r', '--restart',
                        help="restart with latest seeds in latest_seeds.csv", action="store_true")
    parser.add_argument('-p', '--following_pages_limit', type=int,
                        help='''Define limit for maximum number of recent followings to retrieve per \
account to determine most followed friend.
1 page has a maximum of 5000 folllowings.
Lower values speed up collection. Default: 0 (unlimited)''', default=0)
    parser.add_argument('-b', '--bootstrap', help="at every step, add seeds' friends to seed pool",
                        action="store_true")
    parser.add_argument('-t', '--test', help="dev only: test for 2 loops only",
                        action="store_true")
    parser.add_argument('-f', '--fail', help="dev only: test unexpected exception",
                        action="store_true")

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
                          status_lang=args.language, test_fail=args.fail, restart=True,
                          bootstrap=args.bootstrap, language_threshold=args.lthreshold,
                          keywords=args.keywords)
                restart_counter += 1
            else:
                main_loop(coordinator, select=user_details_list,
                          status_lang=args.language, test_fail=args.fail, bootstrap=args.bootstrap,
                          language_threshold=args.lthreshold, keywords=args.keywords)
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
