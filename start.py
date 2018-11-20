import argparse
from sys import stdout

from collector import Coordinator
from setup import Config

parser = argparse.ArgumentParser()
parser.add_argument('-n', '--seeds', type=int, help="specifiy number of seeds", default=10)
parser.add_argument('-l', '--language', help="specify language code of users to gather")
parser.add_argument('-t', '--test', help="test for 2 loops only", action="store_true")

args = parser.parse_args()

if __name__ == "__main__":

    config = Config()

    user_details_list = []
    for detail, sqldatatype in config.config["twitter_user_details"].items():
        if sqldatatype is not None:
            user_details_list.append(detail + " " + sqldatatype)

    coordinator = Coordinator(seeds=args.seeds)

    while True:

        if args.test:
            k = 0
            k += 1
            if k == 3:
                break

        collectors = coordinator.start_collectors(select=user_details_list,
                                                  lang=args.language)

        stdout.write("\nstarting {} collectors\n".format(len(collectors)))
        stdout.flush()

        i = 0

        for instance in collectors:
            instance.join()
            i += 1
            stdout.write("\r{} collectors finished".format(i))
