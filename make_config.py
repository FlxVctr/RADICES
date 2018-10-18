from shutil import copyfile
import subprocess
import sys
import os


def make_config():
    """Creates a new config.yml from a template when the script is run from shell
    directly.

    Args:
        None
    Returns:
        Nothing
    """

    copyfile('config_template.yml', 'config.yml')


if __name__ == '__main__':
    i = 0
    while True:
        if i == 0:
            answer = input('''This program will create a new config.yml.\n
                            After running it, you will be asked with which program to\n
                            open the new file. Please choose your standard text editor.\n
                            Do you wish to create a new config.yml now? (y/n): ''')
        else:
            answer = input('''Sorry, I did not get your input. Do you wish to create \n
                           a new config.yml now? Pleaser answer y for yes or n for no: ''')
        if answer == "n":
            break
        elif answer == "y":
            make_config()
            if sys.platform.startswith('darwin'):
                subprocess.call(('open', "config.yml"))
            elif os.name == 'nt':  # For Windows
                os.startfile("config.yml")
            elif os.name == 'posix':  # For Linux, Mac, etc.
                subprocess.call(('xdg-open', "config.yml"))
            break
        else:
            i = 1
            pass
