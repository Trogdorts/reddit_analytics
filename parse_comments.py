#!/usr/bin/env python3
from os import listdir, makedirs, rename, remove
from os.path import isfile, join, split, isdir
import json
from tqdm import tqdm
import sys
import argparse
from datetime import date


RAW_COMMENTS_DIR = "/mnt/mediashare/data/git/reddit_dev/raw_reddit_comments/"
PROCESSED_COMMENTS_DIR = "/mnt/mediashare/data/git/reddit_dev/processed_reddit_comments/"


parser = argparse.ArgumentParser()
parser.add_argument("-r", "--resume",  action='store_true', help="Resume from previous run.")
parser.add_argument("-n", "--run-name", default= str(date.today()),help="Name of the folder to store the files in. Defaults to date.")
parser.add_argument("-x", "--exit", action='store_true', help="Exit after printing out info, for dev use.")
parser.add_argument("-v", "--verbose", action='store_true', default=False, help="Print out messages to the console.")
options = parser.parse_args()


WORKING_DIR = join(PROCESSED_COMMENTS_DIR, options.run_name)

if not isdir(WORKING_DIR):
    makedirs(WORKING_DIR)

RAW_COMMENT_FILES = [f for f in listdir(RAW_COMMENTS_DIR) if isfile(join(RAW_COMMENTS_DIR, f))]
PROCESSED_COMMENT_FILES = [f for f in listdir(WORKING_DIR) if isfile(join(WORKING_DIR, f))]

if options.resume:
    # remove completed files from the list to process

    # Create a list of any temp files
    tmp_files = [string for string in PROCESSED_COMMENT_FILES if ".tmp" in string]

    # Remove the temp files from the already processed list
    PROCESSED_COMMENT_FILES = [x for x in PROCESSED_COMMENT_FILES if x not in tmp_files]

    # Delete temp files - at some point will want to resume temp files
    for file in tmp_files:
        remove(join(WORKING_DIR, file))

    # Remove processed files from the list of files that need to be processed
    RAW_COMMENT_FILES = [x for x in RAW_COMMENT_FILES if x not in PROCESSED_COMMENT_FILES]

    if options.verbose:
        print("Raw files: ", len(RAW_COMMENT_FILES))
        print("Processed files: ", len(PROCESSED_COMMENT_FILES))

if options.exit:
    print("Processed Files: ", PROCESSED_COMMENT_FILES)
    sys.exit()

try:

    for file in tqdm(RAW_COMMENT_FILES):
        raw_working_file = join(RAW_COMMENTS_DIR, file)
        save_file = join(WORKING_DIR, file)
        tmp_save_file = open(save_file + ".tmp", "a")

        if options.verbose:
            print(raw_working_file)

        with open(raw_working_file, "r") as infile:
            
            for line in tqdm(infile):
                jsontxt = json.loads(line)
                entry = str(jsontxt["body"])

                if entry == "[deleted]" or entry == "[removed]" or entry == " ":
                    continue

                #entry = "XXX" + entry #Use this to mark the beginning of a new entry

                entry = entry.rstrip('\n')
                entry = entry.replace('\r', ' ')
                entry = entry.replace('\n', ' ')
                entry = entry.replace('\t', ' ')
                entry = entry.replace('    ', ' ')
                entry = entry + '\n'

                tmp_save_file.write(entry)

        tmp_save_file.close()
        rename(save_file+ ".tmp", save_file)

except KeyboardInterrupt:
    print ('KeyboardInterrupt exception is caught')
    tmp_save_file.close()
    sys.exit()


