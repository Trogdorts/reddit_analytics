#!/usr/bin/env python3
from os import listdir, makedirs, rename, remove
from os.path import isfile, join, split, isdir
import json
from tqdm import tqdm
import sys
import argparse
from datetime import date
from multiprocessing import Process, Queue


def process_file(file):
    # Work for each file goes here
    print("Processing file ", file)
    INPUT_working_file = join(INPUT_COMMENTS_DIR, file)
    save_file = join(WORKING_DIR, file)
    tmp_save_file = open(save_file + ".tmp", "a")

    if args.verbose:
        print(INPUT_working_file)

    with open(INPUT_working_file, "r") as infile:
        line_count = 0
        for line in infile:
            
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
            line_count += 1
            if args.sample:
                if line_count >= args.sample:
                    line_count == 0
                    break

    tmp_save_file.close()
    rename(save_file+ ".tmp", save_file)



def read_queue(queue_of_files):
    while True:
        try:
            file = queue_of_files.get_nowait()
            process_file(file)
        except Exception as e:
            # The queue is empty, so we are done and can break out of our loop
            print("exception found", e)
            break

def check_positive(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue

def main(args):
    number_of_workers = 20
    queue = Queue()
    workers = []

    file_index = 0

    for file in INPUT_COMMENT_FILES:
        queue.put(file)    

    for w in range(number_of_workers):
        # Make a new process that runs the process_file function which takes a queue as the first argument
        worker = Process(target=read_queue, args=(queue,))

        workers.append(worker) # Add it to the list of workers that we'll wait for at the end
        worker.start() # Tell it to start running



    # Wait for the workers to all finish before exiting
    for worker in workers:
        worker.join() # Wait for the worker to be done
    return True


if __name__ == '__main__':

    # Default folder locations - Change if you are not ME
    INPUT_COMMENTS_DIR = "/mnt/mediashare/data/git/reddit_dev/raw_reddit_comments/"
    OUTPUT_DIR = "/mnt/mediashare/data/git/reddit_dev/processed_reddit_comments/"


    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--resume",  action='store_true', help="Resume from previous run.")
    parser.add_argument("-n", "--run-name", default= str(date.today()),help="Name of the folder to store the files in. Defaults to date.")
    parser.add_argument("-c", "--INPUT_COMMENTS_DIR", default= INPUT_COMMENTS_DIR,help="INPUT Comments folder.")
    parser.add_argument("-o", "--OUTPUT_DIR", default= OUTPUT_DIR,help="Processed Comments folder.")
    parser.add_argument("-x", "--exit", action='store_true', help="Exit after printing out info, for dev use.")
    parser.add_argument("-s", "--sample", type=check_positive, help="The number of entries you want to grab for each file.")
    parser.add_argument("-v", "--verbose", action='store_true', default=False, help="Print out messages to the console.")
    args = parser.parse_args()


    WORKING_DIR = join(args.OUTPUT_DIR, args.run_name)

    if not isdir(WORKING_DIR):
        makedirs(WORKING_DIR)

    INPUT_COMMENT_FILES = [f for f in listdir(INPUT_COMMENTS_DIR) if isfile(join(INPUT_COMMENTS_DIR, f))]
    PROCESSED_COMMENT_FILES = [f for f in listdir(WORKING_DIR) if isfile(join(WORKING_DIR, f))]

    if args.resume:
        # remove completed files from the list to process

        # Create a list of any temp files
        tmp_files = [string for string in PROCESSED_COMMENT_FILES if ".tmp" in string]

        # Remove the temp files from the already processed list
        PROCESSED_COMMENT_FILES = [x for x in PROCESSED_COMMENT_FILES if x not in tmp_files]

        # Delete temp files - at some point will want to resume temp files
        for file in tmp_files:
            remove(join(WORKING_DIR, file))

        # Remove processed files from the list of files that need to be processed
        INPUT_COMMENT_FILES = [x for x in INPUT_COMMENT_FILES if x not in PROCESSED_COMMENT_FILES]

        if args.verbose:
            print("INPUT files: ", len(INPUT_COMMENT_FILES))
            print("INPUT files: ", INPUT_COMMENT_FILES)
            print("Processed files: ", len(PROCESSED_COMMENT_FILES))

    if args.exit:
        print("Running in dev/exit mode. Will exit after printing out data.")
        print("Number of files in input folder:", len(INPUT_COMMENT_FILES))
        print("Processed Files: ", PROCESSED_COMMENT_FILES)
        print("OUTPUT_DIR", args.OUTPUT_DIR)
        print("WORKING_DIR", WORKING_DIR)
        if args.sample:
            print("Sample Size from each file:", str(args.sample))
        sys.exit()

    main(args)