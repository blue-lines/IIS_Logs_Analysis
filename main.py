__author__ = 'Julien Heck'


import configparser
import logging
import sys

from datetime import datetime
from pyspark import SparkConf, SparkContext

import ParseLog


if __name__ == "__main__":

    logfile_name = "log_analysis.log"

    try:
        logging.basicConfig(filename=logfile_name,
                            level=logging.WARN,
                            format='%(asctime)s, %(levelname)s, %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
    except:
        print("Failed to open log file {0}".format(logfile_name))
        sys.exit(-1)

    logging.info("Starting execution")

    try:
        config = configparser.ConfigParser()
        config.read("setup.cfg")
    except:
        logging.error("Failed to read config file")
        sys.exit(-1)
    logging.info("Parser setup")

    conf = SparkConf()\
        .setMaster("local")\
        .setAppName("IISLogAnalysis")\
        .set("spark.executor.memory", "1g")
    sc = SparkContext(conf=conf)

    logging.info("Launching Spark Context")

    # Loading file as RDD
    lines = sc.textFile("file:///sparkIISLog/u_nc160601.log.txt")
    #lines = sc.textFile("file:///sparkIISLog/test.txt")

    # Parse Log lines
    parsed_logs = lines.map(ParseLog.parseIISLogLine).cache()

    # Keeping only successfully parsed log lines
    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    content_sizes = access_logs.map(lambda log: log.content_size).cache()

    print("Content Size Avg: {0}, Min: {1}, Max: {2}".format(
        content_sizes.reduce(lambda a, b: a + b)/content_sizes.count(),
        content_sizes.min(),
        content_sizes.max()))

    logging.info("Terminating execution")