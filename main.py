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

    #conf = SparkConf().setMaster("local").setAppName("IISLogAnalysis")
    #sc = SparkContext(conf=conf)

    sc = SparkContext("local", appName="IISLogAnalysis")
    logging.info("Launching Spark Context")

    lines = sc.textFile("file:///sparkIISLog/u_nc160601.log.txt")

    rdd = lines.map(ParseLog.parseIISLogLine).cache()

    content_sizes = rdd.map(lambda log: log.content_size).cache()
    print("Content Size Avg: {0}, Min: {1}, Max: {2}".format(
        rdd.reduce(lambda a, b: a + b)/content_sizes.count(),
        rdd.min(),
        rdd.max()))

    ######

    #Test

    logging.info("Terminating execution")