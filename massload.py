import pandas as pd
import datetime
import logging
import random
import glob
from pandas import Series, DataFrame
import csv
import re
import os


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    filename='massload.log',
                    filemode='w')
                    
class Inputs(object):
	"""
	Input df objects to mass merge
	"""
	def __init__(self, separator=',', skiprows=None, skipfooter=None, path):
		self.path = path
		self.skipfooter = skipfooter
		self.separator = separator							      #comma separator default 
		self.skiprows = skiprows
		self.col_headers = []
		self.col_headers = self.header_getter(self.path)     #grabs headers once to speed up merging

	def header_getter(self, path):
	"""Grabs a random file from the provided path and gets the column headers"""

		random_file = random.choice(os.listdir(path))
		with open(self.path + "/"+ random_file, 'r') as fh:
			dialect = csv.Sniffer().sniff(fh.read(4096),delimiters='\t,;|\s')      #checks for the proper delimiter
			fh.seek(0)

			reader = csv.reader(fh, dialect)
			for row in reader:
				if len(row) >= 4:
					self.col_headers = row
					break
		logging.info("Column Headers: %s ", str(self.col_headers))
		return self.col_headers


	def massread(self):
	        """Reads path and merges all files into one dataframe"""
	    	allfiles = glob.glob(self.path + '/*.*')
	    	df = [pd.read_csv(file_, sep = self.separator, skiprows = self.skiprows, skipfooter = self.skipfooter, usecols=self.col_headers, engine = 'python') for file_ in allfiles]
	    	frame = pd.concat(df)
	    	logging.info("----------Done with Massreading----------")
	    	return frame


#initialize the class and merge
merge = Inputs(separator='/t', skiprows=1, skipfooter=3, 'F:/Desktop/TestFiles')

#returns master dataframe
df = merge.massread()


