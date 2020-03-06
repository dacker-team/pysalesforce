from pyred import RedDBStream

import pysalesforce
from pysalesforce.Salesforce import Salesforce
from pysalesforce.date import *

import time

s = Salesforce('LEDGER', 5)
s.main()
