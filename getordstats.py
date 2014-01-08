#!/usr/bin/python3

import sys, getopt, os.path, glob
import csv
import sqlite3

def printversion():
   print ('getordstats.py')
   print ('version 0.5')
   print ('by Enrico Bermudez, 2013-12-28')

def printhelp():
   print('')
   print('PURPOSE:  Create ORD statistics from multiple E2E csv files.')
   print('    Also creates a single csv file from the input files.')
   print('    Also creates a single sqlite3 db files from the input files.')
   print('')
   print('SYNTAX:')
   print('python3 getordstats.py -i <inputfile> -o <outputfile>')
   print('python3 getordstats.py -i <inputfile1> <inputfile2> ...')
   print('python3 getordstats.py -i <inputfiles_with_wildcard>')
   print('')
   print('OTHER OPTIONS:')
   print('-h  Prints this help info.')
   print('-v  Prints the version.')
   print('')
   print('EXAMPLE USES:')
   print('python3 getordstats.py -i testinput*.csv')
   print('python3 getordstats.py -i testinput*.csv -o myoutput')

#*********************************************
# mergefiles(inputfile, outputfile, args)
#
# Purpose: Merge one or multiple csv inputfiles into on csv outputfile.
#*********************************************
def mergefiles(inputfile, outputfile, args):
   print('')
   numfiles = len(args)
   o = open(outputfile, "w")
   
   # Copy header line into table.
   x = "**********  FOR OFFICIAL USE ONLY  **********"  

   while len(args) != 0:
      inputfile = args[0]
      args.pop(0)

      if os.path.isfile(inputfile) and os.path.exists(inputfile):
         p=""
         f = open(inputfile)
         header_line = f.readline()
         while header_line.startswith(x):
            header_line = f.readline()
         for line in f:
             p= line
             o.write(p)
         f.close()
         print(inputfile,' ---merged---> ', outputfile)
      else:
         print('Input file does not exist.  Input filename is ', inputfile)
         exit()
   if numfiles > 1:
         print(numfiles,' files were merged.')
   else:
      print(numfiles,' file was merged.')
   o.close()

#****************************************************
# Create_sqlite_table(inputfile, outputfile)
#
# Purpose:  Creates a .db file with a table named 'transactions'.
#           Then creates the field names (23 total) according to the E2E file.
#           Then copies all items from the csv inputfile into the .db file.
#****************************************************

def create_sqlite_table(inputfile, outputfile):

   print('')
   print('Creating the sqlite3 db file...')

   if not(os.path.isfile(inputfile) and os.path.exists(inputfile)):
      print('Error in reading the inputfile.  Input filename is: ', inputfile)
      exit()

   # Create the database
   connection = sqlite3.connect(outputfile)
   cursor = connection.cursor()

   # Create the table
   cursor.execute('DROP TABLE IF EXISTS transactions')

   cursor.execute('CREATE TABLE  transactions ( \
   UserName text, \
   TransactionName text, \
   ORD integer, \
   WorkstationName text, \
   WorkstationIPAddress text, \
   TxStartTime text, \
   TxEndTime text, \
   TxResponseTime real, \
   TxStatus text, \
   Metric1 text, \
   Metric1Value text, \
   Metric2 text, \
   Metric2Value text, \
   Metric3 text, \
   Metric3Value text, \
   Metric4 text, \
   Metric4Value text, \
   Metric5 text, \
   Metric5Value text, \
   Metric6 text, \
   Metric6Value text, \
   Metric7 text, \
   Metric7Value text) \
   ')

   connection.commit()

   # Load the CSV file into CSV reader
   csvfile = open(inputfile, 'r')
   creader = csv.reader(csvfile, delimiter=',', quotechar='|')

   # Iterate through the CSV reader, inserting values into the database
   next(creader, None)  # Note: skip first line (i.e. header line)
   progresscount = 0
   for t in creader:
      cursor.execute('INSERT INTO  transactions \
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', t )
      #Progress output
      if progresscount%100000 > 0:
         progresscount = progresscount + 1
      else:
         progresscount = progresscount + 1
         sys.stdout.write('.')
         sys.stdout.flush()

   # Print out notice to user.
   print('')
   print(inputfile,' ----sqlite3 .db file created---> ', outputfile)

   # Close the csv file, commit changes, and close the connection
   csvfile.close()
   connection.commit()
   connection.close()

#*******************************************
#  create_stats(inputfile, outputfile)
#
#  PURPOSE: 
#
#*******************************************
def create_stats(inputfile, outputfile):
   
   print('')
   print('Creating ORD Statistics...')

   if not(os.path.isfile(inputfile) and os.path.exists(inputfile)):
      print('Input file does not exist.  Input filename is ', inputfile)
      exit()

   badcharacters = "[]()|'"

   # Open the database
   connection = sqlite3.connect(inputfile)
   cursor = connection.cursor()

   # Create a temp database in memory
   connection2 = sqlite3.connect(":memory:")
   cursor2 = connection2.cursor()

   cursor2.execute('CREATE TABLE stats_table ( \
   TransactionName text, \
   ORD integer, \
   TxCount integer, \
   AvgTxResponse real) \
   ')

   connection.commit()
   # Create/open the CSV file into CSV reader
   csvfile = open(outputfile, 'w')
   
   # Write header line
   csvfile.write('Ranking,TransactionName,ORDthreshold,TransactionCount,AvgTransactionResponse')
   csvfile.write('\n')

   # Create the table
   valid_tx = ""
   cursor.execute('SELECT DISTINCT TransactionName FROM transactions WHERE TxStatus is "ARM_GOOD"')
   valid_tx = cursor.fetchall()
   
   progresscount = 0

   for tx in valid_tx:
      transaction_str = str(tx)
      newstring = transaction_str.strip(badcharacters)
      transaction_str = newstring
      newstring = transaction_str.replace("'","")
      transaction_str = newstring
      
      if not(transaction_str.startswith("Workflow")):
         cursor.execute(" SELECT TransactionName, avg(ORD), \
                       count(TxStatus), avg(TxResponseTime) \
                       FROM transactions \
                       WHERE TransactionName is '%s' AND \
                       TxStatus is 'ARM_GOOD' " % tx)
         row = cursor.fetchone()
         cursor2.execute("INSERT INTO stats_table VALUES (?,?,?,?)", row )
         #Progress output
         if progresscount%5 > 0:
            progresscount = progresscount + 1
         else:
            progresscount = progresscount + 1
            sys.stdout.write('.')
            sys.stdout.flush()
      
         
  
   # Iterate through the statistics, inserting values into the csv file 

   newstring = ""
   cursor2.execute("SELECT * FROM stats_table ORDER BY TxCount DESC")
   row = str(cursor2.fetchone())
   ranking = 0

   while row != "None":
      newstring = row.strip(badcharacters)
      row = newstring
      newstring = row.replace("'","")
      row = newstring
      ranking = ranking + 1
      str_ranking = str(ranking)
      csvfile.write(str_ranking+','+row+"\n")
      row = str(cursor2.fetchone())
   connection.commit()

   # Print out notice to user.
   print('')
   print(inputfile,' ----stats generated---> ', outputfile)

   # Close the csv file, commit changes, and close the connection
   csvfile.close()
   connection.commit()
   connection.close()
   connection2.commit()
   connection2.close()


#***************************
# main(argv)
#***************************
def main(argv):
   inputfile = ''
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:o:", ["ifile=", "ofile="])
   except getopt.GetoptError:
      printhelp()
      sys.exit(2)

   # Check for multiple input files and one output file
   oindex=-1
   for i, argstring in enumerate(args):
      if '-o' == argstring:
          obit=True
          oindex=i
   if oindex != -1:
      outputfile = args[oindex+1]
      while oindex < len(args):
          args.pop(oindex)
   else:
      outputfile = "out_default.csv"

   for opt, arg in opts:
      if opt == '-h':
         printhelp()
         sys.exit()
      elif opt == '-v':
         printversion()
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
         args.insert(0,inputfile)
      elif opt in ("-o", "--ofile"):
         outputfile = arg
   if outputfile == "":
      outputfile = "out_"+inputfile
   
   if not(outputfile.endswith('.csv')):
      outputfile = outputfile+'.csv'

   #************
   # Open and process the file(s).  Includes error handling.
   #************
   
   CountMergedFiles = len(args)

   # Merge one or multiple files into one clean output csv file.
   mergefiles(inputfile, outputfile, args)
  
   # Create the sqlite db file.
   # Then create the data table (table is called 'transactions').
   # Then copy all data into the sqlite db file.
   mergedoutputfile = outputfile
   if outputfile.endswith(".csv"):
      sqliteoutputfile = outputfile[:-4]+".db"
   else:
      sqliteoutputfile = outputfile+".db"
   create_sqlite_table(mergedoutputfile, sqliteoutputfile)

   # Create the ORD statistics in an output csv file.
   statisticsoutputfile = mergedoutputfile[:-4]+"_ORDStatistics.csv"
   create_stats(sqliteoutputfile, statisticsoutputfile)

   # Show a summary of what was done.
   print('')
   print('-------SUMMARY-------')
   if CountMergedFiles > 1:
      print('1. ',CountMergedFiles,' files were cleaned and merged.')
   else:
      print('1. ',CountMergedFiles,' file was cleaned (and merged).')
   print('2.  Merged and cleaned CSV filename is:  ', mergedoutputfile)
   print('3.  Sqlite3 .db file of the dataset is:  ', sqliteoutputfile)
   print('4.  CSV file with the ORD Statistics is: ', statisticsoutputfile)
   print('---------------------')


if __name__ == "__main__":
   main(sys.argv[1:])
