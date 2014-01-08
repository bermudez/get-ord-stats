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
# comparestats(outputfile, args)
#
# Purpose: Compares multiple ORD stats from multiple csv files.
#*********************************************
def comparestats(outputfile, args):
   print('')
   numfiles = len(args)
   
   if numfiles < 2:
      print('You must provide at least 2 csv files containing ORD statistics.')
      sys.exit()

   o = open(outputfile, "w")

   # Create a temp database in memory with # of tables matching # of csv files
   connection = sqlite3.connect(":memory:")
   cursor = connection.cursor()

   filecounter=0
   strctr = str(filecounter) #string counter
   for eachfile in args:
      sql = "CREATE TABLE stats_table{0} ( \
      Ranking integer, \
      TransactionName text, \
      ORDthreshold integer, \
      TransactionCount integer, \
      AvgTransactionResponse real)".format(strctr)

      cursor.execute(sql)
      filecounter = filecounter + 1
      strctr = str(filecounter)

   connection.commit()

   # Load the CSV file into CSV reader
   filecounter=0
   strctr = str(filecounter) #string counter
   for eachfile in args:
      csvfile = open(args[filecounter], 'r')
      creader = csv.reader(csvfile, delimiter=',', quotechar='|')

      # Iterate through the CSV reader, inserting values into the database
      next(creader, None)  # Note: skip first line (i.e. header line)
      for t in creader:
         sql = "INSERT INTO  stats_table{0} VALUES (?,?,?,?,?)".format(strctr)
         cursor.execute(sql, t )
      filecounter = filecounter + 1
      strctr = str(filecounter) #string counter
      #Progress output
      sys.stdout.write('.')
      sys.stdout.flush()
       

   # Create/open the output CSV file into CSV reader
   csvfile = open(outputfile, 'w')
   
   # Iterate thru stats tables and write the corresponding statistics
   filecounter=0
   for eachfile in args:
      # Write header line
      csvfile.write(args[filecounter][:-4]+',,,,,,')
      filecounter=filecounter + 1
   csvfile.write('\n')

   for eachfile in args:
      # Write header line
      csvfile.write('Ranking,TransactionName,' \
                     +'ORDthreshold,TransactionCount,' \
                     +'AvgTransactionResponse," ",')
  
   csvfile.write('\n')
    

   # Pick the stats_table0
   cursor.execute("SELECT TransactionName from stats_table0 ORDER BY Ranking")
   transactionlist = cursor.fetchall()

   # Print the statistics for all tables
   progresscounter = 0
   badcharacters = "[]()|'"
   badcharacters2 = "(),"
   for tx in transactionlist:
      filecounter=0
      strctr = str(filecounter) #string counter
      # Iterate thru all the stats_tables
      tx_string = str(tx).strip(badcharacters2)
      for eachfile in args:
         sql = "SELECT * from stats_table{0} WHERE TransactionName is {1}".format(strctr,tx_string)

         cursor.execute(sql)

         table_entry = str(cursor.fetchall())
         temp_string = table_entry.strip(badcharacters)
         table_entry = temp_string
         temp_string = table_entry.replace("'","")
         table_entry = temp_string
         csvfile.write(table_entry+',,')
         filecounter = filecounter + 1
         strctr = str(filecounter) #string counter
      # Go to next line
      csvfile.write('\n')

      # Show progress indicator to user
      if progresscounter%10 == 0:
         sys.stdout.write('.')
      progresscounter = progresscounter + 1
   # Final carriage return for progress indicator
   sys.stdout.write('\n')
 
   # Close the csv file, commit changes, and close the connection
   csvfile.close()
   connection.commit()
   connection.close()



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
   
   CountInputFiles = len(args)

   # Compare statistics between 2 or more ORD stats file
   comparestats(outputfile, args)
  
   # Show a summary of what was done.
   print('')
   print('-------SUMMARY-------')
   print('1. ',CountInputFiles,' files were read.')
   print('2.  CSV file with the compared ORD Statistics is: ', outputfile)
   print('---------------------')


if __name__ == "__main__":
   main(sys.argv[1:])
