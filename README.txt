######  OHIO Injection Well data conversion #####

The code in these files is used to translate data in the ODNR spreadsheets
that record the fees collected for injection well use to a coherent single
data set that document the volume of waste injected.

The process of that translation is complicated because of a number of factors.
- Between 2010 and now (2019), four separate formats have been used by ODNR
to record the data.  Each format requires different code to handle the translation.
- The injection sites are not always well identified; typically an API number is 
used, sometimes with additional numbers (perhaps indicating different injection
points?).  These API numbers should be registered in the state's database of wells but
that is not always true.  From year to year (and even quarter to quarter) some wells are
labeled slightly differently, so we have to make them equivalent. Some wells are labeled
without an API number at all.  To handle all of these issues, we generate a translation
table for every Well Identifier that is not in a list of recognized numbers.
- While the data sheets mostly record a single entry for a given quarter, there are sometimes
there are duplicates.  For each duplicate we have to decide whether the numbers are meant to 
be additive or if the last number entered is a correction.  For this we have another
command file that describes the behavior for each of these duplicate entries.

Directions:
1) Copy new files (and updated copies of older files) into the sources directory. These
files currently must be requested from ODNR directly.  As of this writing, the data files
are included in the github repository of the project at:
https://github.com/gwallison/OH_injection_wells_compile

2) Edit the python file: processInjectionInput.py to include any new files in the lists of files,
probably "fn_2018_plus".  Follow the format of the files already in that list.  Note that this all
assumes that the new files you are adding conform to the format of the previous files!  Mostly that
means that the data are in the same column formats, named the same thing, the header is the same number
of rows, and the excel sheets are used in the same way.  If they aren't, a new function will have to 
be written to work with the new format.  This is not a small task...

3)  Run this code.  If it runs without errors, the output should look something like this:

  ***** Processing BRINE DISPOSAL FEES FOR 2018.xlsx

Processing BRINE DISPOSAL FEES FOR 2018.xlsx,2018
  working on quarter 1
  collapse set for this worksheet: {'3413322860'}
  working on quarter 2
  collapse set for this worksheet: set()
  working on quarter 3
  collapse set for this worksheet: set()
  working on quarter 4
  collapse set for this worksheet: set()

  ***** Processing BRINE DISPOSAL FEES FOR 2019.xlsx

Processing BRINE DISPOSAL FEES FOR 2019.xlsx,2019
  working on quarter 1
  collapse set for this worksheet: {'3412122459'}
  working on quarter 2
  collapse set for this worksheet: {'3412122459'}
  working on quarter 3
  collapse set for this worksheet: {'3412122459'}
  working on quarter 4
  collapse set for this worksheet: {'3412122459'}  

However, there are two likely conditions that will needed attention.  The first is that the code 
doesn't complete and that is probably because there are duplicates of a particular API in a given quarter.
There are three lines of code directly above the assert statement (the likely cause of the abort) that is usually
commented out. Uncomment that code: it will help you figure out what is duplicated.  Add an entry to each quarter
for each duplicate in aggregateAPI.txt to instruct the code how to handle each duplicate.  
The second source of attention are the lists of records for which the API is not recognized.  That list can 
be copied directly into the xlateAPI.txt file.  Just replace the API_HERE text with the actual API to use and 
add a comment...

4) Finally, run the code: mergeToFTformat.py.  This should create two files in the /out directory, one has
the metadata for the individual APIs and the other is a tall-format sheet will all the volume data together.

 