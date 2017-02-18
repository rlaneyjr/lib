#!/usr/local/bin/python2.7
# encoding: utf-8
'''
@author:     Sid Probstein
@contact:    sidprobstein@gmail.com
'''

import sys
import argparse
import glob
import json
import csv
from datetime import datetime

#############################################    

def main(argv):
       
    parser = argparse.ArgumentParser(description='Add real aggregates to KYW JSON files')
    parser.add_argument('inputfilespec', help="path to the json file(s) to merge")
    args = parser.parse_args()

    # initialize
    lstFiles = []
    nFiles = 0

    dictCX = {}
    dictL = {}

    #########       
    # read 2012-present contam file
    
    try:
        fi = open('unreg_2012_present.csv', 'r')
    except Exception, e:
        print "jbind.py: error opening:", e
        sys.exit(e)
    
    try:
        csvReader = csv.reader(fi)   
    except Exception, e:
        print "jbind.py: error reading:", e
        fi.close()

    nRecs = 0
    
    for lstRow in csvReader:
        sPWSID = str(lstRow[0])
        if sPWSID == "PWSID" or sPWSID == "":
            # skip? header?
            continue
        nRecs = nRecs + 1
        if lstRow[17] == "":
            # skip
            continue
        dictC = {}
        dictC['DATE'] = str(lstRow[11])
        dictC['CITY_NAME'] = None
        dictC['CONTAMINANT'] = str(lstRow[13]).encode("utf-8","ignore")
        dictC['LEVEL'] = str(lstRow[17]) #AnalyticalResultValue
        dictC['UNITS'] = "ug/l"
        dictC['SIGN'] = str(lstRow[16]) #AnalyticalResultSign
        if dictCX.has_key(sPWSID):
            # append
            dictCX[sPWSID].append(dictC)
        else:
            # start
            dictCX[sPWSID] = []
            dictCX[sPWSID].append(dictC)
    # end for
        
    fi.close()

    print "jbind.py: loaded", len(dictCX), "records with", nRecs, "total entries!"

    #########       
    # read contaminant decode file
    
    try:
        fi = open("contaminant_decode.csv", 'r')
    except Exception, e:
        print "jbind.py: error opening:", e
        sys.exit(e)
    
    try:
        csvReader = csv.reader(fi)   
    except Exception, e:
        print "jbind.py: error reading:", e
        fi.close()

    for lstRow in csvReader:
        if dictL.has_key(str(lstRow[0]).strip().upper()):
            # print "jbind.py: duplicate found:", lstRow[0], lstRow[1], "old:", dictL[lstRow[0]]
            pass
        else:
            dictL[str(lstRow[0]).strip().upper()] = str(lstRow[1]).strip().upper()
            # print "storing:", str(lstRow[1]).strip().upper()
    # end for
    print "jbind.py: loaded", len(dictL), "entries!"

    fi.close()
    
    #####
    # process files
    
    lstFiles = glob.glob(args.inputfilespec)

    nCHits = 0
    nLHits = 0
            
    # read input file(s)
    for sFile in lstFiles:
        
        # read the input file   
        print "jbind.py: reading:", sFile                      
        try:
            f = open(sFile, 'r')
        except Exception, e:
            print "jbind.py: error opening:", e
            continue
        try:
            jInput = json.load(f)
        except Exception, e:
            print "jbind.py: error reading:", e
            f.close()
            continue
        
        # success reading
        nFiles = nFiles + 1
        f.close()
               
        for jItem in jInput:
            
            # print jItem['PWSID']
            
            # bind all matching PWSID records in
            if dictCX.has_key(jItem['PWSID']):
                for jI in dictCX[jItem['PWSID']]:
                    jItem['CONTAMINANTS'].append(jI)
                nCHits = nCHits + 1
                
            # replace CAS ids with friendly ones
            for jQ in jItem['CONTAMINANTS']:
                # fix case
                jQ['CONTAMINANT'] = jQ['CONTAMINANT'].upper()
                # fix date formats
                if jQ['DATE'].find('-') > -1:
                    if jQ['DATE'].count('-') == 1:
                        date = 0
                    else:
                        date = datetime.strptime(jQ['DATE'],"%d-%b-%y")
                else:
                    if jQ['DATE'].find('/') > -1:
                        date = datetime.strptime(jQ['DATE'],"%m/%d/%Y")
                # if converted, convert to standard date and save
                if date != 0:
                    jQ['DATE'] = date.strftime("%Y-%m-%d")
                # if not, leave the string alone!!
                # replace contaminant names with friendly names
                if dictL.has_key(jQ['CONTAMINANT']):
                    # print "replacing", jQ['CONTAMINANT'], "with", str(dictL[jQ['CONTAMINANT']]).upper(), "for", jItem['PWSID']
                    jQ['CONTAMINANT'] = str(dictL[jQ['CONTAMINANT']]).upper()
                    nLHits = nLHits + 1
            # end for
        # end for
    
        # now write out
        sOutputFile = sFile.replace('.a.json','.b.json')
        try:
            fo = open(sOutputFile, 'wb')
        except Exception, e:   
            print "jbind.py: error opening:", e             
            fo.close()
            sys.exit(e)
        # write the file
        print "jbind.py: writing:", sOutputFile                   
        try:
            json.dump(jInput, fo, sort_keys=True, indent=4, separators=(',', ': '))
        except Exception, e:
            print "jbind.py: error writing:", e
            sys.exit(e)
        fo.close()
        
    # end for
    
    print "jbind.py: bound", nFiles, "files,", nCHits, "contaminant hits,", nLHits, "label hits"
    print "jbind.py: done!"
    
# end main 

#############################################    
    
if __name__ == "__main__":
    main(sys.argv)

# end