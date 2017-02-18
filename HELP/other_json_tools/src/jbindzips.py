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

#############################################    

def main(argv):
       
    parser = argparse.ArgumentParser(description='Add expanded zipcodes to KYW JSON files')
    parser.add_argument('inputfilespec', help="path to the json file(s) to bind, less .json")
    args = parser.parse_args()

    # initialize
    lstFiles = []
    nFiles = 0

    dictZ = {}

    #########       
    # read zip file
    
    try:
        fi = open('pwsid_to_served_zip_codes.csv', 'r')
    except Exception, e:
        print "jbindzips.py: error opening:", e
        sys.exit(e)
    
    try:
        csvReader = csv.reader(fi)   
    except Exception, e:
        print "jbindzips.py: error reading:", e
        fi.close()

    nRecs = 0
    
    for lstRow in csvReader:
        sPWSID = str(lstRow[0])
        if sPWSID == "PWSID" or sPWSID == "":
            # skip? header?
            continue
        nRecs = nRecs + 1
        if lstRow[1] == "":
            # skip
            continue
        if dictZ.has_key(sPWSID):
            dictZ[sPWSID].append(lstRow[1])
        else:
            dictZ[sPWSID] = []
            dictZ[sPWSID].append(lstRow[1])        
    # end for
        
    fi.close()

    print "jbindzips.py: loaded", len(dictZ), "records with", nRecs, "total entries!"
    
    #####
    # process files
    
    lstFiles = glob.glob(args.inputfilespec)

    nCHits = 0
            
    # read input file(s)
    for sFile in lstFiles:
        
        # read the input file   
        print "jbindzips.py: reading:", sFile                      
        try:
            f = open(sFile, 'r')
        except Exception, e:
            print "jbindzips.py: error opening:", e
            continue
        try:
            jInput = json.load(f)
        except Exception, e:
            print "jbindzips.py: error reading:", e
            f.close()
            continue
        
        # success reading
        nFiles = nFiles + 1
        f.close()
               
        for jItem in jInput:
            
            # bind matching zip codes in
            if dictZ.has_key(jItem['PWSID']):
                jItem['ZIP_CODES'] = []
                jItem['ZIP_CODES'] = dictZ[jItem['PWSID']]
                nCHits = nCHits + 1
                
        # end for
    
        # now write out
        sOutputFile = sFile.replace('.b.json','.c.json')
        try:
            fo = open(sOutputFile, 'wb')
        except Exception, e:   
            print "jbindzips.py: error opening:", e             
            fo.close()
            sys.exit(e)
        # write the file
        print "jbindzips.py: writing:", sOutputFile                   
        try:
            json.dump(jInput, fo, sort_keys=True, indent=4, separators=(',', ': '))
        except Exception, e:
            print "jbindzips.py: error writing:", e
            sys.exit(e)
        fo.close()
        
    # end for
    
    print "jbindzips.py: bound", nFiles, "files,", nCHits, "zipcodes"
    print "jbindzips.py: done!"
    
# end main 

#############################################    
    
if __name__ == "__main__":
    main(sys.argv)

# end