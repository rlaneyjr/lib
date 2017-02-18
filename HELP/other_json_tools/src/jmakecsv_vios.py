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

def retstring (x):
    
    if x == None:
        return None
    
    if isinstance(x, str):
        return x
    
    sFoo = ""
    if isinstance(x, unicode):
        for c in x:
            if ord(c) < 128:
                sFoo = sFoo + c
        return sFoo
    
    return x          
    
#############################################    

def main(argv):
       
    parser = argparse.ArgumentParser(description='Write KYW json files into csv files for loading into a DB')
    parser.add_argument('-d', '--debug', action='store_true', help="provide debug information")
    parser.add_argument('inputfilespec', help="path to the json file(s) to write")
    args = parser.parse_args()

    # initialize
    lstFiles = []
    nFiles = 0
    
    ###########################################################################   
    # create the CSVs
    
    ##########
    # violations.csv
    try:
        fo = open('violations.csv', 'wb')
    except Exception, e:
        print "jmakecsv.py: error:", e
        sys.exit()
        
    csv1 = csv.writer(fo, quoting=csv.QUOTE_ALL)
    csv1.writerow( [ 'PWSID', 'VIOLATION_ID', 'VIOLATION_CODE', 'CONTAMINANT_CODE', 
                     'CORRECTIVE_ACTION_ID', 'COMPL_PER_BEGIN_DATE', 'COMPL_PER_END_DATE', 
                     'COMPLIANCE_STATUS_CODE', 'SEVERITY_IND_CNT', 'PUBLIC_NOTIFICATION_TIER', 
                     'IS_MAJOR_VIOL_IND', 'IS_HEALTH_BASED_IND', 'RULE_GROUP_CODE', 
                     'RULE_FAMILY_CODE', 'RULE_CODE', 'RTC_ENFORCEMENT_ID', 'RTC_DATE', 
                     'ORIGINATOR_CODE', 'VIOL_MEASURE', 'UNIT_OF_MEASURE', 'STATE_MCL', 
                     'FACILITY_ID'  ]
                )
    fo.close()
    
    ###########################################################################
    # process files
    
    lstFiles = glob.glob(args.inputfilespec)
            
    # read input file(s)
    for sFile in lstFiles:
        
        # read the input file   
        print "jmakecsv.py: reading:", sFile                      
        try:
            fi = open(sFile, 'r')
        except Exception, e:
            print "jmakecsv.py: error opening:", e
            continue
        try:
            jInput = json.load(fi)
        except Exception, e:
            print "jmakecsv.py: error reading:", e
            fi.close()
            continue
        
        # success reading
        nFiles = nFiles + 1
        fi.close()
                       
        for jItem in jInput:

            ###########################################################################  
            # write out the csvs
                               
            if args.debug:
                print jItem['PWSID']
                    
            # water_system.csv
            try:
                fo = open('violations.csv', 'ab')
            except Exception, e:
                print "jmakecsv.py: error:", e
                sys.exit()
            
            csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
            
            csv1.writerow( [ jItem['PWSID'], jItem['VIOLATION_ID'], jItem['VIOLATION_CODE'], jItem['CONTAMINANT_CODE'], 
                             jItem['CORRECTIVE_ACTION_ID'], jItem['COMPL_PER_BEGIN_DATE'], jItem['COMPL_PER_END_DATE'], 
                             jItem['COMPLIANCE_STATUS_CODE'], jItem['SEVERITY_IND_CNT'], jItem['PUBLIC_NOTIFICATION_TIER'], 
                             jItem['IS_MAJOR_VIOL_IND'], jItem['IS_HEALTH_BASED_IND'], jItem['RULE_GROUP_CODE'], 
                             jItem['RULE_FAMILY_CODE'], jItem['RULE_CODE'], str(jItem['RTC_ENFORCEMENT_ID']), jItem['RTC_DATE'], 
                             jItem['ORIGINATOR_CODE'], jItem['VIOL_MEASURE'], jItem['UNIT_OF_MEASURE'], jItem['STATE_MCL'], 
                             jItem['FACILITY_ID']
                             ]
                        )
            
            fo.close()
            
        # end for
    
    # end for
    
    print "jmakecsv.py: wrote", nFiles, "into CSV format"
    print "jmakecsv.py: done!"
    
# end main 

#############################################    
    
if __name__ == "__main__":
    main(sys.argv)

# end