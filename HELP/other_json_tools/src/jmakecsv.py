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
    # water_system.csv
    try:
        fo = open('water_system.csv', 'wb')
    except Exception, e:
        print "jmakecsv.py: error:", e
        sys.exit()
        
    csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
    csv1.writerow( [ 'PWSID', 'PWS_NAME', 'STATE_CODE', 'CITY_NAME', 'COUNTY_NAME', 'ZIP_CODE', 'ORG_NAME',' ORG_ADDRESS_LINE1',
                   'ORG_ADDRESS_LINE2', 'ORG_PHONE_NUMBER', 'ORG_URI', 'ORG_CCR_REPORT_URI', 'IS_SCHOOL', 
                   'OUTSTANDING_PERFORMER', 'POPULATION_SERVED', 'SOURCE_PURCHASED_PWSID', 'WATER_SOURCE_TYPE' ]
                )
    fo.close()

    ##########
    # aggregates.csv
    try:
        fo = open('aggregates.csv', 'wb')
    except Exception, e:
        print "jmakecsv.py: error:", e
        sys.exit()
        
    csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
    csv1.writerow( [ 'PWSID', 'AGGREGATE', 'CONTAMINANT', 'DATE', 'LEVEL', 'UNITS' ]
                )
    fo.close()

    ##########
    # city_names.csv
    try:
        fo = open('city_names.csv', 'wb')
    except Exception, e:
        print "jmakecsv.py: error:", e
        sys.exit()
        
    csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
    csv1.writerow( [ 'PWSID', 'CITY_SERVED' ]
                )
    fo.close()

    ##########
    # contaminants.csv
    try:
        fo = open('contaminants.csv', 'wb')
    except Exception, e:
        print "jmakecsv.py: error:", e
        sys.exit()
        
    csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
    csv1.writerow( [ 'PWSID', 'CONTAMINANT', 'DATE', 'LEVEL', 'SIGN', 'UNITS', 'CITY' ]
                )
    fo.close()

    ##########
    # source_reservoir.csv
    try:
        fo = open('source_reservoir.csv', 'wb')
    except Exception, e:
        print "jmakecsv.py: error:", e
        sys.exit()
        
    csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
    csv1.writerow( [ 'PWSID', 'FACILITY_ID', 'FACILITY_NAME' ]
                )
    fo.close()

    ##########
    # source_treatment.csv
    try:
        fo = open('source_treatment.csv', 'wb')
    except Exception, e:
        print "jmakecsv.py: error:", e
        sys.exit()
        
    csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
    csv1.writerow( [ 'PWSID', 'FACILITY_ID', 'FACILITY_NAME' ]
                )
    fo.close()
    ##########
    # treatments.csv
    try:
        fo = open('treatments.csv', 'wb')
    except Exception, e:
        print "jmakecsv.py: error:", e
        sys.exit()
        
    csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
    csv1.writerow( [ 'PWSID', 'COMMENTS', 'OBJECTIVE', 'OBJECTIVE_EXPLAIN', 'TREATMENT', 'TREATMENT_EXPLAIN', 'FACILITY_ID' ]
                )
    fo.close()

    ##########
    # zipcodes.csv
    try:
        fo = open('zip_codes.csv', 'wb')
    except Exception, e:
        print "jmakecsv.py: error:", e
        sys.exit()
        
    csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
    csv1.writerow( [ 'PWSID', 'ZIP_CODE' ]
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
                fo = open('water_system.csv', 'ab')
            except Exception, e:
                print "jmakecsv.py: error:", e
                sys.exit()
            
            csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
            
            csv1.writerow( [ jItem['PWSID'], retstring(jItem['PWS_NAME']), jItem['STATE_CODE'], retstring(jItem['CITY_NAME']), 
                            retstring(jItem['COUNTY_NAME']), 
                            jItem['ZIP_CODE'], retstring(jItem['ORG_NAME']), retstring(jItem['ORG_ADDRESS_LINE1']), 
                            retstring(jItem['ORG_ADDRESS_LINE2']), 
                            jItem['ORG_PHONE_NUMBER'], jItem['ORG_URI'], jItem['ORG_CCR_REPORT_URI'], jItem['IS_SCHOOL'], 
                            jItem['OUTSTANDING_PERFORMER'], jItem['POPULATION_SERVED'], jItem['SOURCE_PURCHASED_PWSID'], 
                            jItem['WATER_SOURCE_TYPE'] ]
                        )
            
            fo.close()

            # aggregates.csv
            try:
                fo = open('aggregates.csv', 'ab')
            except Exception, e:
                print "jmakecsv.py: error:", e
                sys.exit()
                
            csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
            
            for jR in jItem['AGGREGATES']:
                csv1.writerow( [ jItem['PWSID'], jR['AGGREGATE'], jR['CONTAMINANT'], jR['DATE'], jR['LEVEL'], jR['UNITS'] ]
                            )
            fo.close()

            # city_names.csv
            try:
                fo = open('city_names.csv', 'ab')
            except Exception, e:
                print "jmakecsv.py: error:", e
                sys.exit()
                
            csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
            for jR in jItem['CITY_NAMES']:
                csv1.writerow( [ jItem['PWSID'], retstring(jR) ] )
            fo.close()

            # contaminants.csv
            try:
                fo = open('contaminants.csv', 'ab')
            except Exception, e:
                print "jmakecsv.py: error:", e
                sys.exit()
                
            csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
            for jR in jItem['CONTAMINANTS']:
                if jR.has_key('CITY_NAME'):
                    if jR.has_key('LEVEL'):
                        csv1.writerow( [ jItem['PWSID'], jR['CONTAMINANT'], jR['DATE'], jR['LEVEL'], jR['SIGN'], jR['UNITS'], jR['CITY_NAME'] ]
                                )
                    else:
                        csv1.writerow( [ jItem['PWSID'], jR['CONTAMINANT'], jR['DATE'], "", jR['SIGN'], jR['UNITS'], jR['CITY_NAME'] ]
                                )
                else:
                    if jR.has_key('LEVEL'):
                        csv1.writerow( [ jItem['PWSID'], jR['CONTAMINANT'], jR['DATE'], jR['LEVEL'], jR['SIGN'], jR['UNITS'], "" ]
                                )
                    else:
                        csv1.writerow( [ jItem['PWSID'], jR['CONTAMINANT'], jR['DATE'], "", jR['SIGN'], jR['UNITS'], "" ]
                                )
            fo.close()

            # source_reservoir.csv
            try:
                fo = open('source_reservoir.csv', 'ab')
            except Exception, e:
                print "jmakecsv.py: error:", e
                sys.exit()
                
            csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
            for jR in jItem['SOURCE_RESERVOIR_INFO']:
                csv1.writerow( [ jItem['PWSID'], jR['FACILITY_ID'], retstring(jR['FACILITY_NAME']) ]
                            )
            fo.close()

            # source_treatment.csv
            try:
                fo = open('source_treatment.csv', 'ab')
            except Exception, e:
                print "jmakecsv.py: error:", e
                sys.exit()
                
            csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
            for jR in jItem['SOURCE_TREATMENTPLANT_INFO']:
                csv1.writerow( [ jItem['PWSID'], jR['FACILITY_ID'], retstring(jR['FACILITY_NAME']) ]
                            )
            fo.close()

            # treatments.csv
            try:
                fo = open('treatments.csv', 'ab')
            except Exception, e:
                print "jmakecsv.py: error:", e
                sys.exit()
                
            csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
            for jR in jItem['TREATMENTS']:
                if jR.has_key('TREATMENT_EXPLAIN'):
                    csv1.writerow( [ jItem['PWSID'], retstring(jR['COMMENTS']), retstring(jR['OBJECTIVE']), retstring(jR['OBJECTIVE_EXPLAIN']), 
                                    retstring(jR['TREATMENT']), retstring(jR['TREATMENT_EXPLAIN']), jR['FACILITY_ID'] ]
                                )
                else:
                    csv1.writerow( [ jItem['PWSID'], retstring(jR['COMMENTS']), retstring(jR['OBJECTIVE']), 
                                    retstring(jR['OBJECTIVE_EXPLAIN']), retstring(jR['TREATMENT']), 
                                    "", jR['FACILITY_ID'] ]
                                )
                    
            fo.close()

            # zip_codes.csv
            try:
                fo = open('zip_codes.csv', 'ab')
            except Exception, e:
                print "jmakecsv.py: error:", e
                sys.exit()
                
            csv1 = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC)
            if jItem.has_key('ZIP_CODES'):
                for jR in jItem['ZIP_CODES']:
                    csv1.writerow( [ jItem['PWSID'], jR ] )
            else:
                csv1.writerow( [ jItem['PWSID'], jItem['ZIP_CODE'] ])
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