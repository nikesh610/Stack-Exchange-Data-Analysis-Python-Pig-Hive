import pandas as pd
import re

def preProcData(fileloc,filename):

#Input the CSV file from the location 
    unProcDS = pd.read_csv(fileloc+filename)
   
    print('Started processing of '+filename+" ...")

    #Removing HTML tags in the Body and Title Column
    unProcDS["Body"] = unProcDS.Body.apply(lambda b: re.sub('<.*?>','', b))
    unProcDS["Title"] = unProcDS.Title.apply(lambda t: re.sub('<.*?>','', t))

    #Removing Commas in Body,Text and Tags fields
    unProcDS["Body"] = unProcDS.Body.apply(lambda x: re.sub(',*','', x))
    unProcDS["Title"] = unProcDS.Title.apply(lambda y: re.sub(',*','', y))
    unProcDS["Tags"] = unProcDS.Title.apply(lambda y: re.sub(',*','', y))

    #Removing White Space,Line Breaks,Tab Space in Body and Title Column
    unProcDS["Body"] = unProcDS.Body.apply(lambda x: re.sub('\\n*\\t*\\r*\\s+',' ', x))
    unProcDS["Title"] = unProcDS.Title.apply(lambda y: re.sub('\\n*\\t*\\r*\\s+',' ', y))

    #Writing the processed data to CSV files at the Output folder
    unProcDS.to_csv(fileloc[0:len(fileloc)-12]+"PreProcessed/"+"preProc"+filename)
    print("Sucessfully Processed "+filename+" to Location "+fileloc[0:len(fileloc)-12]+"PreProcessed/"+"preProc"+filename+"\n")
    

def main(): 
    # Location of Extracted Data sets from Stack Exchange
    orgFileloc = "/Users/NikeshPothabattula/Documents/RawDatasets/"
    
    # CDTS is the prefix for extracted datasets  
    orgFilename = "CTDS"

    # As i have extracted 4 CSV files containing 50,000 rows each , I ran the for loop from 1 to 5  to automate 
    # the extration and cleaning of data set by passing the values of dataset location and filename to preProcData function . 
    for i in range(1,5):
     preProcData(orgFileloc,orgFilename+str(i)+".csv")

    
if __name__=="__main__": 
    main() 

