import os

def main():
    outputFileName = "C:\\Users\\Gowtham\\Downloads\\Energy Efficient Computing\\SmartScaling\\SmartScaling\\Readings\\" + "write_response_times.txt"
    f2 = open(outputFileName,"w")
    f2.seek(0)
    
    for file in os.listdir("C:\\Users\\Gowtham\\Downloads\\Energy Efficient Computing\\SmartScaling\\SmartScaling\\Readings"):
        
        if file.find("I_") > -1:
            print "Ignoring file: ", file
            continue
        
        name = "C:\\Users\\Gowtham\\Downloads\\Energy Efficient Computing\\SmartScaling\\SmartScaling\\Readings\\" + file
        f1 = open(name, "r")
        
        responseTime = 0.0
        requests = 0
        requests_per_sec = 0
        reqDuration = 0.0
        print name
        
        numReads = 0
        numWrites = 0
        
        while 1:
            line = f1.readline()
            #print line
            
            if not line:
                break            
            
            if line.find("Response Time:") > -1:
                
                if line.count("ThreadID") > 1:
                    #print "threadid invalid", responseTime
                    continue
                
                requests += 1
                
                if line.count("Response Time:") > 1:
                    #print "responsetime invalid", responseTime
                    continue
                
                #if line.find("invalid") > -1:
                    #print "invalid entry", responseTime
                #    continue
                
                if line.find("Reads") > -1:
                    numReads += 1
                    
                if line.find("Writes") > -1:
                    numWrites += 1
                
                tokens = line.split("Response Time: ")
                
                
                tokens[1] = tokens[1].replace(' ', '')
                tokens[1] = tokens[1].replace('\n', '')               
                
                #print tokens[1]
                try:
                    responseTime += float(tokens[1])
                    #requests += 1
                    #if float(tokens[1]) > 0.3:
                        #print "val:", float(tokens[1]), responseTime
                        #print line
                    #print tokens[1], responseTime
                except ValueError:
                    print "invalid line"
                    
                #print responseTime
                
            
            if line.find("Total threads duration:") > -1:
                reqTokens = line.split("Total threads duration:")
                #requests = reqTokens[0].replace("Num Requests: ", '')
                reqDuration = reqTokens[1]
                reqDuration = reqDuration.replace(' ', '')
                reqDuration = reqDuration.replace('\n', '')
                
            '''if line.find("Total Requests:") > -1:
                reqTokens = line.split("Total Requests:")
                #requests = reqTokens[0].replace("Num Requests: ", '')
                requests = reqTokens[1]
                requests = requests.replace(' ', '')
                requests = requests.replace('\n', '')
                
            if line.find("NumRequests Per Second:") > -1:
                reqTokens = line.split("NumRequests Per Second:")
                #requests = reqTokens[0].replace("Num Requests: ", '')
                requests_per_sec = reqTokens[1]
                requests_per_sec = requests_per_sec.replace(' ', '')
                requests_per_sec = requests_per_sec.replace('\n', '')
                #print requests'''
               
        if requests != 0:
            avgResponse = responseTime/float(requests)
            #print requests, requests_per_sec, avgResponse, responseTime   
            reqDuration = float(reqDuration) 
            requests_per_sec = float(requests)/float(reqDuration)
            print "Reads: ", numReads, "; Writes: ", numWrites       
            f2.write(str(requests_per_sec) + "," + str(avgResponse) + "\n")
            
            '''if line.find("Time Taken:") > -1:
                reqTokens = line.split("Time")
                requests = reqTokens[0].replace("Num Requests: ", '')
                requests = requests.replace(' ', '')
                requests = requests.replace('\n', '')
                
                tokens = line.split("Taken: ")
                
                tokens[1] = tokens[1].replace(' ', '')
                #tokens[1] = tokens[1].replace('\n', '') 
                               
                f2.write(requests + "," + tokens[1])'''
        
    f1.close()
    
    f2.close()
    print "Finished Parsing all files"
    
if __name__ == "__main__":
    main()