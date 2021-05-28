from dateutil.relativedelta import relativedelta
from datetime import date
from datetime import datetime
from tqdm import tqdm
import traceback
import zstandard
import requests
import sqlite3
import json
import lzma
import bz2
import os

PROGRESSBAR = False

# Time Frame You Want To Download
startDate = date(2005, 12, 1) # 2005 12 is the first available data
currentDate = startDate # Set The Date the is currently being downloaded to be the first date
endDate = date(2006, 1, 1) # End of the time frame you want to download from

sqlConnection = sqlite3.connect("data.db")
cursor = sqlConnection.cursor()
sqlRequests = []

def downloadData(timeframe):
    try:
        compressedData = b"" # Final Download Data, Compressed as BZ2, XZ, ZST
        decompressedData = ""
        fileType = ""

        if timeframe < date(2017, 11, 2):
            fileType = "bz2"

        elif date(2017, 12, 1) <= timeframe <= date(2018, 10, 2):
            fileType = "xz"

        elif timeframe > date(2018, 11, 1):
            fileType = "zst"

        if PROGRESSBAR:
            dataRequest = requests.get(f"https://files.pushshift.io/reddit/comments/RC_{timeframe.year}-{timeframe.month:02}.{fileType}", stream=True)
            # Download Data Stream 
            # Only good till 2017/11
            totalDownloadSize = int(dataRequest.headers.get("Content-Length", 0))
            # Total Download Data Size
            
            progressBar = tqdm(total=totalDownloadSize, unit="B", unit_scale=True, desc=f"File Download: {timeframe.year}-{timeframe.month:02}.{fileType}")
            dataBlockSize = 1024 # How often the progress bar updates

            for data in dataRequest.iter_content(dataBlockSize):
                progressBar.update(len(data)) # Update Progress Bar by size of data
                compressedData += data # Add Current Data to the currentData var
            progressBar.close() # Stop Progress Bar

        else:
            print(f"Downloading: {timeframe.year}-{timeframe.month:02}")
            dataRequest = requests.get(f"https://files.pushshift.io/reddit/comments/RC_{timeframe.year}-{timeframe.month:02}.{fileType}")
            compressedData = dataRequest.content
            # Download Data

        if fileType == "bz2":
            return bz2.decompress(compressedData)

        elif fileType == "xz":
            return lzma.decompress(compressedData)

        elif fileType == "zst":
            return zstandard.decompress(compressedData)

    except MemoryError: # If you run out of memeory/ram
        del decompressedData # Delete the current Decompressed Data
        print("Out of Memory")
        open(f"reddit/{currentDate.year}-{currentDate.month:02}.{fileType}", "wb").write(compressedData) # Open file your going to right in 

        if fileType == "bz2":
            file = bz2.open(f"reddit/{currentDate.year}-{currentDate.month:02}.{fileType}", "rb") # Reopen it as BZ2 object
            return file.read()

        elif fileType == "xz":        
            file = lzma.open(f"reddit/{currentDate.year}-{currentDate.month:02}.{fileType}", "rb") # Reopen it as XZ object
            return file.read()

        elif fileType == "zst":
            file = zstandard.open(f"reddit/{currentDate.year}-{currentDate.month:02}.{fileType}", "rb") # Reopen it as ZST object
            return file.read()


def SQLTransaction(request):
    global sqlRequests
    sqlRequests.append(request) # Add request to list of requests
    if len(sqlRequests) > 1000: # If more than 1000 requests
        cursor.execute("BEGIN TRANSACTION") 
        for request in sqlRequests: # For each request run request
            try:
                cursor.execute(request) 
            except:
                pass

        sqlConnection.commit() # Commit All Changes
        sqlRequests = [] # Reset list of request

def replaceComment(commentID, parentID, parent, comment, subreddit, time, score):
    try:
        request = f"UPDATE parentReplay SET parentID = {parentID}, commentID = {commentID}, parent = {parent}, subreddit = {subreddit}, unix = {int(time)}, score = {score} WHERE parentID = {parentID};"
        # Make Request To Database to update existing data
        SQLTransaction(request)
        # Run Request
    
    except Exception as e:
        print(f"Error in Replace Comment: {e}")

def replyToParent(commentID, parentID, parent, comment, subreddit, time, score):
    try:
        request = f'INSERT INTO parentReply (parentID, commentID, parent, comment, subreddit, unix, score) VALUES ("{parentID}", "{commentID}", "{parent}", "{comment}", "{subreddit}", "{time}", "{score}");'
        # Make Request TO Insert TO Database
        SQLTransaction(request)
        # Run Request
    
    except Exception as e:
        print(f"Error in Reply To Parent: {e}")

def noParent(commentID, parentID, comment, subreddit, time, score):
    try:
        request = f'INSERT INTO parentReply (parentID, commentID, comment, subreddit, unix, score) VALUES ("{parentID}","{commentID}", "{comment}", "{subreddit}", "{time}", "{score}");'
        # Make Request TO Insert TO Database
        SQLTransaction(request)
        # Run Request
    
    except Exception as e:
        print(f"Error in Add No Parent: {e}")

def formatData(data):
    return data.replace("\n", "newlinechar").replace("\r", "newlinechar").replace("'", '"')
    # Replace All Newline Character with 'newlinechar', also Replace all ' with " 

def getParentContent(parentID):
    try:
        request = f"SELECT comment FROM parentReply WHERE commentID = '{parentID}' LIMIT 1"
        cursor.execute(request) # Execute Request
        results = cursor.fetchone() # Get Results
        if results != None:
            return results[0] # Return Results

        else:
            return False # Could Not Find Parent
    
    except Exception as e:
        print(f"Error Finding Parent Comment: {e}")

def getParentReplyScore(parentID):
    try:
        request = f"SELECT score FROM parentReply WHERE commentID = '{parentID}' LIMIT 1"
        cursor.execute(request) # Execute Request
        results = cursor.fetchone() # Get Results

        if results != None:
            return results[0] # Return Results

        else:
            return False # Could Not Find Parent
    
    except Exception as e:
        print(f"Error Finding Parent Reply Score: {e}")
    
def acceptable(data):
    if len(data.split(' ')) > 1000 or len(data) < 1:
        # If Less then 1000 words but more than 1 character
        return False

    elif len(data) > 32000:
        # If data is bigger then 32000 characters
        return False

    elif data == "[deleted]":
        # Check to see if the message was deleted by the author
        return False

    elif data == "[removed]":
        # Check to see if the message was deleted by a moderator
        return False

    else:
        # Data is acceptable
        return True

rowCounter = 0
pairedRowCounter = 0
clearUp = 1000000

def processData(line):
    global pairedRowCounter, rowCounter
    try:
        # For Line in the downloaded Data
        data = json.loads(line) # Turn string to json object
        parentID = data["parent_id"].split("_")[1] # Get Parent Message ID
        content = formatData(data["body"]) # Get Message Content after being formated
        createdUTC = data["created_utc"] # Get Created UTC
        score = data["score"] # Get Score
        commentID = data["id"] # Get Message ID
        subreddit = data["subreddit"] # Subreddit it was posted in
        parentContent = getParentContent(parentID) # Get Parent Content
        # Check to see is parent is already replyed to and get reply score
        parentReplyScore = getParentReplyScore(parentID)
        if parentReplyScore: # If has reply
            if score > int(parentReplyScore): # If Score is greater than existing parent reply
                if acceptable(content): # Check to see if data is acceptable
                    replaceComment(commentID, parentID, parentContent, content, subreddit, createdUTC, score)
        else:
            if parentContent:
                if score >= 2:
                    replyToParent(commentID, parentID, parentContent, content, subreddit, createdUTC, score)
                    pairedRowCounter += 1 # Increment Paired Row Counter 
            else:
                noParent(commentID, parentID, content, subreddit, createdUTC, score)
    except:
        print(traceback.format_exc())
    if rowCounter > startRow:
        if rowCounter % clearUp == 0:
            request = "DELETE FROM parentReply WHERE parent IS NULL" # Remove All Entries if parent is NULL
            cursor.execute(request) # Run request
            sqlConnection.commit() # Commit all changes
            cursor.execute("VACUUM")
            sqlConnection.commit()

while currentDate != endDate:
    # Time to Process All of the Data
    cursor.execute("CREATE TABLE IF NOT EXISTS parentReply(parentID TEXT PRIMARY KEY, commentID TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")
    # Create Table if it does not exist
    startRow = 0

    print("Done Downloading, Starting To Decompress")
    decompressedData = downloadData(currentDate)
    for line in tqdm(decompressedData.splitlines(), desc=f"Lines in {currentDate.year}-{currentDate.month:02}"):
        rowCounter += 1 # Increment Row Counter 
        if rowCounter > startRow:
            processData(line)

    del decompressedData

    currentDate += relativedelta(months=+1) # Add One Month To Current Date
    print(f"Total Rows Read: {rowCounter}\nPaired Rows: {pairedRowCounter}") # Print Total Line Read, And Total Lines Paired

print(f"Total Rows Read: {rowCounter}\nPaired Rows: {pairedRowCounter}") # Print Total Line Read, And Total Lines Paired