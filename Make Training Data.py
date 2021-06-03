import pandas as pd # Dataframe
import sqlite3 # SQL Connection
import shutil # Copy Files

if __name__ == "__main__": # If file not imported
    connection = sqlite3.connect("data.db") # Make Connection to Database
    c = connection.cursor() # Get Cursor
    limit = 5000 # Set import limet
    last_unix = 0 # Setup Last Unix
    cur_lenght = limit # Set cursor max limit to limit
    counter = 0 # Line counter
    test_done = False # Done Getting Test Data

    while cur_lenght == limit:
        df = pd.read_sql("SELECT * FROM  parentReply WHERE unix > {} AND parent NOT NULL AND score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix, limit), connection)
        # Open Database using Pandas read_sql, sorted by unix

        last_unix = df.tail(1)["unix"].values[0] # Set last_unix to the first unix in the dataframe
        cur_lenght = len(df) # Set cursor lenght to the size of the dataframe

        if not test_done: # If not done getting test data
            with open("nmt_chatbot/new_data/tst2012.from", "a", encoding="utf8") as f: # Open Test from file
                for content in df["parent"].values: # For parent message in the dataframe
                    f.write(content+"\n") # Write the parent message

            with open("nmt_chatbot/new_data/tst2012.to", "a", encoding="utf8") as f: # Open Test to file
                for content in df["comment"].values: # For reply in the dataframe
                    f.write(content+"\n") # Write the reply message

            test_done = True # Set the getting test data to true

        else: # If done getting test data
            with open("nmt_chatbot/new_data/train.from", "a", encoding="utf8") as f: # Open Train from file
                for content in df["parent"].values: # For parent message in the dataframe
                    f.write(content+"\n") # Write the parent message

            with open("nmt_chatbot/new_data/train.to", "a", encoding="utf8") as f:
                for content in df["comment"].values: # For reply in the dataframe
                    f.write(content+"\n") # Write the reply message

        counter += 1 # Iterate the line counter 
        if counter % 20 == 0: # If the counter modulo of 20 is 0 
            print(counter*limit, " rows completed so far") # Print counter*limit

shutil.copy("nmt-chatbot/new_data/tst2012.from", "nmt-chatbot/new_data/tst2013.from") # Copy tst2012.from to tst2013.from
shutil.copy("nmt-chatbot/new_data/tst2012.to", "nmt-chatbot/new_data/tst2013.to") # Copy tst2012.to to tst2013.to