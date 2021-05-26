import pandas as pd
import sqlite3
import shutil

if __name__ == "__main__":
    connection = sqlite3.connect("data.db")
    c = connection.cursor()
    limit = 5000
    last_unix = 0
    cur_lenght = limit
    counter = 0
    test_done = False

    while cur_lenght == limit:
        df = pd.read_sql("SELECT * FROM  parent_reply WHERE unix > {} AND parent NOT NULL AND score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix, limit), connection)
        
        last_unix = df.tail(1)["unix"].values[0]
        cur_lenght = len(df)

        
        # Also tst2013 tst2012

        if not test_done:
            with open("nmt_chatbot/new_data/tst2012.from", "a", encoding="utf8") as f:
                for content in df["parent"].values:
                    f.write(content+"\n")
            with open("nmt_chatbot/new_data/tst2012.to", "a", encoding="utf8") as f:
                for content in df["comment"].values:
                    f.write(content+"\n")

            test_done = True



        else:
            with open("nmt_chatbot/new_data/train.from", "a", encoding="utf8") as f:
                for content in df["parent"].values:
                    f.write(content+"\n")
            with open("nmt_chatbot/new_data/train.to", "a", encoding="utf8") as f:
                for content in df["comment"].values:
                    f.write(content+"\n")

        counter += 1
        if counter % 20 == 0:
            print(counter*limit, " rows completed so far")

shutil.copy("nmt_chatbot/new_data/tst2012.from", "nmt_chatbot/new_data/tst2013.from")
shutil.copy("nmt_chatbot/new_data/tst2012.to", "nmt_chatbot/new_data/tst2013.to")