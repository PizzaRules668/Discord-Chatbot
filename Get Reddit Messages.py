from dateutil.relativedelta import relativedelta
from datetime import date
from tqdm import tqdm
import requests
import bz2
import os

startDate = date(2013, 1, 1)
currentData = startDate
endDate = date(2013, 2, 1)

while currentData != endDate:
    allData = b""

    response = requests.get(f"https://files.pushshift.io/reddit/comments/RC_{currentData.year}-{currentData.month:02}.bz2", stream=True)
    total_size_in_bytes= int(response.headers.get('content-length', 0))
    block_size = 1024
    progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)

    for data in response.iter_content(block_size):
        progress_bar.update(len(data))
        allData = allData + data
    progress_bar.close()

    try:
        open(f"reddit/{currentData.year}-{currentData.month:02}","wb").write(bz2.decompress(allData))

    except MemoryError:
        open(f"reddit/{currentData.year}-{currentData.month:02}.bz2","wb").writable(allData)
        print("Out or Memory")

    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        print("ERROR, something went wrong")

    currentData += relativedelta(months=+1)

print(f"Downloaded {startDate.year}/{startDate.month}-{endDate.year}/{endDate.month}")