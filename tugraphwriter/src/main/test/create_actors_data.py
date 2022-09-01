from multiprocessing import Pool
import csv
import random
import string
import datetime
import os

def create_data(index):
    TEXT = string.ascii_letters + string.digits
    f = open("data/actors_data_" + str(index) + '.csv', 'w', newline='')
    writer = csv.writer(f)
    for i in range(100000000):
        #line = [random.randint(0,100000000),
        #        ''.join(random.sample(TEXT, 20)),
        #        (datetime.datetime.now() + datetime.timedelta(minutes=random.randint(-10000,10000))).strftime("%Y-%m-%d %H:%M:%S"),
        #        ''.join(random.sample(TEXT, 20))]
        line = [str(index) + ''.join(random.sample(TEXT, 10)), ''.join(random.sample(TEXT, 20))]
        writer.writerow(line)
    f.close()

if __name__ == '__main__':
    with Pool(processes=10) as pool:
        os.system('mkdir data')
        for i in range(10):
            result = pool.apply_async(create_data, (i,))
        pool.close()
        pool.join()