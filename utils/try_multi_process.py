# -*- coding: utf-8 -*-

# alist = ['d', 'd', 7, 4, 'd', 'd', 2, 1]
# for item in alist:
#      if item != 'd':
#          pass
#      else:
#          alist.remove('d')
#
# print alist


from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    wait, as_completed
)
import time

# executor = ProcessPoolExecutor(max_workers=3)
executor = ThreadPoolExecutor(max_workers=3)



def test(i):
    """
    函数时间长要用多进程或者多线程
    分计算密集型和IO密集型又分别适用于多进程和多线程
    :param i:
    :return:
    """
    print i
    time.sleep(1)
    return i * 2


if __name__ == '__main__':
    start = time.time()
    loop = 8
    results = []

    fs = []
    for i in range(loop):
        future = executor.submit(test, i)
        fs.append(future)
    #
    # for future in fs:
    #     result = future.result()
    #     results.append(result)

    wait(fs)
    for future in as_completed(fs):
        results.append(future.result())

    # for 循环
    # for i in range(loop):
    #     results.append(test(i))

    print time.time() - start
    print 'results总长度：', results.__len__()
