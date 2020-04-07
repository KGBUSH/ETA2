# -*- coding: utf-8 -*-

alist = ['d', 'd', 7, 4, 'd', 'd', 2, 1]
for item in alist:
     if item != 'd':
         pass
     else:
         alist.remove('d')

print alist