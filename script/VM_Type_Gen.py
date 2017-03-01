#!/usr/bin/env python3


# 在一定的vm數量下，找出不同的vm的組合方式


import random
import math

def main():
	results = []
	cores = [22, 14, 13]
	findAllResult(0, results, [0] * len(cores), cores, int( input('vm total: ') ) )
	
	count = 1
	if len(results) <= 25:
		for i in results:
			#print(count, i)
			print(i)
			count += 1
	else:
		interval = (len(results)-1)/24
		for i in range(25):
			r = round( i*interval )
			#print( r, i, i*interval)
			#print(count, results[r])
			print(results[r])
			i += interval
			count += 1
	
	pass

	
def findAllResult(layer, results, result, cores, totalVMs):
	if layer == len(cores):
		if sum(result) == totalVMs:
			results.append( list( result ) )
		return
	
	for i in range( cores[layer] ):
		result[layer] = i
		if sum(result) == totalVMs:
			results.append( list( result ) )
			result[layer] = 0
			return
		elif sum(result) > totalVMs:
			result[layer] = 0
			return
		else:
			findAllResult(layer+1, results, result, cores, totalVMs)
		result[layer] = 0
			
	
if __name__ == '__main__':
	main()