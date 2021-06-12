
# Imports 

import sys

import multiprocessing as mp
import re, string
from collections import Counter
import os
from itertools import groupby

import time

nr_threads = 10

# Command line arg
text_filepath =  sys.argv[1] if len(sys.argv) > 1 else "input/sample20.txt"
print("Text file to check: " + text_filepath)


# Functions

def cleanse_text( text ):
    """
    Clean text from punctuation and normalize words to lower
    
    Params: 
        text (str): a string of text
    
    Returns:   
        clean_text (List[str]): list of all words in the text sanitized 
    """
    lower_text = text.lower().split(' ')
    clean_text = [re.sub('[\W_]+', '', word) for word in lower_text ]
    return clean_text
    
    
def chunk_it( filepath, nr_chunks ) :
    """
    Divide text file into chunks by line to distribute to processing nodes.
    
    Params: 
        filepath (str): absolute path of .txt file
        nr_chunks (int): number of partitions to divide text into 
    
    Returns:
        line_chunks (List[List[str]]) : A nested list of strings containing the lines for each node to process.
    """
    
    file = open(filepath, "r")
    sample_text = file.readlines()
    
    import math
    line_length = math.ceil(len(sample_text) / nr_chunks)

    line_chunks = []
    
    for i in range(nr_chunks):
        line_chunks.append(sample_text[i*line_length:(i+1)*line_length])
    
    print( "We have {} chunks to process".format( len(line_chunks) ) )
    
    return line_chunks


def count_occurrences( lines ):
    """
    Clean text and count each word in the text, iterating per line provided.
    
    Params: 
        lines (List[str]): list containing the lines within the text
        
    Returns:
        count (str, int): count of occurrences per word within the chunk of text 
    """
    
    print( "Process running: " + str(os.getpid()) + "\n" ) # print process id

    cleansed = []
    for line in lines:
        clean_line = cleanse_text(line)
        for word in clean_line:
            cleansed.append(word)
        
    count = Counter(cleansed).most_common()
    return count

def write_file ( destination_filepath , wordcount_list ):
	with open( destination_filepath, 'w+') as f:
		for word, value in wordcount_list: 
	    		f.write("{} , {}\n".format(word, value))


def main():
    """
    Main multiprocessing function to divide text according number of partitions, 
    map to each count process and 
    reduce by summing occurrences per word.
    """
    lines_array = chunk_it( text_filepath, nr_threads)
    results = pool.map(count_occurrences, lines_array)
        
    words = [ entry for line in results for entry in line]

    sorted_words = groupby(sorted(words), key = lambda x: x[0])
    
    word_aggregate = [ [key, sum(value for _, value in group)] for key, group in sorted_words ]
    
    sorted_counts = sorted(word_aggregate, key = lambda x: x[1], reverse=True)
    
    # save results
    output_filepath = text_filepath.replace("input/" , "output/")
    write_file( output_filepath , sorted_counts )

if __name__ == '__main__':

	start_time = time.time()
	
	pool = mp.Pool(processes=nr_threads)
	main()

	print("Process took %s seconds" % (time.time() - start_time))