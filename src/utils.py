import time

'''
File: utils.py
Created: September 17, 2015
Authors: Paul Kowalski <paulkowa@buffalo.edu>
         Dhanasekar Karuppasamy <dhanasek@buffalo.edu>
Copyright (c) 2015-2016 Paul Kowalski, Dhanasekar Karuppasamy

Distributed under the MIT License.
See accompanying file LICENSE_MIT.txt.
This file is part of BiGPy.
'''

def timeit(method):
    '''
    Time it Decorator.
    TODO :
         * Have an option to emit time data to a external file.
         * Data pertaining to the size of the file must be logged
         * Make timing optional.
    '''
    def timed(*args, **kw):
        '''
        Mesure time for a function call.
        '''
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r (%r, %r) %2.2f sec' % \
              (method.__name__, args, kw, te-ts)
        return result
    return timed
