###############################################################
# SCRIPT TO FIND THE AVERAGE TIME BETWEEN NODE FAILURE IN A
# GIVEN FAILURE TRACE 
###############################################################

import sys

###############################################################

TRACE_FILE = sys.argv [1]

###############################################################

def readNextTraceLine (traceHandle) :
  """ Function to read the next line from the trace. """
  traceLine = []
  nextTraceLine = traceHandle.readline ()

  if not nextTraceLine :
    return traceLine

  nextTraceLine = nextTraceLine.strip ()
  traceLine = [int (d) for d in nextTraceLine.split ("\t")]
  return traceLine

###############################################################

if __name__ == "__main__":

  total = 0
  n = 0

  traceHandle = open (TRACE_FILE, 'r')

  traceLine = readNextTraceLine (traceHandle)
  while traceLine and traceLine [0] == 1 :
    traceLine = readNextTraceLine (traceHandle)
    
  if traceLine :

    time1 = traceLine [2]

    traceLine = readNextTraceLine (traceHandle)
    while traceLine and traceLine [0] == 1 :
      traceLine = readNextTraceLine (traceHandle)

  while traceLine :

    time2 = traceLine [2]

    total += time2 - time1 
    n += 1

    time1 = time2

    traceLine = readNextTraceLine (traceHandle)
    while traceLine and traceLine [0] == 1 :
      traceLine = readNextTraceLine (traceHandle)

  print float (total)/(n * 3600)

##############################################################
