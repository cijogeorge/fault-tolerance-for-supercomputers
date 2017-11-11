
##############################################################################################################################
# MODULE FOR SIMULATION OF FAILURE PREDICTION                                                                                #
##############################################################################################################################

import random
import sys

##############################################################################################################################

TRACE_FILE = "wei100000" 
PREDICTOR_TRACE = sys.argv [2]
NUMBER_OF_NODES = 101000
PREDICTOR_PRECISION = float (sys.argv [3])
PREDICTOR_RECALL = float (sys.argv [4])
TRACE_TYPE = int (sys.argv [1]) # 0: failure |  1: recovery
START_TIME = 3600 * 24 * 30 * 6 
END_TIME = START_TIME + (3600 * 24 * 7)

##############################################################################################################################

# Defines a Predictor that predicts events at regular intervals #
class predictorClass :

  def __init__ (self, fTrace, pTrace, traceType, precision, recall, totalNodes, startTime, endTime):
    """ Init function for class predictorClass. 
        Returns None. """

    self.traceType = traceType
    self.precision = precision
    self.recall = recall
    self.totalNodes = totalNodes
    self.startTime = startTime
    self.endTime = endTime
    self.fTraceHandle = open (fTrace, "r")
    self.pTraceHandle = open (pTrace, "w")

    self.readNextTraceLine (self.fTraceHandle)

    self.totalEvents = 0
    self.predictedEvents = 0
    self.correctPredictions = 0
    self.totalPredictions = 0

  ############################################################################################################################

  def readNextTraceLine (self, traceHandle):
    """ Function to read the next line from the trace. 
        Updates the traceData list. """

    self.traceData = []

    self.nextTraceLine = traceHandle.readline ()
      
    if not self.nextTraceLine :
      self.traceData = None
      return

    self.nextTraceLine = self.nextTraceLine.strip ()
    self.traceData = [int (d) for d in self.nextTraceLine.split ("\t")]

  ############################################################################################################################

  def predictFailures (self) :
    """ Function that predicts the events for the next interval.
        Updates the list of nodes expected to fail/recover in the interval. """
    
    while self.traceData :

        if self.traceData [0] == self.traceType :

          self.totalEvents += 1

          if random.random () < self.recall :

            self.predictedEvents += 1
            self.pTraceHandle.write ("%d\t%d\t%d\n" % (self.traceData [0], self.traceData [1], self.traceData [2]))
            self.correctPredictions += 1
            self.totalPredictions += 1

        self.readNextTraceLine (self.fTraceHandle)

    if self.precision < 1 :

      wrongPredictions = int ((float (self.correctPredictions * (1  - self.precision)) / self.precision) + 0.5)
    
      interval = int ((self.endTime - self.startTime) / wrongPredictions)
      start = self.startTime
      end = start + interval

      for i in range (wrongPredictions) :
     
        self.pTraceHandle.write ("%d\t%d\t%d\n" % (0, random.randint (0, self.totalNodes - 1), \
                                                      random.randint (start, end - 1)))
        self.totalPredictions += 1
        start = end
        end = start + interval

  ############################################################################################################################
  
  if __name__ == "__main__" :

    import pTraceGen

    predictor = pTraceGen.predictorClass (TRACE_FILE, PREDICTOR_TRACE, TRACE_TYPE, PREDICTOR_PRECISION, PREDICTOR_RECALL, \
                                          NUMBER_OF_NODES, START_TIME, END_TIME)
    predictor.predictFailures ()

    print "Precision: %f" % (float (predictor.correctPredictions) / predictor.totalPredictions)
    print "Recall: %f" % (float (predictor.predictedEvents) / predictor.totalEvents)

##############################################################################################################################
# END OF MODULE                                                                                                              #
##############################################################################################################################

