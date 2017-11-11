
##############################################################################################################################
# MODULE FOR SIMULATION OF FAILURE PREDICTION                                                                                #
##############################################################################################################################

import random

##############################################################################################################################

# Defines a Predictor that predicts failures at regular intervals #
class predictorClass :

  def __init__ (self, fTrace, F_pTrace, R_pTrace, totalNodes, F_precision, R_precision, F_recall, R_recall, startTime):
    """ Init function for class predictorClass. 
        Returns None. """

    self.totalNodes = totalNodes
    self.F_precision = F_precision
    self.F_recall = float (F_recall)
    self.R_precision = R_precision
    self.R_recall = R_recall
    self.F_traceData = []
    self.R_traceData = []

    if F_pTrace :
      
      self.F_fTraceHandle = None
      self.F_pTraceHandle = open (F_pTrace, "r")
      self.readNextTraceLine (self.F_pTraceHandle, 0)

    else : 
      self.F_pTraceHandle = None
      self.F_fTraceHandle = open (fTrace, "r")
      self.readNextTraceLine (self.F_fTraceHandle, 0)

    if R_pTrace :

      self.R_fTraceHandle = None
      self.R_pTraceHandle = open (R_pTrace, "r")
      self.readNextTraceLine (self.R_pTraceHandle, 1)

    else :
      self.R_pTraceHandle = None
      self.R_fTraceHandle = open (fTrace, "r")
      self.readNextTraceLine (self.R_fTraceHandle, 1)

    self.nodeFailures = []
    self.timeOfFailures = [0 for i in range (self.totalNodes)]

    self.nodeRecoveries = []
    self.totalFailures = 0
    self.totalRecoveries = 0
    self.predictedFailures = 0
    self.predictedRecoveries = 0
    self.F_correctPredictions = 0
    self.R_correctPredictions =0
    self.F_totalPredictions = 0
    self.R_totalPredictions = 0

    self.timeOfPrediction = 0
    self.timeOfFirstF = 0

  ############################################################################################################################

  def readNextTraceLine (self, traceHandle, flag):
    """ Function to read the next line from the trace. 
        Updates the traceData list. """

    traceData = []

    nextTraceLine = traceHandle.readline ()
      
    if not nextTraceLine :

      if not flag :
        self.F_traceData = []

      else :
        self.R_traceData = []

      return

    nextTraceLine = nextTraceLine.strip ()
    traceData = [int (d) for d in nextTraceLine.split ("\t")]

    #print traceData

    if not flag :
      self.F_traceData = list (traceData)

    else :
      self.R_traceData = list (traceData)

    #print self.F_traceData, self.R_traceData

  ############################################################################################################################

  def invokePredictor (self, system, timeOfPrediction, interval) :
    """ Function that invokes failure predictor and recovery predictor """
 
    self.predictFailures (system, timeOfPrediction, interval) 
    self.predictRecoveries (system, timeOfPrediction, self.timeOfFirstF - timeOfPrediction)

  ############################################################################################################################
  
  def predictFailures (self, system, timeOfPrediction, interval) :
    """ Function that predicts the node failures for the next interval.
        Updates the list of nodes expected to fail in the interval. """

    endTime = timeOfPrediction + interval
    #self.nodeFailures = []
    self.timeOfPrediction = timeOfPrediction
    self.timeOfFirstF = 0
    nodesToBeRemoved = []
 
    for node in self.nodeFailures :
      if self.timeOfFailures [node.number] < timeOfPrediction :
        nodesToBeRemoved.append (node)

    for node in nodesToBeRemoved :
      self.nodeFailures.remove (node)
      self.timeOfFailures [node.number] = 0
 
    #if self.F_pTraceHandle :
    #  self.F_pTraceHandle.seek (0,0)

    #while self.F_traceData and self.F_traceData [2] < timeOfPrediction :
      
    #  if self.F_pTraceHandle == None :
    #    self.readNextTraceLine (self.F_fTraceHandle, 0)

    #  else :
    #    self.readNextTraceLine (self.F_pTraceHandle, 0)
    
    while self.F_traceData and self.F_traceData [2] <= endTime :

      if self.F_pTraceHandle == None :

        if self.F_traceData [0] == 0 :

          self.totalFailures += 1

          if random.random () < self.F_recall :

            self.predictedFailures += 1
            self.nodeFailures.append (system.nodeList [self.F_traceData [1]]) 
            self.F_correctPredictions += 1
            self.F_totalPredictions += 1

            if not self.timeOfFirstF :
              self.timeOfFirstF = self.F_traceData [2]

        self.readNextTraceLine (self.F_fTraceHandle, 0)
    
      else :
    
        self.nodeFailures.append (system.nodeList [self.F_traceData [1]])
        self.timeOfFailures [system.nodeList [self.F_traceData [1]].number] = self.F_traceData [2]

        if not self.timeOfFirstF :
          self.timeOfFirstF = self.F_traceData [2]

        self.readNextTraceLine (self.F_pTraceHandle, 0)
 
    if self.F_pTraceHandle == None :

      if not self.nodeFailures and random.random () < 0.5 :  

        while self.F_totalPredictions and float (self.F_correctPredictions) / \
                                             self.F_totalPredictions > self.F_precision :
          self.nodeFailures.append (system.nodeList [random.randint (0, self.totalNodes - 1)])
          self.F_totalPredictions += 1

          if not self.timeOfFirstF :
            self.timeOfFirstF = self.F_traceData [2]

      else : 

        while self.F_totalPredictions and random.random () < 0.5 and float (self.F_correctPredictions) / \
                                                                          self.F_totalPredictions > self.F_precision :
          self.nodeFailures.append (system.nodeList [random.randint (0, self.totalNodes - 1)])
          self.F_totalPredictions += 1

          if not self.timeOfFirstF :
            self.timeOfFirstF = self.F_traceData [2]

    self.nodeFailures = list (set (self.nodeFailures))

    #for node in self.nodeFailures :
    #  print node.number, self.timeOfFailures [node.number]

    #print "....."

  ############################################################################################################################

  def predictRecoveries (self, system, timeOfPrediction, interval) :
    """ Function that predicts the node recoveries for the given interval.
        Updates the list of nodes expected to recover in the interval. """

    endTime = timeOfPrediction + interval
    self.nodeRecoveries = []

    while self.R_traceData and self.R_traceData [2] < timeOfPrediction :

      if self.R_pTraceHandle == None :
        self.readNextTraceLine (self.R_fTraceHandle, 1)
     
      else :
        self.readNextTraceLine (self.R_pTraceHandle, 1)
    
    while self.R_traceData and self.R_traceData [2] <= endTime :

      if self.R_pTraceHandle == None :

        if self.R_traceData [0] == 0 :

          self.totalRecoveries += 1

          if random.random () < self.R_recall :

            self.predictedRecoveries += 1
            self.nodeRecoveries.append (system.nodeList [self.R_traceData [1]]) 
            self.R_correctPredictions += 1
            self.R_totalPredictions += 1

        self.readNextTraceLine (self.R_fTraceHandle, 1)
    
      else :
     
        self.nodeRecoveries.append (system.nodeList [self.R_traceData [1]])
        self.readNextTraceLine (self.R_pTraceHandle, 1)
 
    if self.R_pTraceHandle == None :

      if not self.nodeRecoveries and random.random () < 0.5 :  

        while self.R_totalPredictions and float (self.R_correctPredictions) / \
                                             self.R_totalPredictions > self.R_precision :
          self.nodeRecoveries.append (system.nodeList [random.randint (0, self.totalNodes - 1)])
          self.R_totalPredictions += 1

      else : 

        while self.R_totalPredictions and random.random () < 0.5 and float (self.R_correctPredictions) / \
                                                                          self.R_totalPredictions > self.R_precision :
          self.nodeRecoveries.append (system.nodeList [random.randint (0, self.totalNodes - 1)])
          self.R_totalPredictions += 1

    self.nodeRecoveries = list (set (self.nodeRecoveries))
    #if self.nodeRecoveries :
      #print [node.number for node in self.nodeRecoveries]
  
##############################################################################################################################
# END OF MODULE                                                                                                              #
##############################################################################################################################

