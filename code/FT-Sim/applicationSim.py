
##############################################################################################################################
# MODULE DEFINING ATTRIBUTES & FUNCTIONS FOR APPLICATION INSTANCES                                                           #
##############################################################################################################################

import random

##############################################################################################################################

# Defines an application running on the system and its attributes #
class applicationClass :
  
  def __init__ (self, system, nodes_n, repNodes_n, isMalleable, workUnit, scaleCurveFlag, \
                scaleCurveFromFile, scaleFileHandle, scaleCurveType, A, s, degradeFactor) :
    """ Init function for applicationClass. """
    self.system = system
    self.applStatus = "Stopped"
    self.isMalleable = isMalleable
    self.workUnit = workUnit
    self.scaleCurveFromFile = scaleCurveFromFile
    self.scaleFileHandle = scaleFileHandle
    self.scaleCurveFlag = scaleCurveFlag
    self.scaleCurveArray = self.scaleCurveGen (scaleCurveType, A, s, degradeFactor)
    self.totalWorkDone = 0
    self.timeSinceLastCP = 0
    self.timeSinceLastFailure = 0
    self.workDoneSinceLastCP = 0
    self.workDoneSinceLastAP = 0
    self.workingNodes = []
    self.repNodesList = []
    self.nodesApp = self.noOfNodesUsed (nodes_n - repNodes_n)
    self.nodesRep = repNodes_n
    self.nodesRepCurrent = 0
    self.stopCount = 0
    self.totalWorkLost = 0

  ############################################################################################################################
 
  def readNextScaleLine (self) :
    """ Function to read the next line from the trace. """

    scaleHandle = self.scaleFileHandle
    scaleLine = []
    nextScaleLine = scaleHandle.readline ()

    if not nextScaleLine :
      return scaleLine

    nextScaleLine = nextScaleLine.strip ()
    scaleLine = [float (d) for d in nextScaleLine.split ("\t")]
    return scaleLine

  ############################################################################################################################

  def scaleCurveGen (self, scaleCurveType, A, s, degradeFactor) :
    """ Function to generate scalability curve for the application. """

    scaleCurve = [0 for n in range (len (self.system.nodeList))]
    N = len (self.system.nodeList)

    if self.scaleCurveFromFile == 1 :

      for i in range (N) :
        scaleLine = self.readNextScaleLine ()
        scaleCurve [i] = scaleLine [1]

    else :
   
      for i in range (N) :

        n = i + 1

        if scaleCurveType == 1 :

          if 1 <= n and n <= A :
            scaleCurve [i] = self.workUnit * ((A * n) / (A + (s / 2) * (n - 1))) - degradeFactor * n
     
          elif A <= n and n <= (2 * A -1) :
            scaleCurve [i] = self.workUnit * ((A * n) / (s * (A - 1 / 2) + n * (1 - s / 2))) - degradeFactor * n

          elif (2 * A - 1) <= n :
            scaleCurve [i] = self.workUnit * A - degradeFactor * n

        elif scaleCurveType == 2 :
      
          if 1 <= n and n <= (A + A * s - s) :
            scaleCurve [i] = self.workUnit * ((n * A * (s + 1))/ (s * (n + A - 1) + A)) - degradeFactor * n

          elif (A + A * s - s) <= n :
            scaleCurve [i] = self.workUnit * A - degradeFactor * n
    
    #for i in range (N) :
      #print scaleCurve [i]
  
    #print len (scaleCurve)
 
    return scaleCurve

  ########################################################################################################################### 
   
  def updateStatus (self, failedNodes) :
    """ Updates the status of the application on node failure. """
    nodesToRemove = []

    # REPLICATION BEGIN
    flag = False
    # REPLICATION END

    for node in failedNodes :

      if node in self.workingNodes :
        nodesToRemove.append (node)
 
    if nodesToRemove :
     
      for node in nodesToRemove :

        if node in self.workingNodes : 

          # REPLICATION BEGIN
          if not node.replica or node.replica.down == True :
            flag = True

          elif node in self.repNodesList :
            self.repNodesList.remove (node)

          elif node.replica in self.repNodesList :
            self.repNodesList.remove (node.replica)
          # REPLICATION END
            
          self.workingNodes.remove (node)
          node.application = None
          node.replica = None

      # REPLICATION BEGIN
      if self.applStatus == "Running" and flag: 
      # REPLICATION END
        self.applStatus = "Stopped"
        self.stopCount += 1
        self.timeSinceLastFailure = 0
        #print "Application Failed!", self.stopCount

      #if self.applStatus == "Running" and not flag:
        #print "Saved due to replication :-)"

  ############################################################################################################################

  def updateApplStats (self, time) :
    """ Updates the work done in the previous time interval. """
    
    if self.applStatus == "Running" :
      self.totalWorkDone += self.workDoneReal (time, self.nodesApp)
      self.workDoneSinceLastCP += self.workDoneReal (time, self.nodesApp)
      self.timeSinceLastCP += time
      self.timeSinceLastFailure += time
      self.workDoneSinceLastAP += self.workDoneReal (time, self.nodesApp)

  ###########################################################################################################################

  def noOfNodesUsed (self, noOfNodesMax) :
    """ Function that returns the no. of nodes used if N nodes are available. 
        No. of nodes depend on the scheduling algorithm. 
        If application is not malleable, it will return noOfNodesMax. """

    if not self.isMalleable or self.scaleCurveFlag == 0 :
      return noOfNodesMax

    elif self.scaleCurveFlag == 1 :

      noOfNodesOptimal = noOfNodesMax
      
      while noOfNodesOptimal >= 2 and \
            self.scaleCurveArray [noOfNodesOptimal - 2] >= self.scaleCurveArray [noOfNodesOptimal - 1] :
        noOfNodesOptimal -= 1

      return noOfNodesOptimal
  
  ###########################################################################################################################
 
  def workDone (self, time, procs) :
    """ Returns the work that can be done by the application for the given time &
        no. of procs. """
  
    if self.scaleCurveFlag == 0 :
      return time * procs

    elif self.scaleCurveFlag == 1 :
      return time * self.scaleCurveArray [procs - 1]

  ############################################################################################################################
  
  def workDoneReal (self, time, procs) :
    """ Returns the work that the application actually does for the given time &
        no. of procs. """

    w = self.scaleCurveArray [procs - 1] #+ random.uniform (-0.05, 0.05) * self.scaleCurveArray [procs - 1]
    return time * w

  ############################################################################################################################

  def timeTaken (self, work, procs) :
    """ Returns the time taken by the application to complete 'work' amount of work using 'procs' processors. """

    w = self.scaleCurveArray [procs - 1]
    return float (work) / w 
  
##############################################################################################################################
# END OF MODULE                                                                                                              #
##############################################################################################################################

