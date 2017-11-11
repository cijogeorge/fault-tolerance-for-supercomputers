
##############################################################################################################################
# MODULE FOR SIMULATION OF FAULT TOLERANCE ACTIONS 								             #
##############################################################################################################################

import math

##############################################################################################################################

# Defines a class for fault tolerance actions #
class faultToleranceClass :
 
  def __init__ (self, W, MTBF, ftAction, repEnabled, cpInterval, predictor, useRecoveryPrediction, reschOnRecoveryFlag, TTFflag, \
                cpTime, migTime, reschTime, recoverTime, lb, ub) :
    """ Init function for class faultTolerance. """
    self.W = W
    self.ftAction = ftAction
    self.repEnabled = repEnabled
    self.cpInterval = cpInterval
    self.predictor = predictor
    self.reschOnRecoveryFlag = reschOnRecoveryFlag
    self.useRecoveryPrediction = useRecoveryPrediction
    self.TTFflag = TTFflag
    self.cpTime = cpTime
    self.migTime = migTime
    self.recoverTime = recoverTime
    self.reschTime = reschTime
    self.repChangeThresh = 1800
    self.currentTime = 0
    self.efMTTIBasedOnRecall = 0
    self.mtbf = MTBF
    self.lb = lb
    self.ub = ub

    self.APindex = 0
    self.noOfProactiveCP = 0
    self.noOfProactiveMig = 0
    self.noOfProactiveSkip = 0
    self.noOfProactiveResch = 0
    self.noOfOppCP = 0
    self.noOfOppMig = 0
    self.noOfOppSkip = 0
    self.noOfOppResch = 0
    
    self.noOfRepChanges = 0
    self.noOfSwaps = 0
    self.noOfAdapts = 0

    self.noOfReacReschUp = 0
    self.noOfReacReschDown = 0
    self.noOfPerCP = 0

    # log files #
 
    self.NfNsfile = None
    self.costFilePro = None
    self.costFileOpp = None
    
  ############################################################################################################################

  def nodeFailureAction (self, time, system, failedNodes) :
    """ Function to handle a node failures. """
    #print "nodeFailure"
    for node in failedNodes :
      
      if node.down == False :  

        node.nodeDown (time)
        system.downNodes.append (node)

        if node in system.spareNodes :
          system.spareNodes.remove (node)
 
        if node in self.predictor.nodeFailures :
          self.predictor.nodeFailures.remove (node)

        #else:
        #  print "IMPENDING FAILURE"

    system.application.updateStatus (failedNodes)

    #print len (system.application.workingNodes) 
    if system.application.applStatus == "Stopped" :
      #print "Stopped"
      system.recoverAppl (system.application, self)

  ############################################################################################################################
 
  def nodeRecoveryAction (self, recoveryTime, system, recoveredNodes) :
    """ Function to handle node recoveries. """
    #print "nodeRecovery"
    for node in recoveredNodes :
     
      if node.down == True : 

        node.nodeUp ()
        system.downNodes.remove (node)
        system.spareNodes.append (node)

    if self.reschOnRecoveryFlag and system.application.applStatus == "Running" :
      self.reschOnRecovery (recoveryTime, system)

    elif system.application.applStatus == "Stopped" :
      system.recoverAppl (system.application, self)

  ############################################################################################################################

  def checkpoint (self, time, appl, n) :
    """ Function to checkpoint an application. """
    appl.timeSinceLastCP = time
    #if appl.timeSinceLastCP < 0:
      #print "checkpoint: something is wrong!"
    appl.workDoneSinceLastCP = appl.workDoneReal (time, appl.nodesApp)
    appl.totalWorkDone -= n * appl.workDoneReal (self.cpTime, appl.nodesApp)
    #print "checkpoint"
  
  ############################################################################################################################

  def periodicCP (self, application, n) :
    """ Function for periodic checkpointing. """
    time = application.timeSinceLastCP - n * (self.cpInterval + self.cpTime)
    self.checkpoint (time, application, n)

  ############################################################################################################################

  def proactiveCP (self, pastTime, application) :
    """ Function for proactive checkpointing. """
    self.checkpoint (pastTime - self.cpTime, application, 1)
    self.noOfProactiveCP += 1

  ############################################################################################################################

  def migrate (self, system, appl, srcList, destList) :
    """ Function to migrate a set of nodes from source to destination. """
    if len (srcList) != len (destList) : 
      print "migrate: len (sourceNodes) != len (destNodes); Cannot migrate!"
   
    else :
    
      #print "migrating.."  
      for srcNode in srcList :

        destNode = destList.pop (0)

        if destNode.application == None and destNode.replica == None :

          destNode.application = appl
          destNode.replica = srcNode.replica
          srcNode.application = None
          srcNode.replica = None

          system.application.workingNodes.remove (srcNode)
          system.application.workingNodes.append (destNode)

          if srcNode in system.application.repNodesList :

            system.application.repNodesList.remove (srcNode)
            system.application.repNodesList.append (destNode)

          system.spareNodes.append (srcNode)
          system.spareNodes.remove (destNode)

        else :
          print "migrate: Something is wrong!"

        appl.totalWorkDone -= appl.workDoneReal (self.migTime, appl.nodesApp)
        appl.workDoneSinceLastCP -= appl.workDoneReal (self.migTime, appl.nodesApp)

        self.noOfProactiveMig += 1

  ############################################################################################################################

  def repSwap (self, system, sourceList, targetList, spareList) :
    """ Function to swap. Used in replication."""

    #print "swapping.."

    if len (sourceList) == len (targetList) and len (targetList) == len (spareList) :

      self.migrate (system, system.application, sourceList, spareList)
      self.migrate (system, system.application, targetList, sourceList)
      self.noOfSwaps += 1

    else :
      print "repSwap: Something wrong!"

  ############################################################################################################################
  
  def proactiveMigration (self, pastTime, system, nodeFailuresRaw) :
    """ Function for proactive Migration. """
    #self.proactiveCP (pastTime, system.application)

    nodeFailures = [node for node in nodeFailuresRaw if node in system.application.workingNodes]
    #print "FIN. PRED. LEN.:", len (nodeFailures)

    healthySpareNodes = [node for node in system.spareNodes if node not in nodeFailuresRaw] 
    healthySparesNo = len (healthySpareNodes)

    #print "No. of Healthy Spare Nodes: ", healthySparesNo

    if healthySparesNo == 0 :
      return 1

    nodeFailuresReal = []
    sourceNodes = []
    destNodes = []   
 
    # REPLICATION BEGIN

    if self.repEnabled :
      swapList = []

      for node in nodeFailures :

        if not node.replica or (node.replica in nodeFailures and node.replica not in nodeFailuresReal) :
          nodeFailuresReal.append (node)

    # REPLICATION END

    else :
      nodeFailuresReal = nodeFailures
    
    #print healthySparesNo, len (nodeFailuresReal)

    appl = system.application
   
    if self.TTFflag == 1 :
      nodeFailuresReal.sort (key = lambda node: node.timeToNextFailure)
   
    if appl.applStatus == "Running" :
 
      for node in nodeFailuresReal :
          
        if node.application == appl and healthySparesNo > 0 :
          sourceNodes.append (node)
          healthySparesNo -= 1
  
        elif healthySparesNo == 0 :
          
          # REPLICATION BEGIN

          if self.repEnabled :
            swapList.append (node)

          # REPLICATION END

          else :
            break

      # REPLICATION BEGIN

      if self.repEnabled and swapList :

        repListForSwapTemp = [node for node in appl.repNodesList if node not in nodeFailures \
                              and node.replica not in nodeFailures]

        if repListForSwapTemp :

          repListForSwap = []

          if len (repListForSwapTemp) < len (swapList) :
            swapListFinal = [node for node in swapList if swapList.index (node) < len (repListForSwapTemp)]
            repListForSwap = repListForSwapTemp

          else :
            swapListFinal = swapList

            for i in range (len (swapList)) :
              repListForSwap.append (repListForSwapTemp.pop (0))

          n = int (len (swapListFinal) / len (healthySpareNodes))
          sourceListForSwap = []
          targetListForSwap = []

          while n > 0 :
        
            for i in range (len (healthySpareNodes)) :

              sourceListForSwap.append (swapListFinal.pop (0))
              targetListForSwap.append (repListForSwap.pop (0))

            self.repSwap (system, sourceListForSwap, targetListForSwap, healthySpareNodes)
            healthySpareNodes = [node for node in system.spareNodes if node not in nodeFailures]
            sourceListForSwap = []
            targetListForSwap = []
            n -= 1
            #print len (healthySpareNodes)
        
          n = len (swapListFinal)
          spareListForSwap = []

          if n > 0 :
            spareListForSwap = [node for node in healthySpareNodes if healthySpareNodes.index (node) < n]

          self.repSwap (system, swapListFinal, repListForSwap, spareListForSwap)
          healthySpareNodes = [node for node in system.spareNodes if node not in nodeFailures]

      # REPLICATION END
          
      destNodes = self.selectNodes (healthySpareNodes, len (sourceNodes))
      self.migrate (system, appl, sourceNodes, destNodes)
 
      return 0

  ############################################################################################################################

  def selectNodes (self, nodesAvailable, noOfNodesReq) :
    """ Selects 'noOfNodesReq' no. of nodes from 'nodesAvailable' list.
        Selection depends on value of ttfFlag. """
    if len (nodesAvailable) > noOfNodesReq :

      if self.TTFflag == 0 :
        return [node for node in nodesAvailable if nodesAvailable.index (node) < noOfNodesReq]

      elif self.TTFflag == 1 :
        nodesAvailable.sort (key = lambda node: node.timeToNextFailure, reverse=True)
        return [node for node in nodesAvailable if nodesAvailable.index (node) < noOfNodesReq]

    else :
      return nodesAvailable
       
  ############################################################################################################################

  def reschedule (self, pastTime, appl, newNodes) :
    """ Function to Redistribute. """
    appl.totalWorkDone -= appl.workDoneReal (pastTime, appl.nodesApp)

    appl.nodesApp = appl.noOfNodesUsed (len (newNodes))

    if appl.nodesApp != len (appl.workingNodes) :
      reschFlag = 1

    else :
      reschFlag = 0

    appl.workingNodes = self.selectNodes (list (newNodes), appl.nodesApp)

    if len (appl.workingNodes) < len (newNodes) :

      for node in newNodes :
        if node not in appl.workingNodes :
          node.application = None
          appl.system.spareNodes.append (node)

    for node in appl.workingNodes :
      node.application = appl

    if reschFlag :
      appl.totalWorkDone += appl.workDoneReal ((pastTime - self.reschTime - self.recoverTime), appl.nodesApp)

    else :
      appl.totalWorkDone += appl.workDoneReal ((pastTime - self.recoverTime), appl.nodesApp)

  ############################################################################################################################

  def proactiveReschedule (self, pastTime, system, nodeFailures):
    """ Function for proactive Reschedule. """
    self.proactiveCP (pastTime, system.application)
    newNodes = []

    for node in system.spareNodes :
      if node not in nodeFailures :
        newNodes.append (node)

    for node in newNodes :
      if node in system.spareNodes :
        system.spareNodes.remove (node)

    for node in system.application.workingNodes :

      if node in nodeFailures :
        node.application = None
        system.spareNodes.append (node)
        
      else :
        newNodes.append (node)
  
    self.reschedule (pastTime, system.application, newNodes)

  ############################################################################################################################

  def changeReplication (self, pastTime, system, flag) :
    """ Function to change the degree of replication """

    appl = system.application
    self.proactiveCP (pastTime, appl)
    appl.totalWorkDone -= appl.workDoneReal (pastTime, appl.nodesApp)
 
    if flag :

      if appl.nodesRep == 0 :

        for node in system.spareNodes :
          node.application = appl
          node.replica = None
          appl.workingNodes.append (node)
          system.spareNodes.remove (node)
          
        for index in range (len (appl.workingNodes) / 2) :
          indexRep = index + (len (appl.workingNodes) / 2)
          appl.workingNodes [index].replica = appl.workingNodes [indexRep]
          appl.workingNodes [indexRep].replica = appl.workingNodes [index]
        
        appl.nodesRep = len (appl.workingNodes) / 2
        appl.nodesRepCurrent = appl.nodesRep
        appl.nodesApp = len (appl.workingNodes) - appl.nodesRep
        self.repEnabled = 1
  
      else :
        print "changeReplication 1: Something Wrong!"

    else :

      if appl.nodesRep != 0 :

        while len (appl.workingNodes) < appl.nodesApp * 2 and system.spareNodes:
          node = system.spareNodes.pop (0)
          node.application = appl
          appl.workingNodes.append (node)
        
        #print len (appl.workingNodes), len (system.spareNodes), len (system.downNodes)

        #if len (appl.workingNodes) == appl.nodesApp * 2 :

        for node in appl.workingNodes:
          node.replica = None
          
        appl.nodesRep = 0
        appl.nodesRepCurrent = 0
        appl.nodesApp = len (appl.workingNodes)
        self.repEnabled = 0

        #else :
        #  print "changeReplication: Not Enough Nodes!"

      else :
        print "changeReplication 2: Something Wrong!"

    self.noOfRepChanges += 1
    appl.totalWorkDone += appl.workDoneReal ((pastTime - self.reschTime - self.recoverTime), appl.nodesApp)

  ###################################################################################################################

  def adaptiveRep (self, pastTime, system, flag) :

    nodeFailures = [node for node in self.predictor.nodeFailures if node in system.application.workingNodes]
    nodeFailuresReal = []

    for node in nodeFailures :

      if not node.replica or (node.replica in nodeFailures and node.replica not in nodeFailuresReal) :
        nodeFailuresReal.append (node)

    #if nodeFailuresReal and flag :
    #  system.application.totalWorkDone -= system.application.workDoneReal (pastTime, system.application.nodesApp)
    #  self.proactiveCP (pastTime, system.application)

    P = self.predictor.F_precision
    appl = system.application

    healthySpareNodes = [node for node in system.spareNodes if node not in self.predictor.nodeFailures]

    index = 0

    #print "LEN of nodeFailuresReal", len (nodeFailures)
    #print "LEN of replicas", appl.nodesRepCurrent
    #print "LEN of healthy spares", len (healthySpareNodes)
    #for node in nodeFailures :
    #  print node.number

    for node in nodeFailuresReal :

      #print "..", node.number

      if healthySpareNodes and len (appl.workingNodes) < appl.nodesApp + appl.nodesRep :

        repNode = healthySpareNodes.pop (0)
        repNode.application = appl
        repNode.replica = node
        node.replica = repNode
        system.spareNodes.remove (repNode)
        appl.workingNodes.append (repNode)
        appl.repNodesList.append (repNode)
        appl.nodesRepCurrent += 1
        #print "hi1"

      else :
        #print "hi2", appl.nodesRep

        while index < len (appl.workingNodes) :
       
          repNode = appl.workingNodes [index]

          if repNode.replica and repNode.replica.replica == repNode \
             and repNode not in nodeFailures and repNode.replica not in nodeFailures :

            repNode.replica.replica = None
            
            if repNode.replica in appl.repNodesList :
              appl.repNodesList.remove (repNode.replica)
              appl.repNodesList.append (repNode)

            repNode.replica = node
            node.replica = repNode
            #print node.number
            index += 1
            break

          #elif repNode.replica and repNode.replica.replica != repNode :
          #  print "Scene 1"

          #elif repNode in nodeFailures or repNode.replica in nodeFailures:
          #  print "Scene 2"
 
          index += 1

      self.noOfAdapts += 1
      #print index

    """if nodeFailures and appl.nodesRep == 0:

      result = self.proactiveMigration (pastTime, system, nodeFailures)

      if result and appl.nodesRep == 0 :

        self.changeReplication (pastTime, system, 1)
 
    elif not nodeFailures :

      if appl.nodesRep != 0 :
        self.changeReplication (pastTime, system, 0)"""

    #if nodeFailuresReal and flag :
    #  appl.totalWorkDone += appl.workDoneReal ((pastTime - self.recoverTime), appl.nodesApp)

    for node in nodeFailures :
      if not node.replica :
        print "adaptiveRep: Something Wrong!"
   
  ############################################################################################################################
 
  def findMTTI (self, nodesRep, nodesReq):
    """Function to find MTTI"""

    if nodesRep :
    
      F = pow (float ((22/7) * nodesRep) / 2, 0.5) + (2/3)

      if nodesRep < nodesReq / 2 :

        mtti_1 = (float (self.mtbf) / (nodesRep * 2)) * F
        mtti_2 = float (self.mtbf) / (nodesReq - 2 * nodesRep)
        mtti = (mtti_1 * mtti_2) / (mtti_1 + mtti_2)

      else :
        mtti = self.mtbf / ((nodesRep * 2)) * F
 
    else :
      mtti = float (self.mtbf) / nodesReq

    return mtti

  ############################################################################################################################

  def findNewRepNo (self, appl, pastTime) :
    """ Function to find new replication number at each AP"""

    newNodesRep = appl.nodesRep
    newNodesApp = appl.nodesApp
    efMTTIBasedOnRecall = self.efMTTIBasedOnRecall

    if self.efMTTIBasedOnRecall <= appl.timeTaken (self.W, appl.nodesApp) + appl.timeSinceLastFailure - pastTime :

      while newNodesRep < self.ub * (appl.nodesApp + appl.nodesRep) and efMTTIBasedOnRecall / 2 \
                                                                        <= appl.timeTaken (self.W, newNodesApp) \
                                                                           + self.cpTime + self.reschTime + self.recoverTime \
                                                                           + appl.timeSinceLastFailure - pastTime :

        newNodesRep += 1
        newNodesApp -= 1
        mtti = self.findMTTI (newNodesRep, newNodesRep + newNodesApp)

        efMTTIBasedOnRecall = float (mtti) / (1 - self.predictor.F_recall)

      T1 = appl.timeTaken (self.W, newNodesApp) + self.cpTime + self.reschTime + self.recoverTime
      T2 = appl.timeTaken (2 * self.W, appl.nodesApp) + self.cpTime + self.recoverTime
      T3 = appl.timeTaken (2 * self.W, appl.nodesApp) + appl.timeSinceLastCP + self.recoverTime

      if T1 < T2 and T1 < T3 :

        self.efMTTIBasedOnRecall = efMTTIBasedOnRecall
        #print "1", newNodesRep, self.predictor.F_recall, mtti/3600, efMTTIBasedOnRecall/3600, \
        #                        (appl.timeSinceLastFailure - pastTime + appl.timeTaken (self.W, appl.nodesApp))/3600
        return newNodesRep

      elif T2 < T1 and T2 < T3 :

        self.proactiveCP (pastTime, appl)
        return appl.nodesRep

      else :
        return appl.nodesRep

    elif self.efMTTIBasedOnRecall * 2 > appl.timeTaken (self.W, appl.nodesApp) + appl.timeSinceLastFailure - pastTime :

      newNodesRepFinal = appl.nodesRep
      newNodesAppFinal = appl.nodesApp

      while newNodesRep > self.lb * (appl.nodesApp + appl.nodesRep) and efMTTIBasedOnRecall * 2 \
                                                                    > appl.timeTaken (self.W, newNodesApp) \
                                                                      + appl.timeSinceLastFailure - pastTime :

        newNodesRep -= 1
        newNodesApp += 1
        mtti = self.findMTTI (newNodesRep, newNodesRep + newNodesApp)
        efMTTIBasedOnRecall = float (mtti) / (1 - self.predictor.F_recall)

        if efMTTIBasedOnRecall > appl.timeSinceLastFailure - pastTime + appl.timeTaken (self.W, newNodesApp) :
          efMTTIBasedOnRecallFinal = efMTTIBasedOnRecall
          newNodesRepFinal = newNodesRep
          newNodesAppFinal = newNodesApp

      if appl.timeTaken (self.W, newNodesAppFinal) + self.cpTime + self.reschTime + self.recoverTime \
         < appl.timeTaken (self.W, appl.nodesApp) :
        
        self.efMTTIBasedOnRecall = efMTTIBasedOnRecallFinal
        #print "2", newNodesRepFinal, efMTTIBasedOnRecallFinal/3600, \
        #                             (appl.timeSinceLastFailure - pastTime + appl.timeTaken (self.W, appl.nodesApp))/3600
        return newNodesRepFinal

      else :
        return appl.nodesRep

  ############################################################################################################################

  def repDegreeChange (self, time, pastTime, system, n, changeNo, flag) :

    appl = system.application

    #print len (appl.workingNodes)

    #for node in appl.workingNodes :
    #  if node.replica and node.replica.replica != node :
    #    print "WAT THE HELL1"
    #    break

    while appl.nodesRepCurrent < appl.nodesRep :
      
      for node in appl.workingNodes :

        if not node.replica :

          repNode = system.spareNodes.pop (0)
          repNode.application = appl
          repNode.replica = node
          node.replica = repNode
          appl.workingNodes.append (repNode)
          appl.nodesRepCurrent += 1
          break

    #for node in appl.workingNodes :
    #  if node.replica and node.replica.replica != node :
    #    print "WAT THE HELL2"
    #    break

    x = n * changeNo

    while not flag and appl.nodesRepCurrent - x < 0 :

      x -= changeNo
      n -= 1

    while flag and appl.nodesRepCurrent + x > appl.nodesApp - x :
      x -= changeNo
      n -= 1

    appl.totalWorkDone -= appl.workDoneReal (pastTime + ((n - 1) * self.repChangeThresh), appl.nodesApp)
    self.proactiveCP (pastTime, appl)
    nodesToBeRemoved = []

    if not flag and x > 0:

      for node in appl.workingNodes :

        if node.replica :

          node.replica.replica = None
          node.replica = None
          
          x -= 1
          appl.nodesApp += 1
          appl.nodesRep -= 1
          appl.nodesRepCurrent -= 1

        if not x :
          break

      if x < 0 or x > 0 :
        print "repDegreeChange: something wrong!"

      #for node in appl.workingNodes :
      #  if node.replica and node.replica.replica != node :
      #    print "WAT THE HELL3"
      #    break


    if flag and x > 0 :
   
      index = len (appl.workingNodes) - 1

      for node1 in appl.workingNodes :

        #print 1
        if not node1.replica :

          while index > 0 :
 
            node2 = appl.workingNodes [index]
            
            if node1 != node2 and not node2.replica :
              
              node1.replica = node2
              node2.replica = node1
               
              x -= 1
              appl.nodesApp -= 1
              appl.nodesRep += 1
              appl.nodesRepCurrent += 1
              index -= 1
              #print 2
              break
 
            index -= 1

          if not x :
            break

          elif index == 0 :
            print "repDegreeChange: index = 0. Something Wrong!"

      #for node in appl.workingNodes :
      #  if node.replica and node.replica.replica != node :
      #    print "WAT THE HELL4"
      #    break

    self.noOfRepChanges += 1

    while n > 1 :
      appl.totalWorkDone += appl.workDoneReal ((self.repChangeThresh - self.reschTime - self.recoverTime), \
                                               appl.nodesApp - (n - 1) * changeNo)
      repDeg = float ((appl.nodesApp - (n - 1) * changeNo) + (appl.nodesRep + (n - 1) * changeNo)) \
                     / (appl.nodesApp - (n - 1) * changeNo)

      t = time - (n - 1) * self.repChangeThresh

      n -= 1

    appl.totalWorkDone += appl.workDoneReal ((pastTime - self.reschTime - self.recoverTime), appl.nodesApp)

    repDeg = float (appl.nodesApp + appl.nodesRep) / appl.nodesApp

    appl.timeSinceLastRepChange = pastTime

    if appl.nodesRep and not self.repEnabled :
      self.repEnabled = 1

    if not appl.nodesRep and self.repEnabled :
      self.repEnabled = 0

    #for node in appl.workingNodes :
    #  if node.replica and node.replica.replica != node :
    #    print "WAT THE HELL"
    #    break

    #print len (appl.workingNodes)
    
  ############################################################################################################################

  def repChangeBasedOnMTTI (self, pastTime, system) :
    """ Function to change replication based on MTTI"""
   
    appl = system.application
    time = self.currentTime - pastTime
    newNodesRep = self.findNewRepNo (appl, pastTime)

    #for node in appl.workingNodes :
    #  if node.replica and node.replica.replica != node :
    #    print "repCH1"
    #    break

    if newNodesRep > appl.nodesRep :
      self.repDegreeChange (time, pastTime, system, 1, newNodesRep - appl.nodesRep, 1)
      #print "Changing Replication UP"
      return 0

      #for node in appl.workingNodes :
      #  if node.replica and node.replica.replica != node :
      #    print "repCH2"
      #    break

    elif newNodesRep < appl.nodesRep :
      self.repDegreeChange (time, pastTime, system, 1, appl.nodesRep - newNodesRep, 0)
      #print "Changing Replication DOWN"
      return 0

      #for node in appl.workingNodes :
      #  if node.replica and node.replica.replica != node :
      #    print "repCH3"
      #    break

    return 1

  ############################################################################################################################

  def proactiveAction (self, pastTime, system) :
    """ Function to handle a node failures. """
    nodeFailures = [node for node in self.predictor.nodeFailures]
 
    appl = system.application
    returnVal = 0
    
    if self.ftAction == 0 :

      self.proactiveCP (pastTime, appl)
      #self.noOfProactiveCP += 1

    elif self.ftAction == 1 :
      
      self.proactiveMigration (pastTime, system, nodeFailures)
      #self.noOfProactiveMig += 1

    elif self.ftAction == 2 :
      
      if appl.isMalleable :
        self.proactiveReschedule (pastTime, system, nodeFailures)
        self.noOfProactiveResch += 1

    elif self.ftAction == 3 :
      I = appl.timeTaken (self.W, appl.nodesApp)
      self.paperWork (pastTime, system, I)
      
    elif self.ftAction == 4 :
      I = appl.timeTaken (self.W, appl.nodesApp)
      returnVal = self.model_1 (pastTime, system, I)

    elif self.ftAction == 5 :
      I = appl.timeTaken (self.W, appl.nodesApp)
      returnVal = self.model_2 (pastTime, system, I)

    elif self.ftAction == 6 :

      flag = 1
      self.adaptiveRep (pastTime, system, flag)

    elif self.ftAction == 7 :

      if self.predictor.F_recall :
        self.predictor.F_recall = 0

      if not self.efMTTIBasedOnRecall :
        appl = system.application
        mtti = self.findMTTI (appl.nodesRep, appl.nodesRep + appl.nodesApp)
        self.efMTTIBasedOnRecall = float (mtti) / (1 - self.predictor.F_recall)

      flag = self.repChangeBasedOnMTTI (pastTime, system)

    elif self.ftAction == 8 :

      if not self.efMTTIBasedOnRecall :
        appl = system.application
        mtti = self.findMTTI (appl.nodesRep, appl.nodesRep + appl.nodesApp)
        self.efMTTIBasedOnRecall = float (mtti) / (1 - self.predictor.F_recall)

      flag = self.repChangeBasedOnMTTI (pastTime, system)
      self.adaptiveRep (pastTime, system, flag)
    
    elif self.ftAction == 9 :
      I = appl.timeTaken (self.W, appl.nodesApp)
      returnVal = self.adaptiveReplication (pastTime, system, I)

    return returnVal

  ############################################################################################################################

  def reschOnRecovery (self, recoveryTime, system) :
    """ Function that does opportunistic rescheduling on node recovery. """
    nodeFailures = list (self.predictor.nodeFailures)
    appl = system.application

    if self.reschOnRecoveryFlag == 1 :
      self.proactiveReschedule (0, system, nodeFailures)
      self.noOfOppResch += 1

    elif self.reschOnRecoveryFlag == 2 :
      I = appl.timeTaken (self.W - appl.workDoneSinceLastAP, appl.nodesApp)
      self.model_1 (0, system, I)

    elif self.reschOnRecoveryFlag == 3 :
      I = appl.timeTaken (self.W - appl.workDoneSinceLastAP, appl.nodesApp)
      self.model_2 (0, system, I)

  ############################################################################################################################

  def paperWork (self, pastTime, system, I) :

    nodeFailures = list (self.predictor.nodeFailures)
    P = self.predictor.F_precision
    appl = system.application

    Nf = len ([node for node in nodeFailures if node.application == appl])
    Ns = len ([node for node in system.spareNodes if node not in nodeFailures])

    timeSinceLastCP = appl.timeSinceLastCP

    fappl = 1 - pow (1 - P, Nf)

    EnxtSkip = fappl * (self.recoverTime + 2 * I + timeSinceLastCP) + (1 - fappl) * I
    EnxtCheckpoint = fappl * (self.cpTime + self.recoverTime + 2 * I) + (I + self.cpTime) * (1 - fappl)

    if Nf > Ns :
      fappl = 1 - pow (1 - P, Nf - Ns)
    
    else :
      fappl = 0

    EnxtMigration = fappl * (self.cpTime + self.migTime + self.recoverTime + 2 * I + timeSinceLastCP) \
                  + (1 - fappl) * (I + self.cpTime + self.migTime)

    if EnxtCheckpoint < EnxtSkip and EnxtCheckpoint < EnxtMigration:
      self.proactiveCP (pastTime, appl)
      #self.noOfProactiveCP += 1
      Action = "Checkpoint"
 
    elif EnxtMigration < EnxtSkip :
      self.proactiveMigration (pastTime, system, nodeFailures)
      self.noOfProactiveMig += 1
      Action = "Migrate"

    else :

      Action = "No Action"

      if Nf :
        self.noOfProactiveSkip += 1
        Action = "Skip"

    """self.NfNsfile.write ("%d\t%d\t%d\t%s\n" % (self.APindex, Ns, Nf, Action))

    if Nf :
      self.costFilePro.write ("%d\t%.2f\t%.2f\t%.2f\t%s\n" % (self.APindex, EnxtSkip, EnxtCheckpoint, \
                                                              EnxtMigration, Action))"""

  ############################################################################################################################

  def model_1 (self, pastTime, system, I) :

    nodeFailures = list (self.predictor.nodeFailures)
    P = self.predictor.F_precision
    appl = system.application
    returnVal = 0

    Nf = len ([node for node in nodeFailures if node.application == appl])
    Ns = len ([node for node in system.spareNodes if node not in nodeFailures])

    if self.useRecoveryPrediction :
      nodeRecoveries = list (self.predictor.nodeRecoveries)
      Nr = len ([node for node in nodeRecoveries if node not in nodeFailures and node not in system.spareNodes \
                                                                             and node.application == None ])

    else :
      Nr = 0

    ##print Nr

    Nw = appl.nodesApp

    timeSinceLastCP = appl.timeSinceLastCP
    Wlost = appl.workDoneSinceLastCP

    fappl = 1 - pow (1 - P, Nf)
      
    # SKIP #
 
    EnxtSkip = fappl * (I + (self.reschTime + self.recoverTime)
                        + appl.timeTaken ((Wlost + self.W), appl.noOfNodesUsed (Nw - Nf + Ns + Nr))) \
             + (1 - fappl) * I

    # CHECKPOINT #

    EnxtCheckpoint = fappl * (self.cpTime + I + (self.reschTime + self.recoverTime) \
                              + appl.timeTaken (self.W, appl.noOfNodesUsed (Nw - Nf + Ns + Nr))) \
                   + (1 - fappl) * (self.cpTime + I)

    # MIGRATION #

    if Nf <= Ns :
      fappl = 0

    else :
      fappl = 1 - pow (1 - P, Nf - Ns)

    EnxtMigration = fappl * (self.cpTime + self.migTime + I + (self.reschTime + self.recoverTime) \
                             + appl.timeTaken ((Wlost + self.W), appl.noOfNodesUsed (Nw - Nf + Ns + Nr))) \
                  + (1 - fappl) * (self.cpTime + self.migTime + I)

    # RESCHEDULE #
  
    EnxtReschedule = self.cpTime + self.reschTime + self.recoverTime \
                     + appl.timeTaken (self.W, appl.noOfNodesUsed (Nw - Nf + Ns))       
   
    #print "%.2f %.2f %.2f %.2f" % (EnxtSkip, EnxtCheckpoint, EnxtMigration, EnxtReschedule)
 
    if EnxtReschedule < EnxtSkip and EnxtReschedule < EnxtMigration and EnxtReschedule < EnxtCheckpoint :
      self.proactiveReschedule (pastTime, system, nodeFailures)

      if Nf :
        self.noOfProactiveResch += 1
        Action = "Pro. Resch."
    
      else :
        self.noOfOppResch += 1
        Action = "Opp. Resch."

    elif EnxtCheckpoint < EnxtSkip and EnxtCheckpoint < EnxtMigration :
      self.proactiveCP (pastTime, appl)
      #self.noOfProactiveCP += 1
      Action = "Checkpoint"
      
    elif EnxtMigration < EnxtSkip :
      self.proactiveMigration (pastTime, system, nodeFailures)
      self.noOfProactiveMig += 1
      returnVal = 1
      Action = "Migrate"

    else :

      returnVal = 1
      Action = "No Action"

      if Nf :
        self.noOfProactiveSkip += 1
        Action = "Skip"

    """self.NfNsfile.write ("%d\t%d\t%d\t%s\n" % (self.APindex, Ns, Nf, Action))

    if Nf :
      self.costFilePro.write ("%d\t%.2f\t%.2f\t%.2f\t%.2f\t%s\n" % (self.APindex, EnxtSkip, EnxtCheckpoint, \
                                                                    EnxtMigration, EnxtReschedule, Action))

    else :
      self.costFileOpp.write ("%d\t%.2f\t%.2f\t%s\n" % (self.APindex, EnxtSkip, EnxtReschedule, Action))"""

    return returnVal
   
  ############################################################################################################################

  def adaptiveReplication (self, pastTime, system, I) :

    nodeFailures = []

    for node in list (self.predictor.nodeFailures) :

      if not node.replica or (node.replica in self.predictor.nodeFailures and node.replica not in nodeFailures) :
        nodeFailures.append (node)

    P = self.predictor.F_precision
    R = self.predictor.F_recall

    appl = system.application
    returnVal = 0

    Nf = len ([node for node in nodeFailures if node.application == appl])    
    Ns = len ([node for node in system.spareNodes if node not in nodeFailures])
    Nrep = appl.nodesRep

    if self.useRecoveryPrediction :
      nodeRecoveries = list (self.predictor.nodeRecoveries)
      Nr = len ([node for node in nodeRecoveries if node not in nodeFailures and node not in system.spareNodes \
                                                                             and node.application == None])
    else :
      Nr = 0

    #print Nr

    Nw = appl.nodesApp

    timeSinceLastCP = appl.timeSinceLastCP
    Wlost = appl.workDoneSinceLastCP
      
    EnxtSkip = EnxtCheckpoint = EnxtMigration = EnxtReschedule = 0
      
    # SKIP #

    for i in range (1, Nf + 1) :
      
      overhead1 = 0
      for j in range (1, i + 1) :
        overhead1 += appl.timeTaken ((Wlost + self.W), appl.noOfNodesUsed (Nw - j + Ns + Nr))

      overhead2 = i * (self.reschTime + self.recoverTime)

      EnxtSkip += self.nCr (Nf, i) * pow (P, i) * pow ((1 - P), (Nf - i)) \
                  * (I + overhead2 + overhead1) 
        
    EnxtSkip += pow ((1 - P), Nf) * I

    # CHECKPOINT #

    for i in range (1, Nf + 1) :

      overhead1 = 0
      for j in range (1, i + 1) :
        overhead1 += appl.timeTaken (self.W, appl.noOfNodesUsed (Nw - j + Ns + Nr))

      overhead2 = i * (self.reschTime + self.recoverTime)

      EnxtCheckpoint += self.nCr (Nf, i) * pow (P, i) * pow ((1 - P), (Nf - i)) \
                        * (self.cpTime + I + overhead2 + overhead1)
             
    EnxtCheckpoint += pow ((1 - P), Nf) * (self.cpTime + I)

    # MIGRATION #

    if Nf <= Ns :
      EnxtMigration = self.migTime + I

    else :
      
      Nfm = Nf - Ns

      for i in range (1, Nfm + 1) :

        overhead1 = 0
        for j in range (1, i + 1) :
          overhead1 += appl.timeTaken ((Wlost + self.W), appl.noOfNodesUsed (Nw - j + Ns + Nr))

        overhead2 = i * (self.reschTime + self.recoverTime)

        EnxtMigration += self.nCr (Nfm, i) * pow (P, i) * pow ((1 - P), (Nfm - i)) \
                         * (self.cpTime + self.migTime + I + overhead2 + overhead1)

      EnxtMigration += pow ((1 - P), Nfm) * (self.cpTime + self.migTime + I)

    # RESCHEDULE #
  
    if appl.isMalleable :

      EnxtReschedule = self.cpTime + self.reschTime + self.recoverTime \
                       + appl.timeTaken (self.W, appl.noOfNodesUsed (Nw - Nf + Ns))
    else :
      EnxtReschedule = 9999999999
   
    #print "%.2f %.2f %.2f %.2f" % (EnxtSkip, EnxtCheckpoint, EnxtMigration, EnxtReschedule)
 
    if EnxtReschedule < EnxtSkip and EnxtReschedule < EnxtMigration and EnxtReschedule < EnxtCheckpoint :
      self.proactiveReschedule (pastTime, system, nodeFailures)

      if Nf :
        self.noOfProactiveResch += 1
        Action = "Pro. Resch."
     
      else :
        self.noOfOppResch += 1
        Action = "Opp. Resch."
      
    elif EnxtCheckpoint < EnxtSkip and EnxtCheckpoint < EnxtMigration :
      self.proactiveCP (pastTime, appl)
      #self.noOfProactiveCP += 1
      Action = "Checkpoint"
      
    elif EnxtMigration < EnxtSkip :
      self.proactiveMigration (pastTime, system, nodeFailures)
      self.noOfProactiveMig += 1
      returnVal = 1
      Action = "Migrate"

    else :

      returnVal = 1
      Action = "No Action"
   
      if Nf :
        self.noOfProactiveSkip += 1
        Action = "Skip"

    """self.NfNsfile.write ("%d\t%d\t%d\t%s\n" % (self.APindex, Ns, Nf, Action))

    if Nf :
      self.costFilePro.write ("%d\t%.2f\t%.2f\t%.2f\t%.2f\t%s\n" % (self.APindex, EnxtSkip, EnxtCheckpoint, \
                                                                EnxtMigration, EnxtReschedule, Action))

    else :
      self.costFileOpp.write ("%d\t%.2f\t%.2f\t%s\n" % (self.APindex, EnxtSkip, EnxtReschedule, Action))"""

    return returnVal

  ############################################################################################################################ 

  def model_2 (self, pastTime, system, I) :

    nodeFailures = list (self.predictor.nodeFailures)

    P = self.predictor.F_precision
    appl = system.application
    returnVal = 0

    Nf = len ([node for node in nodeFailures if node.application == appl])
    
    Ns = len ([node for node in system.spareNodes if node not in nodeFailures])

    if self.useRecoveryPrediction :
      nodeRecoveries = list (self.predictor.nodeRecoveries)
      Nr = len ([node for node in nodeRecoveries if node not in nodeFailures and node not in system.spareNodes \
                                                                             and node.application == None])

    else :
      Nr = 0

    #print Nr

    Nw = appl.nodesApp

    timeSinceLastCP = appl.timeSinceLastCP
    Wlost = appl.workDoneSinceLastCP
      
    EnxtSkip = EnxtCheckpoint = EnxtMigration = EnxtReschedule = 0
      
    # SKIP #

    for i in range (1, Nf + 1) :
      
      overhead1 = 0
      for j in range (1, i + 1) :
        overhead1 += appl.timeTaken ((Wlost + self.W), appl.noOfNodesUsed (Nw - j + Ns + Nr))

      overhead2 = i * (self.reschTime + self.recoverTime)

      EnxtSkip += self.nCr (Nf, i) * pow (P, i) * pow ((1 - P), (Nf - i)) \
                  * (I + overhead2 + overhead1) 
        
    EnxtSkip += pow ((1 - P), Nf) * I

    # CHECKPOINT #

    for i in range (1, Nf + 1) :

      overhead1 = 0
      for j in range (1, i + 1) :
        overhead1 += appl.timeTaken (self.W, appl.noOfNodesUsed (Nw - j + Ns + Nr))

      overhead2 = i * (self.reschTime + self.recoverTime)

      EnxtCheckpoint += self.nCr (Nf, i) * pow (P, i) * pow ((1 - P), (Nf - i)) \
                        * (self.cpTime + I + overhead2 + overhead1)
             
    EnxtCheckpoint += pow ((1 - P), Nf) * (self.cpTime + I)

    # MIGRATION #

    if Nf <= Ns :
      EnxtMigration = self.migTime + I

    else :
      
      Nfm = Nf - Ns

      for i in range (1, Nfm + 1) :

        overhead1 = 0
        for j in range (1, i + 1) :
          overhead1 += appl.timeTaken ((Wlost + self.W), appl.noOfNodesUsed (Nw - j + Ns + Nr))

        overhead2 = i * (self.reschTime + self.recoverTime)

        EnxtMigration += self.nCr (Nfm, i) * pow (P, i) * pow ((1 - P), (Nfm - i)) \
                         * (self.cpTime + self.migTime + I + overhead2 + overhead1)

      EnxtMigration += pow ((1 - P), Nfm) * (self.cpTime + self.migTime + I)

    # RESCHEDULE #
  
    if appl.isMalleable :

      EnxtReschedule = self.cpTime + self.reschTime + self.recoverTime \
                       + appl.timeTaken (self.W, appl.noOfNodesUsed (Nw - Nf + Ns))
    else :
      EnxtReschedule = 9999999999
   
    #print "%.2f %.2f %.2f %.2f" % (EnxtSkip, EnxtCheckpoint, EnxtMigration, EnxtReschedule)
 
    if EnxtReschedule < EnxtSkip and EnxtReschedule < EnxtMigration and EnxtReschedule < EnxtCheckpoint :
      self.proactiveReschedule (pastTime, system, nodeFailures)

      if Nf :
        self.noOfProactiveResch += 1
        Action = "Pro. Resch."
     
      else :
        self.noOfOppResch += 1
        Action = "Opp. Resch."
      
    elif EnxtCheckpoint < EnxtSkip and EnxtCheckpoint < EnxtMigration :
      self.proactiveCP (pastTime, appl)
      #self.noOfProactiveCP += 1
      Action = "Checkpoint"
      
    elif EnxtMigration < EnxtSkip :
      self.proactiveMigration (pastTime, system, nodeFailures)
      self.noOfProactiveMig += 1
      returnVal = 1
      Action = "Migrate"

    else :

      returnVal = 1
      Action = "No Action"
   
      if Nf :
        self.noOfProactiveSkip += 1
        Action = "Skip"

    """self.NfNsfile.write ("%d\t%d\t%d\t%s\n" % (self.APindex, Ns, Nf, Action))

    if Nf :
      self.costFilePro.write ("%d\t%.2f\t%.2f\t%.2f\t%.2f\t%s\n" % (self.APindex, EnxtSkip, EnxtCheckpoint, \
                                                                EnxtMigration, EnxtReschedule, Action))

    else :
      self.costFileOpp.write ("%d\t%.2f\t%.2f\t%s\n" % (self.APindex, EnxtSkip, EnxtReschedule, Action))"""

    return returnVal

  ############################################################################################################################

  def nCr (self, n,r) :

    f = math.factorial
    return f(n) / f(r) / f(n-r)
   
##############################################################################################################################
# END OF MODULE 	                      										     #
##############################################################################################################################

