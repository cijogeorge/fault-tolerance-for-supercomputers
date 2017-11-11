
##############################################################################################################################
# MODULE FOR CLASSES THAT DEFINE AN HPC SYSTEM                                                                               #
##############################################################################################################################

# Defines a Node with its different attributes #
class nodeClass :
  
  def __init__ (self, n, sys, simStart) :
    """ Init function for class nodeClass. """
    self.number = n
    self.sysNo = sys
    self.avgTTF = 9999999999
    self.lastFailureTime = simStart
    self.timeToNextFailure = self.avgTTF
    self.down = False
    self.application = None
    # REPLICATION BEGIN
    self.replica = None
    # REPLICATION END

  ############################################################################################################################
  
  def nodeDown (self, time) :
    """ Function to set the node as down. """
    self.down = True
    
    # REPLICATION BEGIN
    if self.replica :
      self.replica.replica = None 
      self.application.nodesRepCurrent -= 1 
    # REPLICATION END
    
    if self.avgTTF == 9999999999 :
      self.avgTTF = time - self.lastFailureTime
     
    else :
      self.avgTTF = (self.avgTTF + (time - self.lastFailureTime)) / 2

    self.lastFailureTime = time
    self.timeToNextFailure = self.avgTTF
      
  ############################################################################################################################

  def nodeUp (self) :
    """ Function to set the node as up. """
    self.down = False
 
  ############################################################################################################################

  def updateTTF (self, pastTime) :
    """ Function to update TTF of a node. """
    if self.timeToNextFailure > 0 :
      self.timeToNextFailure -= pastTime

##############################################################################################################################

# Defines a System that consists of a number of  Nodes #
class systemClass:

  def __init__ (self, nodes_n, simStart) :
    """ Init function for class systenClass. """
    self.nodeList = [nodeClass (n, self, simStart) for n in range (nodes_n)]
    self.application = None
    self.downNodes = []
    self.spareNodes = list (self.nodeList)

  ############################################################################################################################

  def updateNodeTTF (self, pastTime) :
    """ Function to update TTF of all the nodes in the system. """
    for node in self.nodeList :
      if node not in self.downNodes :
        node.updateTTF (pastTime)

  ############################################################################################################################

  def startAppl (self, appl) :
    """ Function to start a given application in a set of nodes. """
    
    appl.workingNodes = []
 
    if appl.nodesApp + appl.nodesRep  > len (self.spareNodes) :
      print "startAppl: Not enough spare nodes available!"
      return

    else :

      nodesApp = appl.nodesApp
   
      while nodesApp > 0 :
      
        node = self.spareNodes.pop (0)      
        node.application = appl
        appl.workingNodes.append (node)
        nodesApp -= 1

        # REPLICATION BEGIN

        if appl.nodesRepCurrent < appl.nodesRep :

          repNode = self.spareNodes.pop (nodesApp)
          repNode.application = appl
          appl.workingNodes.append (repNode)
          appl.repNodesList.append (repNode)

          node.replica = repNode
          repNode.replica = node

          appl.nodesRepCurrent += 1

        #nodesApp -= 1

        # REPLICATION END  

        if nodesApp == 0 :
          break

      appl.applStatus = "Running"
      self.application = appl

    #print "StartAppl"
    #print "Working Nodes:", len (appl.workingNodes)
    #print "Reps:", appl.nodesRepCurrent
    #for node in appl.workingNodes :
    #  if node.replica and node.replica.replica != node :
    #    print "startAPPL"
    #    break


  ############################################################################################################################

  def recoverAppl (self, appl, faultTolerance) :
    """ Function to recover an application after it fails due to node failure. """

    if appl.isMalleable :
      
      newNodes = list (appl.workingNodes)

      for node in self.spareNodes :
        if node not in faultTolerance.predictor.nodeFailures :
          newNodes.append (node) 

      self.spareNodes = []
 
      if appl.nodesApp < appl.noOfNodesUsed (len (newNodes)) :
        faultTolerance.noOfReacReschUp += 1
   
      elif appl.nodesApp > appl.noOfNodesUsed (len (newNodes)) :
        faultTolerance.noOfReacReschDown += 1

      faultTolerance.reschedule (0, appl, newNodes)

      appl.totalWorkDone -= appl.workDoneSinceLastCP
      appl.workDoneSinceLastCP = 0
      appl.timeSinceLastCP = 0

      appl.applStatus = "Running"

    else :

      #for node in appl.workingNodes :
      #  if node.replica and node.replica.replica != node :
      #    print "HELL 1"
     
      #print len (appl.workingNodes) 
      extraNodesReq = appl.nodesApp + appl.nodesRep - len (appl.workingNodes)
      extraAppNodes = extraNodesReq - (appl.nodesRep - appl.nodesRepCurrent)

      if extraNodesReq > 0 :
      
        if len (self.spareNodes) >= extraAppNodes :

          while extraAppNodes > 0 :
            
            node = self.spareNodes.pop (0)
            
            if node not in appl.workingNodes :
              appl.workingNodes.append (node)
              node.application = appl

            else :
              print node.number, "recoverAppl: node is present appl.workingNodes, but it should not be. Something fishy!"

            extraAppNodes -= 1
      
          # REPLICATION BEGIN

          if appl.nodesRepCurrent < appl.nodesRep and len (self.spareNodes) > 0 :
            
            for node in appl.workingNodes :
            
              if not node.replica :

                repNode = self.spareNodes.pop (0)

                if repNode not in appl.workingNodes :

                  repNode.application = appl
                  node.replica = repNode
                  repNode.replica = node
                  appl.workingNodes.append (repNode)
                  appl.repNodesList.append (repNode)
                  appl.nodesRepCurrent += 1

                else :
                  print node.number, "recoverAppl: node is present appl.workingNodes, but it should not be. Something fishy!"

                if appl.nodesRepCurrent == appl.nodesRep or not self.spareNodes :
                  break

            else :
              print "recoverAppl: Node enough spare nodes for replication!"

          # REPLICATION END  
      
          appl.totalWorkDone -= appl.workDoneSinceLastCP + appl.workDoneReal (faultTolerance.recoverTime, appl.nodesApp)
          appl.totalWorkLost += appl.workDoneSinceLastCP
          #print "Time lost due to Rollback: ", appl.timeTaken (appl.workDoneSinceLastCP, appl.nodesApp)
          appl.workDoneSinceLastCP = 0
          appl.timeSinceLastCP = 0
      
          appl.applStatus = "Running"
          #faultTolerance.adaptiveRep (0, self)

          #for node in appl.workingNodes :
          #  if node.replica and node.replica.replica != node :
          #    print "HELL 2"

 
          #print "RecoverAppl"
          #print "Working Nodes:", len (appl.workingNodes)
          #print "Reps:", appl.nodesRepCurrent
         
      else :
        print "recoverAppl: Seems No Extra Nodes are required for the application. Something fishy!"

##############################################################################################################################
# END OF MODULE                                                                                                              #
##############################################################################################################################

