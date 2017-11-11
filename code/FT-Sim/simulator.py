
##############################################################################################################################
# MAIN APPLICATION EXECUTION SIMULATOR                                                                                       #
##############################################################################################################################

import sys
import math

# CONFIGURATION ##############################################################################################################

TRACE_FILE = sys.argv [1]
SIM_START = 3600 * 24 * 30 * 6
SIM_END = SIM_START + (3600 * 24 * 7)
NODES_REQUESTED = int (sys.argv [2])
NUMBER_OF_SPARES = int (0.01 * NODES_REQUESTED)
NUMBER_OF_NODES = NODES_REQUESTED + NUMBER_OF_SPARES
PROACTIVE_ACTION = 6
LB = 0.01
UB = NODES_REQUESTED
OUTPUT_FILE = sys.argv [5]

# FLAGS ---------------------------------------------------------------------------------------------------------------------#

PERIODIC_CP_ENABLED = 1

REPLICATION_ENABLED = 1

if REPLICATION_ENABLED == 1 :
  NUMBER_OF_REPLICAS = int (LB * (NODES_REQUESTED)) #(NODES_REQUESTED / 40) * int (sys.argv [3])

else :
  NUMBER_OF_REPLICAS = 0

if NUMBER_OF_REPLICAS > NODES_REQUESTED / 2 or NUMBER_OF_REPLICAS < 0 :
  print "Invalid Number of Replicas!"
  print "Setting it as (total no. of nodes used for execution) / 2."
  NUMBER_OF_REPLICAS = int (NODES_REQUESTED / 2)

elif NUMBER_OF_REPLICAS == 0 :
  REPLICATION_ENABLED = 0

AUTO_REPLICATION_ENABLED = 0

PROACTIVE_FT_ENABLED = 1
USE_RECOVERY_PREDICTION = 0
RESCHEDULE_ON_RECOVERY = 0
OPP_RESCH_ENABLED = 0
CONSIDER_SCALE_CURVE = 1
CONSIDER_TTF = 0

# PREDICTOR PARAMETERS ------------------------------------------------------------------------------------------------------#

F_PREDICTOR_TRACE = sys.argv [3]
F_PREDICTOR_PRECISION = 0.7
F_PREDICTOR_RECALL = 0.7
R_PREDICTOR_TRACE = None
R_PREDICTOR_PRECISION = 0
R_PREDICTOR_RECALL = 0

# APPLICATION PARAMETERS ----------------------------------------------------------------------------------------------------#

MALLEABILITY = 0
WORK_UNIT = 1
A = NODES_REQUESTED
S = 0
SCALE_DEGRADE_FACTOR = 0
SCALE_CURVE_TYPE = 2
W_SCALE_FACTOR = 3600 * 0.5
SCALE_CURVE_FROM_FILE = 0
SCALE_CURVE_FILE = "namd_8388608"

# COST PARAMETERS -----------------------------------------------------------------------------------------------------------#

CP_BANDWIDTH = 1 # TB/s
MEMORY_PER_NODE = 16 # GB
CP_TIME = 5 * 60 #(float (MEMORY_PER_NODE) / (CP_BANDWIDTH * 1024)) * (NODES_REQUESTED - NUMBER_OF_REPLICAS)
RESCH_TIME = 3 * 60
RECOVERY_TIME = CP_TIME
MIG_TIME = 0.33 * 60

if REPLICATION_ENABLED :
  rMPI_OVERHEAD = 0#(math.log ((NODES_REQUESTED / 2) * 2) / 10) + 3.67 # Formula from SC'11 paper on Replication
else :
  rMPI_OVERHEAD = 0

# OTHER PARAMETERS ----------------------------------------------------------------------------------------------------------#

MTBF = int (3600 * 24 * 365 * 12)
cpIntRecall = 0 #float (sys.argv [8]) * 3600
mttiRecall = 0

# ---------------------------------------------------------------------------------------------------------------------------#

def MTTI_Rep (repEnabled, nodesReq, nodesRep, mtbf) :

  if repEnabled :
    F = pow (float ((22/7) * nodesRep) / 2, 0.5) + (2/3)

    if nodesRep < nodesReq / 2 :

      mtti_1 = (float (mtbf) / (nodesRep * 2)) * F
      mtti_2 = float (mtbf) / (nodesReq - 2 * nodesRep)

      #if mtti_1 < mtti_2 :
      #  mtti = mtti_1

      #else :
      #  mtti = mtti_2

      mtti = (mtti_1 * mtti_2) / (mtti_1 + mtti_2)

    else :
      mtti = mtbf / ((nodesRep * 2)) * F

  else :
    mtti = float (mtbf) / nodesReq

  return mtti

# ---------------------------------------------------------------------------------------------------------------------------#

MTTI = int (float (sys.argv [4]) * 3600) #MTTI_Rep (REPLICATION_ENABLED, NODES_REQUESTED, NUMBER_OF_REPLICAS, MTBF)
MTTI = MTTI / (1 - F_PREDICTOR_RECALL)

if CP_TIME < 2 * MTTI :

  CP_INTERVAL = pow (2 * CP_TIME * MTTI, 0.5) \
              * (1 + ((1/3) * pow (CP_TIME / (2 * MTTI), 0.5)) + ((1/9) * (CP_TIME / (2 * MTTI)))) - CP_TIME
else :

  CP_INTERVAL = MTTI

if cpIntRecall :
  CP_INTERVAL = cpIntRecall
else :
  cpIntRecall = CP_INTERVAL

mttiRecall = MTTI

# IMPORT MODULES #############################################################################################################

import failurePredictorSim
import systemSim
import faultToleranceSim
import applicationSim
  
# FUNCTIONS ##################################################################################################################

def readNextTraceLine (traceHandle) :
  """ Function to read the next line from the trace. """
  traceLine = []
  nextTraceLine = traceHandle.readline ()

  if not nextTraceLine :
    return traceLine

  nextTraceLine = nextTraceLine.strip ()
  traceLine = [int (d) for d in nextTraceLine.split ("\t")]
  return traceLine

# MAIN #######################################################################################################################

if __name__ == "__main__":

  opfile = open (OUTPUT_FILE, 'a')

  if SCALE_CURVE_FROM_FILE == 1  :
    scaleFileHandle = open (SCALE_CURVE_FILE, 'r')

  else :
    scaleFileHandle = None
 
  system = systemSim.systemClass (NUMBER_OF_NODES, SIM_START)
  predictor = failurePredictorSim.predictorClass (TRACE_FILE, F_PREDICTOR_TRACE, R_PREDICTOR_TRACE, NUMBER_OF_NODES, \
                                                  F_PREDICTOR_PRECISION, R_PREDICTOR_PRECISION, F_PREDICTOR_RECALL, \
                                                  R_PREDICTOR_RECALL, SIM_START)
  application = applicationSim.applicationClass (system, NODES_REQUESTED, NUMBER_OF_REPLICAS, MALLEABILITY, WORK_UNIT, \
                                                 CONSIDER_SCALE_CURVE, SCALE_CURVE_FROM_FILE, scaleFileHandle, \
                                                 SCALE_CURVE_TYPE, A, S, SCALE_DEGRADE_FACTOR)

  # -------------------------------------------------------------------------------------------------------------------------#

  # Test code - not getting satisfactory results 
  if AUTO_REPLICATION_ENABLED :

    #x = NUMBER_OF_REPLICAS

    T = SIM_END - SIM_START
    cpTime = CP_TIME

    wDoneMaxFinal = 0
    cpIntervalOptFinal = T
    nCPOptFinal = 0
    nodesRepFinal = 0
  
    x = int (NODES_REQUESTED / 2)

    while x >= 1000 :

    #if x == NUMBER_OF_REPLICAS :

      #print x
      if x == 0 :
        repEnabled = 0
      else :
        repEnabled = 1

      appNodes = NODES_REQUESTED - x
      mtti = MTTI_Rep (repEnabled, NODES_REQUESTED, x, MTBF)
      mtti = mtti / (1 - F_PREDICTOR_RECALL)

      cpInterval = 300
      wDoneMax = 0
      cpIntervalOpt = T
      nCPOpt = 0

      while cpInterval < T/2 :

        nF = int (float (T) / mtti)
        nCP = int (float (T - (nF * (float (cpInterval)/2))) / (cpInterval + cpTime))
   
        if nF == 0 :
          nF = 1

        wDone = application.workDoneReal (T - nCP * cpTime - nF * ((float (cpInterval)/2) + RECOVERY_TIME), appNodes)

        if wDone > wDoneMax :
          wDoneMax = wDone
          cpIntervalOpt = cpInterval
          nCPOpt = nCP
 
        cpInterval += 300
      
      print wDoneMax
      if wDoneMax > wDoneMaxFinal :
        wDoneMaxFinal = wDoneMax
        cpIntervalOptFinal = cpIntervalOpt
        nCPOptFinal = nCPOpt
        nodesRepFinal = x

      x -= 1000

    print "CP Interval Optimal:", float (cpIntervalOptFinal) / 3600
    print "No. of CP Optimal:", nCPOptFinal
    print "No. of CP Optimal:", nodesRepFinal
 
    sys.exit (0)

  # -------------------------------------------------------------------------------------------------------------------------#

  W = WORK_UNIT * application.noOfNodesUsed (NODES_REQUESTED) * W_SCALE_FACTOR

  faultTolerance = faultToleranceSim.faultToleranceClass (W, MTBF, PROACTIVE_ACTION, REPLICATION_ENABLED, CP_INTERVAL, predictor, \
                                                          USE_RECOVERY_PREDICTION, RESCHEDULE_ON_RECOVERY, CONSIDER_TTF, \
                                                          CP_TIME, MIG_TIME, RESCH_TIME, RECOVERY_TIME, LB, UB)


  # Open log files #

  """if PROACTIVE_ACTION == 3 :
    faultTolerance.NfNsfile = open ('3_NsNf', 'a')

    faultTolerance.costFilePro = open ("3_CostPro", 'a')

  elif PROACTIVE_ACTION == 4 :
    faultTolerance.NfNsfile = open ('4_NsNf', 'a')
    faultTolerance.costFilePro = open ("4_CostPro", 'a')
    faultTolerance.costFileOpp = open ("4_CostOpp", 'a')

  elif PROACTIVE_ACTION == 5 :
    faultTolerance.NfNsfile = open ('5_NsNf', 'a')
    faultTolerance.costFilePro = open ("5_CostPro", 'a')
    faultTolerance.costFileOpp = open ("5_CostOpp", 'a')"""

  # Start Application #
  system.startAppl (application)

  simTraceHandle = open (TRACE_FILE, "r")
  traceLine = []
  prevTime = SIM_START

  totalTime = SIM_END - SIM_START
  statusBarInc = totalTime / 50
  statusBarPoint = totalTime / 100

  traceLine = readNextTraceLine (simTraceHandle)

  print "\n"

  flag = 0

  while traceLine :

    #for node in application.workingNodes :
    #  if node.replica and node.replica.replica != node :
    #    print "SIM1", traceLine [2]
    #    break


    #print "TRACE LINE: ", traceLine [0], traceLine [1], traceLine [2]

    system.updateNodeTTF (traceLine [2] - prevTime)
    application.updateApplStats (traceLine [2] - prevTime)
    faultTolerance.currentTime = traceLine [2] 

    if PERIODIC_CP_ENABLED :  

      if application.timeSinceLastCP >= faultTolerance.cpInterval + CP_TIME :
        n = int (application.timeSinceLastCP / (faultTolerance.cpInterval + CP_TIME))
        faultTolerance.periodicCP (application, n)
        faultTolerance.noOfPerCP += n

    if PROACTIVE_FT_ENABLED :
   
      # flag = 1 for beginning of execution, to consider SIM_START as an AP
      if not flag and traceLine [2] - application.timeTaken (W, application.nodesApp) < SIM_START :
        flag = 1

      else :
        flag = 0

      if int (application.workDoneSinceLastAP / W) >= 1 or flag :
   
        if not flag : 
          i = int (application.workDoneSinceLastAP / W)
     
        else :
          i = 0

        pastTime = application.timeTaken (i * W, application.nodesApp) \
                   + application.timeTaken (application.workDoneSinceLastAP % W, application.nodesApp)
        timeOfAP = traceLine [2] - pastTime

        while traceLine [2] >= timeOfAP :
          interval = application.timeTaken (W, application.nodesApp)
          predictor.invokePredictor (system, timeOfAP, interval * 2)

          #for node in predictor.nodeFailures :
            #print node.number
          
          faultTolerance.APindex += 1

          if (PROACTIVE_ACTION == 3 or not OPP_RESCH_ENABLED) and not predictor.nodeFailures :

            returnVal = 1

            """if PROACTIVE_ACTION == 3 :
              faultTolerance.NfNsfile.write ("%d\t%d\t0\tNo Action\n" % (faultTolerance.APindex, NUMBER_OF_SPARES))
            
            elif not OPP_RESCH_ENABLED :

              faultTolerance.NfNsfile.write ("%d\t%d\t0\tNo Action\n" % (faultTolerance.APindex, len (system.spareNodes)))"""

          else :
            returnVal = faultTolerance.proactiveAction (pastTime, system)
         
          """if W and F_PREDICTOR_RECALL < 1 :

            MTTI=MTTI_Rep (faultTolerance.repEnabled, application.nodesApp + application.nodesRep, application.nodesRep, MTBF)

            efMTTI = MTTI / (1 - F_PREDICTOR_RECALL)

            if CP_TIME < 2 * efMTTI :
              cpInt = pow (2 * CP_TIME * efMTTI, 0.5) \
                         * (1 + ((1/3) * pow (CP_TIME / (2 * efMTTI), 0.5)) + ((1/9) * (CP_TIME / (2 * efMTTI)))) - CP_TIME
            else :
              cpInt = efMTTI

            mttiRecall = efMTTI

            if cpIntRecall :
              cpInt = cpIntRecall

            else :
              cpIntRecall = cpInt
            #print cpIntRecall / 3600
            #cpInt = 0.66 * 3600

            if application.timeSinceLastCP - pastTime >= cpInt and pastTime > CP_TIME :
              faultTolerance.proactiveCP (pastTime, application)
              faultTolerance.noOfPerCP += 1"""

          prevTime = timeOfAP
          timeOfAP += application.timeTaken (W, application.nodesApp)
          pastTime = traceLine [2] - timeOfAP
      
        application.workDoneSinceLastAP = application.workDoneReal ((traceLine [2] - timeOfAP), application.nodesApp)
     
    failedNodes = []
    recoveredNodes = []
    prevTime = traceLine [2]
 
    while traceLine and traceLine [0] == 0 and prevTime == traceLine [2] :

      if system.nodeList [traceLine [1]].down == False : 
        failedNodes.append (system.nodeList [traceLine [1]])

      prevTime = traceLine [2]
      traceLine = readNextTraceLine (simTraceHandle)

    failedNodes = list (set (failedNodes))

    if failedNodes :
      faultTolerance.nodeFailureAction (prevTime, system, failedNodes)
      
    while traceLine and traceLine [0] == 1 and prevTime == traceLine [2] :
     
      if system.nodeList [traceLine [1]].down == True :
        recoveredNodes.append (system.nodeList [traceLine [1]])

      prevTime = traceLine [2]
      traceLine = readNextTraceLine (simTraceHandle)

    recoveredNodes = list (set (recoveredNodes))

    if recoveredNodes :
      faultTolerance.nodeRecoveryAction (prevTime, system, recoveredNodes)

    #print application.nodesApp, application.nodesRep
    
    i = prevTime - SIM_START

    sys.stdout.write ("\r|" + "=" * (i/ statusBarInc) + \
                      " " * ((totalTime - i)/ statusBarInc) + "| " + str (i / statusBarPoint) + " %\r")
    sys.stdout.flush ()
 
    #workDonePerUnitTime = float (application.totalWorkDone) / i
    #rMPIoverHead = (float (rMPI_OVERHEAD) * workDonePerUnitTime) / 100
    #workDonePerUnitTime = workDonePerUnitTime - rMPIoverHead
    #efficiency = (float (workDonePerUnitTime) * 100) / application.workDoneReal (1, NODES_REQUESTED)
    #print "%.2f\n" % (efficiency)
 
    #print application.nodesApp, application.nodesRep, len (application.workingNodes), \
    #      len (system.spareNodes), len (system.downNodes), system.application.applStatus

  sys.stdout.write ("\r|" + "=" * (i/ statusBarInc) + \
                    " " * ((totalTime - i)/ statusBarInc) + "| DONE!\n")
  sys.stdout.flush ()

  if PROACTIVE_FT_ENABLED and not F_PREDICTOR_TRACE :

    observedPrecision = float (predictor.F_correctPredictions) / predictor.F_totalPredictions
    observedRecall = float (predictor.predictedFailures) / predictor.totalFailures
    print "\nObserved Predictor Precision: %.3f" % observedPrecision
    print "Observed Predictor Recall: %.3f" % observedRecall

  elif PROACTIVE_FT_ENABLED :
    print "\nPredictor Precision: %f" % F_PREDICTOR_PRECISION
    print "Predictor Recall: %f" % F_PREDICTOR_RECALL

  #print "\nStatus of application now:", application.applStatus

  print "\nNo. of Pro. SKip:\t", faultTolerance.noOfProactiveSkip, \
        "\nNo. of Pro. CP:\t\t", faultTolerance.noOfProactiveCP, \
        "\nNo. of Pro. Mig:\t", faultTolerance.noOfProactiveMig, \
        "\nNo. of Pro. Resch:\t", faultTolerance.noOfProactiveResch, \
        "\nNo. of Swaps:\t", faultTolerance.noOfSwaps, \
        "\nNo. of Rep Changes:\t", faultTolerance.noOfRepChanges, \
        "\nNo. of Adaptations:\t", faultTolerance.noOfAdapts

  print "\nNo. of Replicas: ", NUMBER_OF_REPLICAS
  print "MTTI: ", mttiRecall/3600
  print "CP Interval: ", cpIntRecall/3600
  print "No. of Pre. CP: ", faultTolerance.noOfPerCP
  print "No. of App. failures : ", application.stopCount
  #      "\nNo. of Reac. Resch. Up : ", faultTolerance.noOfReacReschUp, \
  #      "\nNo. of Reac. Resch. Down : ", faultTolerance.noOfReacReschDown

  #print "\nNo. of Opp. Resch:\t",  faultTolerance.noOfOppResch

  workDonePerUnitTime = float (application.totalWorkDone) / (SIM_END - SIM_START)
  rMPIoverHead = (float (rMPI_OVERHEAD) * workDonePerUnitTime) / 100
  workDonePerUnitTime = workDonePerUnitTime - rMPIoverHead
  efficiency = (float (workDonePerUnitTime) * 100) / application.workDoneReal (1, NODES_REQUESTED)
  print "\nTotal Work Done per unit time: %f\n" %  workDonePerUnitTime
  print "\nEfficiency: %.2f\n" % efficiency

  cpLostTime = float (faultTolerance.noOfPerCP) * CP_TIME
  recLostTime = float (application.stopCount) * RECOVERY_TIME

  print "\nFailure Free Work Done : ", (application.workDoneReal (SIM_END - SIM_START, NODES_REQUESTED))/(SIM_END-SIM_START)
  print "\nRollback Loss:", float (application.totalWorkLost)/(SIM_END - SIM_START)
  print "\nCP Loss:", application.workDoneReal (cpLostTime, NODES_REQUESTED - NUMBER_OF_REPLICAS)/(SIM_END-SIM_START)
  print "\nRec. Loss:", application.workDoneReal (recLostTime, NODES_REQUESTED - NUMBER_OF_REPLICAS)/(SIM_END-SIM_START) 

  opfile.write ("%s\t%d\t%d\t%.2f\t%.2f\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n" % \
               (TRACE_FILE, NODES_REQUESTED, int (LB * NODES_REQUESTED), float (mttiRecall)/3600, float (cpIntRecall)/3600, \
                faultTolerance.noOfProactiveCP, \
                application.stopCount, float (application.totalWorkLost)/(SIM_END - SIM_START), \
                application.workDoneReal (cpLostTime, NODES_REQUESTED - NUMBER_OF_REPLICAS)/(SIM_END - SIM_START), \
                application.workDoneReal (recLostTime, NODES_REQUESTED - NUMBER_OF_REPLICAS)/(SIM_END - SIM_START), \
                workDonePerUnitTime, (float (workDonePerUnitTime) * 100) / application.workDoneReal (1, NODES_REQUESTED)))

  # CLOSE ALL OPEN FILES #

  opfile.close ()

  """if PROACTIVE_ACTION == 3 or PROACTIVE_ACTION == 4 or PROACTIVE_ACTION == 5 :
    faultTolerance.NfNsfile.close ()
    faultTolerance.costFilePro.close ()

  elif PROACTIVE_ACTION == 4 or PROACTIVE_ACTION == 5 :
    faultTolerance.costFileOpp.close ()"""

  simTraceHandle.close ()

  if F_PREDICTOR_TRACE :
    predictor.F_pTraceHandle.close ()

  else :
    predictor.F_fTraceHandle.close ()

  if R_PREDICTOR_TRACE :
    predictor.R_pTraceHandle.close ()
  
  else :
    predictor.R_fTraceHandle.close ()

##############################################################################################################################
# END OF SIMULATOR                                                                                                           #
##############################################################################################################################

