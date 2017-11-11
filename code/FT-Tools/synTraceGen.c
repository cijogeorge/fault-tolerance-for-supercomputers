/* PROGRAM TO GENERATE SYNTHETIC FAILURE TRACE BASED ON GIVEN INPUTS. */

#include <stdio.h>
#include <time.h>
#include <gsl/gsl_rng.h>
#include <gsl/gsl_randist.h>

#define N_NODES 200000
#define MAX_TIME 60 * 24 * 365 //min
#define FAILURE_DIST 1
#define MEAN_NODE_TTF 60 * 24 * 365 * 6.25 //min
#define MEAN_NODE_TTR 60 * 2 //min

int main (int argc, char *argv[])
{
  long node, timeCount;
  long x, timeToFailure, timeToRecover;
  FILE *fp = fopen ("temp", "w");
  time_t t;
  
  time (&t);
  srand (t);

  gsl_rng *fTimePtr, *rTimePtr;
  fTimePtr = gsl_rng_alloc (gsl_rng_default);
  gsl_rng_set (fTimePtr, rand ());
  rTimePtr = gsl_rng_alloc (gsl_rng_default);
  gsl_rng_set (rTimePtr, rand ());

  for (node=0; node<N_NODES; node++)
  {
    timeCount = 0;

    do
    {
      if (FAILURE_DIST == 1)
        timeToFailure = (long) gsl_ran_weibull (fTimePtr, MEAN_NODE_TTF/1.27, 0.7);

      else if (FAILURE_DIST == 2)
        timeToFailure = (long) gsl_ran_exponential (fTimePtr, MEAN_NODE_TTF);

      if (timeToFailure < 0)
      {
        timeToFailure = 0;
      }

      timeCount += timeToFailure;

      if (timeCount < MAX_TIME)
      {

        fprintf (fp, "0\t%ld\t%ld\n", node, timeCount * 60);
        
        do

        {
          timeToRecover = (long) (gsl_ran_lognormal (rTimePtr, 0, 0.5) * MEAN_NODE_TTR);
        }

        while (timeToRecover <= 0);

        timeCount += timeToRecover;
  
        if (timeCount < MAX_TIME)
          fprintf (fp, "1\t%ld\t%ld\n", node, timeCount * 60);
      }
      
    } 
    while (timeCount < MAX_TIME);

  }

  fclose (fp);
  
  system ("sort -n -k 3 temp > synTrace");
  system ("rm -f temp");
  
  return(0);
}
