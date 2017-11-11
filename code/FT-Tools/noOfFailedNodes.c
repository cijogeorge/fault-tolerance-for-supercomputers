/* Use of this program: 
   
   If x is the number of nodes in the system which are DOWN at a given point of time, then
   this program finds the maximum value that x had taken during the life time of the system.
   It also gives the average of x during the life time of the system.

   This can be used to select an optimal value for the number of spare nodes in the system
   for proactive migration technique and similar techniques */

#include <stdio.h>

#define N 8388608 // total number of nodes

/* Function to read the next line from the trace file */
int readNxtLine (FILE *fp, int trace [])
{
  int i;
  char buf [10];

  if (fscanf (fp, "%s", buf) == EOF)
    return -1;

  trace [0] = atoi (buf);

  for (i=1; i<3; i++)
  {
    if (fscanf (fp, "%s", buf) == EOF)
     return -1;

    trace [i] = atoi (buf);
  }

  return 0;
}

void main ()
{
  int i, trace [3], count=0, totalCount=0, n=0, maxCount=0, nodes [N];

  FILE *fp = fopen ("synTrace_8388608", "r");
   
  for (i=0; i<N; i++)
    nodes [i] = 1;

  while (!readNxtLine (fp, trace))
  {
    n++;

    if (trace [0] == 0)
      nodes [trace [1]] = 0;
  
    else if (trace [0] == 1)
      nodes [trace [1]] = 1;

    count = 0;

    for (i=0; i<N; i++)
    {
      if (nodes [i] == 0)
        count ++;
    }
       
    if (maxCount < count)
      maxCount = count;

    totalCount += count;  
  }
  
  printf ("Max: %d\n", maxCount);
  printf ("Avg: %f\n", (float) totalCount/n);
  
  fclose (fp);
}
 
