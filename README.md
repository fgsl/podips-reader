# podips-reader

This program reads logs about changes of pods in a Kubernetes cluster and send the data to a queue.

This program was made to be used with **podips-writer** and **podips-monitor**.

This program was made to run inside a Kubernetes pod. Use a Dockerfile to create the pod image. 
