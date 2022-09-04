#!/bin/bash
for i in {1..6}
do
   gnome-terminal -- bash -c "java -jar /home/elemary/Projects/DataBaseManagement/out/artifacts/DataBaseManagement_jar/DataBaseManagement.jar $i 6 20 3 10 10 2 2 1; exec bash"
done
