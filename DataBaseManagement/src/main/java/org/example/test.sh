#!/bin/bash
for i in {1..5}
do
   gnome-terminal -- bash -c "java -jar /home/elemary/Projects/DataBaseManagement/out/artifacts/DataBaseManagement_jar/DataBaseManagement.jar $i 5 20 3 10 10 2 2 1; exec bash"
done
