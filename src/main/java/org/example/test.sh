#!/bin/bash
for i in {1..10}
do
   gnome-terminal -- bash -c "java -jar /home/al-sharif-mohamed/IdeaProjects/DataBaseManagement/out/artifacts/DataBaseManagement_jar/DataBaseManagement.jar $i 10 20 3 10 10 2 2; exec bash"
done
