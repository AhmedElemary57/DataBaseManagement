#!/bin/bash
for i in {1..5}
do
   gnome-terminal -- bash -c "java -jar /home/al-sharif-mohamed/IdeaProjects/DataBaseManagement/out/artifacts/DataBaseManagement_jar/DataBaseManagement.jar $i 5 20 3 10 5 2 4; exec bash"
done