#!/bin/bash
for i in {1..5}
do
   gnome-terminal -- bash -c "java -jar ./out/artifacts/DataBaseManagement_jar/Zarka-DataBase-System.jar $i 5 20 3 10 5 2 2 1 0; exec bash"
done
