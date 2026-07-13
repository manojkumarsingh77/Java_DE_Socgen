#!/bin/bash
# Compile + run script (Java 11+)
set -e
mkdir -p out
find src -name "*.java" > sources.txt
javac --release 11 -d out @sources.txt
java -Xmx2g -cp out com.omnibank.TrainingMain
