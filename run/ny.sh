#!/bin/zsh
para_file='./ny.yaml'
jar_file='../geoburst/GeoBurst.jar'
java -jar -Xmx30G $jar_file $para_file
