#!/bin/zsh

para_file='./la.yaml'
jar_file='../geoburst/GeoBurst.jar'
java -jar -Xmx30G $jar_file $para_file
