#!/bin/zsh

para_file='./sample.yaml'
jar_file='../geoburst/GeoBurst.jar'
java -jar -Xmx10G $jar_file $para_file
