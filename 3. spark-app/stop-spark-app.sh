#!/bin/bash

cd $SPARK_HOME

eval $(minikube -p minikube docker-env)

K8S_SERVER=$(kubectl config view --output=jsonpath='{.clusters[].cluster.server}')

./bin/spark-submit \
  --master k8s://$K8S_SERVER \
  --kill pyspark:pyspark-driver