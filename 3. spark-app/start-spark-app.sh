#!/bin/bash

cd $SPARK_HOME

eval $(minikube -p minikube docker-env)

K8S_SERVER=$(kubectl config view --output=jsonpath='{.clusters[].cluster.server}')

./bin/spark-submit \
  --master k8s://$K8S_SERVER \
  --deploy-mode cluster \
  --name twitter-spark-app \
  --conf spark.kubernetes.container.image=spark-app:1.0 \
  --conf spark.kubernetes.context=minikube \
  --conf spark.kubernetes.namespace=pyspark \
  --conf spark.kubernetes.driver.pod.name=pyspark-driver \
  --conf spark.executor.instances=1 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=pyspark-service \
  --conf spark.kubernetes.file.upload.path=/tmp \
  --conf spark.kubernetes.submission.waitAppCompletion=false \
  local://${SPARK_HOME}/twitter-spark-app/spark-app.py