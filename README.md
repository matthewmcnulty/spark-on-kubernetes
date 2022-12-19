# APACHE-SPARK-STREAMING-TWITTER-1

minikube start

minikube dashboard

cd $SPARK_HOME

eval $(minikube -p minikube docker-env)

./bin/docker-image-tool.sh \
  -m \
  -t v3.3.1 \
  -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile \
  build

cd twitter-app
docker build -t twitter-app:1.0 .

cd spark-app
docker build -t spark-app:1.0 .

cd streamlit-app
docker build -t streamlit-app:1.0 .

kubectl create ns pyspark

kubectl config set-context --current --namespace=pyspark

kubectl create serviceaccount pyspark-service \
  -n pyspark

kubectl create clusterrolebinding pyspark-clusterrole \
  --clusterrole=edit \
  --serviceaccount=pyspark:pyspark-service \
  -n pyspark

start-spark-app.sh

stop-spark-app.sh

minikube stop

minikube delete --all --purge
