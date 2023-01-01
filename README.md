# twitter-spark-streaming

In this project, we will use the Python library Tweepy to stream tweets on a specific topic in real-time through a socket. Following that, we'll use spark-submit to start a Spark Structured Streaming application on a Minikube cluster. This application will analyse the sentiment of the Twitter stream and append the results to a PostgreSQL database table. Finally, we will create a dashboard using the Python library Streamlit to visualise the results simultaneously.

## 1. Prequisites

### 1.1  Install Docker Desktop with WSL 2 Backend

<https://docs.docker.com/desktop/install/windows-install/>

### 1.2 Install Ubuntu

<https://www.microsoft.com/store/productId/9PN20MSR04DW>

### 1.3 Install Java

First update your apt package index.

```bash
sudo apt update -y
```

To install the JDK, execute the following command, which will also install the JRE:

```bash
sudo apt install default-jdk -y
```

Verify the installations with the following command.

```bash
java -version
```

### 1.4 Install Spark

Download the jar files for Spark 3.1.1 and Hadoop 3 from archive.apache.org.

```bash
wget https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
```

Unpack the tar file into a directory of the same name.

```bash
tar xzf spark-3.3.1-bin-hadoop3.tgz
```

Remove the tar file.

```bash
rm spark-3.3.1-bin-hadoop3.tgz
```

### 1.5 Add Environment Variables

Move the directory into /opt and make a symlink in the same directory /opt/spark that points to it.

```bash
sudo mv -f spark-3.3.1-bin-hadoop3 /opt
sudo ln -s spark-3.3.1-bin-hadoop3 /opt/spark
```

Copy the path from your preferred installation. Then open ~/.profile using nano or your favorite text editor.

```bash
vim ~/.profile
```

Add the following to the bottom of the script.

```bash
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3
```

Reload this file to apply the changes to your current session.

```bash
source ~/.profile
```

Verify that the environment variable is set:

```bash
echo $JAVA_HOME
echo $SPARK_HOME
```

### 1.6 Install Minikube

Download the latest Minikube package for Linux using curl.

```bash
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
```

We will use the install command to copy files into the correct directory and set permissions.

```bash
sudo install minikube-linux-amd64 /usr/local/bin/minikube
```

The [Running Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html#prerequisites) documentation recommends at least 3 CPUs and 4g of memory.

```bash
minikube config set memory 8192
minikube config set cpus 3
```

We will now start the minikube cluster.

```bash
minikube start
```

### 1.7 Install Kubectl

We begin by downloading the Kubectl files from using curl.

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
```

Then we use the install tool to copy the files to /usr/local/bin/kubectl. We set the owner as root, the group owner as root, and the mode to 0755.

```bash
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
```

Test out Kubectl by getting all objects on the Minikube cluster in all namespaces.

```bash
kubectl get all -A
```

## 2. Enable Role-Based Access Control (RBAC)

The Spark driver pod requires a service account that has the permission to create executor pods.

First we create a new namespace.

```bash
kubectl create ns pyspark
```

Set it as the default namespace.

```bash
kubectl config set-context --current --namespace=pyspark
```

Then we create a new service account.

```bash
kubectl create serviceaccount pyspark-service \
  -n pyspark
```

Then we grant the service account access to resources in the namespace.

```bash
kubectl create clusterrolebinding pyspark-clusterrole \
  --clusterrole=edit \
  --serviceaccount=pyspark:pyspark-service \
  -n pyspark
```

## 3. Build Docker Images

### 3.1 Point Shell to the Minikube Docker Daemon

```bash
eval $(minikube -p minikube docker-env)
```

### 3.2 Build PySpark Image

```bash
cd $SPARK_HOME
```

```bash
./bin/docker-image-tool.sh \
  -m \
  -t v3.3.1 \
  -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile \
  build
```

### 3.3 Build Application Images

Inside 2. twitter-app.

```bash
docker build -t twitter-app:1.0 .
```

Inside 3. spark-app.

```bash
docker build -t spark-app:1.0 .
```

Inside 4. streamlit-app.

```bash
docker build -t streamlit-app:1.0 .
```

List the Docker images.

```bash
docker images
```

## 4. Deploy to Minikube

### 4.1 Start Minikube Dashboard

Use the following command and open the given address.

```bash
minikube dashboard
```

### 4.2 Start PostgreSQL Database

Inside 1. postgresql.

```bash
kubectl apply -f .
```

### 4.3 Start Twitter Stream

Inside 2. twitter-app.

```bash
kubectl apply -f .
```

### 4.4 Start PySpark Application

Inside 3. spark-app.

To start the PySpark application, use the following command.

```bash
start-spark-app.sh
```

To stop the PySpark application, use the following command.

```bash
stop-spark-app.sh
```

### 4.5 Start Streamlit Dashboard

Inside 4. streamlit-app.

Use the following command and open the given address.

```bash
kubectl apply -f .
```

## 5. Clean Up

### 5.1 Stop the Minikube Cluster

```bash
minikube stop
```

### 5.2 Delete the Minikube Cluster

```bash
minikube delete
```
