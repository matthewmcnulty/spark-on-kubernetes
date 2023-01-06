# Spark on Kubernetes: An end-to-end Streaming Data Pipeline

The Python library Tweepy will be used in this project to stream tweets on a specific topic in real-time.  After that, the spark-submit command will be executed to launch a Spark application on a Minikube cluster. The results of this application's sentiment analysis of the Twitter stream will be appended to a PostgreSQL database table. Finally, the Python library Streamlit will then be used to visualise the findings in a real-time dashboard.

## 1. Prequisites

### 1.1  Install Docker Desktop with WSL 2 Backend

<https://docs.docker.com/desktop/install/windows-install/>

### 1.2 Install Ubuntu

<https://www.microsoft.com/store/productId/9PN20MSR04DW>

### 1.3 Install Java

Update the apt package index.

```bash
sudo apt update -y
```

Install the JDK.

```bash
sudo apt install default-jdk -y
```

Verify that Java is installed.

```bash
java -version
```

### 1.4 Install Spark

Download the jar files for Spark 3.1.1.

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

Open ~/.profile using nano or vim.

```bash
vim ~/.profile
```

Add the following lines to the script.

```bash
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3
```

Reload this file to apply the changes.

```bash
source ~/.profile
```

Verify that the environment variables are set.

```bash
echo $JAVA_HOME
echo $SPARK_HOME
```

### 1.6 Install Minikube

Download the latest Minikube package using curl.

```bash
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
```

Use the install command to copy the files into the correct directory and set permissions.

```bash
sudo install minikube-linux-amd64 /usr/local/bin/minikube
```

The [documentation](https://spark.apache.org/docs/latest/running-on-kubernetes.html#prerequisites) recommends at least 3 CPUs and 4g of memory.

```bash
minikube config set cpus 3
minikube config set memory 8192
```

Start the minikube cluster.

```bash
minikube start
```

### 1.7 Install Kubectl

Download the Kubectl files using curl.

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
```

Use the install command to copy the files into /usr/local/bin/kubectl. Set the owner as root, the group owner as root, and the mode to 0755.

```bash
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
```

Test Kubectl by getting all objects in the Minikube cluster in all namespaces.

```bash
kubectl get all -A
```

## 2. Enable Role-Based Access Control (RBAC)

Create a new namespace.

```bash
kubectl create ns pyspark
```

Set it as the default namespace.

```bash
kubectl config set-context --current --namespace=pyspark
```

Create a new service account.

```bash
kubectl create serviceaccount pyspark-service \
  -n pyspark
```

Grant the service account access to resources in the namespace.

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

```bash
minikube dashboard
```

### 4.2 Start PostgreSQL Database

Inside 1. postgresql, start PgAdmin and PostgreSQL.

```bash
kubectl apply -f .
```

Access pgAdmin from outside the cluster.

```bash
minikube service pgadmin-service -n pyspark
```

### 4.3 Start Twitter Stream

Inside 2. twitter-app, start Twitter stream socket.

```bash
kubectl apply -f twitter-app.yaml
```

### 4.4 Start PySpark Application

Inside 3. spark-app, start the Spark service.

```bash
kubectl apply -f spark-service.yaml
```

To start the PySpark application, use the following command.

```bash
./start-spark-app.sh
```

To stop the PySpark application, use the following command.

```bash
./stop-spark-app.sh
```

Access Spark web user interface from outside the cluster.

```bash
minikube service spark-service -n pyspark
```

### 4.5 Start Streamlit Dashboard

Inside 4. streamlit-app, start the Streamlit dashboard.

```bash
kubectl apply -f streamlit-app.yaml
```

Access the Streamlit dashboard from outside the cluster.

```bash
minikube service streamlit-service -n pyspark
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
