### PySpark Docker Playground
A lightweight, containerized environment for experimenting with PySpark, exploring distributed data processing concepts, and running Spark scripts without the hassle of local installation. This project bundles everything you need—Spark, Python, and supporting tools—into a clean, reproducible Docker setup so you can focus entirely on learning and building.


### Usage

**Start all services:**
```sh
docker compose up -d
```

**Submit a Spark script in batch mode:**
```sh
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /apps/${SCRIPT_NAME}
```