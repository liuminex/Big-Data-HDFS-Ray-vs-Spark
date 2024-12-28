## Okeanos VMs

Create account at [okeanos-knossos.grnet.gr](https://okeanos-knossos.grnet.gr/home/) (choose Academic Login and sign in with NTUA).

Read the [guide](https://colab.research.google.com/drive/1pjf3Q6T-Ak2gXzbgoPpvMdfOHd1GqHZG?usp=sharing).

- Create 2 Debian 12 VMs
- Start the VMs
- Clone this repository in the home directory
```bash
$ cd ~
$ git clone https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark
```
- Change ``SSH_USER``, ``SSH_PORT`` and ``HOSTNAME`` in [okeanos-setup.sh](https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/scripts/okeanos-setup.sh) as needed.
- Execute the script as root in each machine.
- In both VMs, configure all necessary variables in [config.sh](https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/scripts/config.sh)
- Then execute in the following scripts in order as given:
  - [2-java.sh](https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/scripts/2-java.sh)
  - [3-install-hadoop-spark.sh](https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/scripts/3-install-hadoop-spark.sh)
  - [4-configure-hadoop.sh](https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/scripts/4-configure-hadoop.sh)
