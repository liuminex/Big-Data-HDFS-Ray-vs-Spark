## Usage

After successful installation and configuration, after reboot you can start them using:
![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542)
```bash
start-dfs.sh
start-yarn.sh
$SPARK_HOME/sbin/start-history-server.sh
./project/documentation/scripts/8-start-ray.sh master
```

![Worker VM Badge](https://img.shields.io/badge/VM-Worker-f5dd42)
```bash
./project/documentation/scripts/8-start-ray.sh worker
```
