# Amazon Reviews Medallion Pipeline on Azure: Project Report

## 1. Environment and tooling

| Tool | Version | Purpose |
|---|---|---|
| Ubuntu on WSL2 | 24.04 | Development environment matching cloud VMs |
| Python | 3.12 | Pipeline language |
| Java | OpenJDK 17 | Spark runtime |
| PySpark | 3.5.x | Distributed processing engine |
| Docker Desktop | 29.x | Packaging |
| Azure CLI / AzCopy | latest / 10.x | Infrastructure and data transfer |

Decision record: Ubuntu on WSL2 was chosen so that local, container and cloud runs use the same operating system, avoiding Windows-specific Hadoop binaries. A Python 3.14 installation on the Windows host was incompatible with PySpark 3.5 and was left out of the project.

References: Apache Spark documentation (spark.apache.org/docs/latest), Microsoft Learn WSL documentation.
