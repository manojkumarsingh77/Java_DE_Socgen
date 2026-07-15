# Retail Platform Promotion Model

Multi-Environment Strategy demo for Spark-on-AKS: **Dev/Test/Prod separation,
config externalization, secrets management, and promotion workflows**.

Start here: **`LAB_GUIDE.md`** — a complete, step-by-step, copy-pasteable lab.

```
retail-platform-promotion-model/
├── pom.xml                          Maven build (Java 17, Spark 3.5.1, Delta 3.2.0)
├── LAB_GUIDE.md                     Full step-by-step lab (read this first)
├── run-configs/                     IntelliJ VM options for macOS M1 Max & Windows 11
├── src/main/resources/
│   ├── application.conf             Base config (env-agnostic business rules)
│   ├── application-dev.conf         Dev overlay  (local[*], synthetic data, env-var secrets)
│   ├── application-test.conf        Test overlay (AKS, ADLS Gen2, Key Vault)
│   └── application-prod.conf        Prod overlay (AKS, ADLS Gen2, Key Vault, full scale)
├── src/main/java/com/retailbank/
│   ├── config/                      ConfigLoader + typed AppConfig records
│   ├── secrets/                     SecretsProvider abstraction (local env vs Azure Key Vault)
│   ├── data/                        Synthetic retail banking dataset generator
│   └── pipeline/                    PromotionPipeline (main) + scoring engine
├── docker/Dockerfile                Multi-stage build on apache/spark:3.5.1-java17
└── k8s/
    ├── base/                        Environment-agnostic SparkApplication CRD
    └── overlays/{dev,test,prod}/    Kustomize overlays (scale, secrets, image tag per env)
```
