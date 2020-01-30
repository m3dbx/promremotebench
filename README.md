# promremotebench

Prometheus remote write benchmarker (acts as a Prometheus sending node_exporter samples).

It can also run in scrape mode where it serves as a scrape server for Prometheus to ingest metrics from.

It uses the InfluxData node_exporter simulator to generate and progress time using realistic host metrics and cardinality.

## Configuring

The main controls for controlling cardinality is the number of hosts to generate metrics for. You can also use the new series percent to tweak how many host IDs churn every scrape cycle (which is why scrape interval is required, even when running in scrape mode).

You can also specify fixed labels to apply to each benchmarker instance should you want to query the metrics. Use the environment variable `PROMREMOTEBENCH_LABELS_JSON` or the command line argument `labels`.

## Kubernetes manifest

Here is what a Kubernetes manifest looks like for hosting the benchmarker, simply turn up number of replicas for more load or targets.

### Remote write benchmarking

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: promremotebench
spec:
  replicas: 2
  selector:
    matchLabels:
      k8s-app: promremotebench
  template:
    metadata:
      labels:
        k8s-app: promremotebench
    spec:
      containers:
      - name: promremotebench
        image: quay.io/m3db/promremotebench:latest
        env:
        - name: PROMREMOTEBENCH_TARGET
          # NB: this can be a comma separated string of targets for writing to multiple targets.
          value: "http://m3coordinator-dedicated-bench-cluster:7201/api/v1/prom/remote/write"
        - name: PROMREMOTEBENCH_NUM_HOSTS
          value: "1000"
        - name: PROMREMOTEBENCH_INTERVAL
          value: "10"
        - name: PROMREMOTEBENCH_BATCH
          value: "128"
        - name: PROMREMOTEBENCH_NEW_SERIES_PERCENTAGE
          value: "0.01"
```

### Scrape benchmarking

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: promremotebench
spec:
  replicas: 2
  selector:
    matchLabels:
      k8s-app: promremotebench
  template:
    metadata:
      labels:
        k8s-app: promremotebench
    spec:
      containers:
      - name: promremotebench
        image: quay.io/m3db/promremotebench:latest
        env:
        - name: PROMREMOTEBENCH_SCRAPE_SERVER
          value: "0.0.0.0:4242"
        - name: PROMREMOTEBENCH_NUM_HOSTS
          value: "1000"
        - name: PROMREMOTEBENCH_NEW_SERIES_PERCENTAGE
          value: "0.01"
        ports:
        - name: http
          containerPort: 4242
```
