#!/usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-dev}"

kubectl apply -f k8s/base/namespace.yaml
kubectl apply -f https://strimzi.io/install/latest?namespace=streaming -n streaming
kubectl apply -f https://raw.githubusercontent.com/GoogleCloudPlatform/spark-on-k8s-operator/v1beta2-1.5.2-3.1.1/manifests/spark-operator.yaml
kubectl apply -f https://github.com/kedacore/keda/releases/download/v2.15.1/keda-2.15.1.yaml
kubectl apply -k "k8s/overlays/${ENVIRONMENT}"
