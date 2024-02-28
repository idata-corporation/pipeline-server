# pipeline-core
The core project for the IData Pipeline

# helm

## install helm cli https://helm.sh/docs/intro/install/


## ingress nginx for initial ingress
```
kubectl create ns ingress-nginx
helm install ingress-nginx ingress-nginx/ingress-nginx -f ./values/ingress-nginx/values.yaml -n ingress-nginx
```

## enable cloudwatch logs
```
kubectl apply -f https://raw.githubusercontent.com/aws-samples/amazon-cloudwatch-container-insights/latest/k8s-deployment-manifest-templates/deployment-mode/daemonset/container-insights-monitoring/cloudwatch-namespace.yaml

kubectl create configmap cluster-info \
--from-literal=cluster.name=poc-eks \
--from-literal=logs.region=us-east-1 -n amazon-cloudwatch

kubectl apply -f https://raw.githubusercontent.com/aws-samples/amazon-cloudwatch-container-insights/latest/k8s-deployment-manifest-templates/deployment-mode/daemonset/container-insights-monitoring/fluentd/fluentd.yaml
```

## Helm Deployment for pipeline

```
kubectl create namespace pipeline-qa
```

```bash
# install helm chart
helm upgrade -i pipelineapi-qa ./helm/pipelineapi -f ./helm/values/poc/values.yaml -n pipeline-qa

# uninstall helm chart
helm ls -n pipeline-qa
helm delete pipelineapi-qa -n pipeline-qa
```

## Get kubectl identity


```
# login to eks
aws eks update-kubeconfig --name poc-eks

# check running pod in edp namespace
kubectl get po -n pipeline-qa

# get name of deployed pods
kubectl get deploy -n pipeline-qa

# check logs 
kubectl logs -f deploy/pipelineapi-qa -n pipeline-qa

# ssh to the pod with bash entrypoint
kubectl exec -it pipelineapi-697554bd99-vg5fs -n edp bash

# delete pod
kubectl delete po pipelineapi-697554bd99-vg5fs -n edp

# get emr-containers virtual-cluster-id
aws emr-containers list-virtual-clusters --query "virtualClusters[?state=='RUNNING'].id" --output text
```
