#!/bin/bash
set -e
docker build -t goncaloferreirauva/gd-wp6-kpi-service:latest -f ./kpi_service/Dockerfile ./kpi_service
docker push goncaloferreirauva/gd-wp6-kpi-service:latest