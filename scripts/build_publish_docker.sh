#!/bin/bash
set -e
docker build -t goncaloferreirauva/gd-wp6-kpi-service:latest -f ./ci_calc_service/Dockerfile ./ci_calc_service
docker push goncaloferreirauva/gd-wp6-kpi-service:latest