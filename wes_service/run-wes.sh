#!/bin/bash
wes-server --backend=wes_service.pivot_wes \
  --opt extra=--workDir=/toil-intermediate/workdir --opt extra=--batchSystem=chronos \
  --opt extra=--no-match-user --opt extra=--linkImports --opt extra=--not-strict \
  --opt extra=--jobStore=/toil-intermediate/jobstore-REPLACE \
  --opt extra=--defaultCores=16 --opt extra=--defaultMemory=20G --opt extra=--defaultDisk=30G 
