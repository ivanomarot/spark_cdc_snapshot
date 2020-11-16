SHELL := /bin/bash
emr_cluster_id ?= $(shell [ -z "${EMR_CLUSTER}" ] || echo ${EMR_CLUSTER})
s3_bucket ?= $(shell [ -z "${S3_BUCKET}" ] || echo ${S3_BUCKET})
table ?= $(shell [ -z "${TABLE}" ] || echo ${TABLE})
database ?= $(shell [ -z "${DATABASE}" ] && echo "default."  || echo "${DATABASE}.")
run_date ?= $(shell [ -z "${RUN_DATE}" ] || echo ${RUN_DATE})
is_partitioned ?= $(shell [ -z "${PARTITIONED}" ] && echo "false"  || echo ${PARTITIONED})
partitioned_flag ?= $(shell [ "${is_partitioned}" == "true" ] && echo ",--partitioned"  || echo "")

build:
	@make clean
	@[ "${s3_bucket}" ] || ( echo ">> S3_BUCKET is not set."; exit 1 )
	@mkdir -p ./dist/
	@(ls -d */ | grep -v "dist" | xargs -I{} cp -r {} ./dist/{})
	@mkdir -p ./libs/core/
	@cp -r ./core/* ./libs/core/
	@(cd ./libs/ && zip -r ../dist/libs.zip .)
	aws s3 sync --delete ./dist/ s3://${s3_bucket}/build/

clean:
	@rm -rf ./libs/
	@rm -rf ./dist/

run:
	@make build
	@[ "${emr_cluster_id}" ] || ( echo ">> EMR_CLUSTER is not set."; exit 1 )
	@[ "${table}" ] || ( echo ">> TABLE is not set."; exit 1 )
	@[ "${run_date}" ] || ( echo ">> RUN_DATE is not set."; exit 1 )
	aws emr add-steps --cluster-id ${emr_cluster_id} \
		--steps Name="EmrfsSync",Jar="command-runner.jar",Args=["emrfs","sync","s3://${s3_bucket}/build/"]
	aws emr add-steps --cluster-id ${emr_cluster_id} \
		--steps Type=Spark,Name="sparkcdc_${table}",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--name,"sparkcdc",--py-files,s3://${s3_bucket}/build/libs.zip,s3://${s3_bucket}/build/raw/generate_cdc_snapshot.py,--raw-bucket,"${s3_bucket}",--input-table,"${table}",--output-table,"${database}${table}_snapshot",--processing-date,"${run_date}"${partitioned_flag}]
