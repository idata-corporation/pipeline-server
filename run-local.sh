java -Xms8G -Xmx8G -Dspring.profiles.active=poc -Daws.profile=poc -Dspring.config.location=./pipelineserver/src/main/resources/application.poc.yaml -jar ./pipelineserver/target/scala-2.12/pipeline-pipelineserver-assembly-2.3.5.jar