# Resources for GenAI Powered Anomaly Detection: Spotting Anomalies in real-time

The purpose of this code repository is to house the resources used in the workshop called "GenerativeAI-Powered anomaly detection: Spotting anomalies in real-time". The repository contains the following sections:

### 1. CDK Project
This is located on the root of the project (`app.py`) is the entrypoint, coupled with the  `requirements.txt` for dependencies and `cdk/` folder for the shared code resources and other complementary CDK files.

Type `pip install -r requirements.txt` to start working with the project, then `cdk synth` or `cdk deploy` to deploy the required resources to your own account using the CDK.

### 2. Flink app
Located in `flink-app/`, can be executed by loading into an IDE and executing the program, or by running `mvn compile` to build an application jar.

### 3. Lambda Code
The lambda code can be found under `code/` tab under `lambdas` for any component of the workshop that leverages lambda functions.

The remaining components of the workshop can be found via the workshop itself found [here](https://catalog.us-east-1.prod.workshops.aws/workshops/7cb7c933-4024-4b69-a29a-b1159d6b909f/).


### 4. Sagemaker Jupyter notebook
In this sample notebook, you will train, build, and deploy a model using the IP Insights algorithm and Amazon VPC flowlog data. You will query an Athena table and create a dataset for model training. You will perform data transformation on the results from the VPC flowlog data. Train an IP Insights model with this data. Deploy your model to a SageMaker endpoint and ultimately test your model.