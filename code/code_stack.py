# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import os.path as path
from aws_cdk import (
    Duration,
    Stack,
    Aws,
    CfnParameter,
    CustomResource,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct
from aws_cdk.aws_lambda_python_alpha import PythonLayerVersion
from aws_cdk import aws_sns as sns
from aws_cdk import aws_kms as kms

PARENT_DIR: str = path.join(os.path.dirname(__file__), "..")
LAMBDA_PATH: str = path.join(PARENT_DIR, "code", "lambdas")
REGION_NAME = Aws.REGION
ACCOUNT_ID = Aws.ACCOUNT_ID
APP_LOG_LEVEL = "INFO"

LAMBDAS_LAYER_ARN = (
    f"arn:aws:lambda:{REGION_NAME}:336392948345:layer:AWSSDKPandas-Python311"
)

# AWSSDKPandas-Python311
# arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python311:10
class CodeStack(Stack):
    """
    Define all AWS resources for the app
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.topic_name = "AnomalyReportSNSTopic"
        self.lambda_runtime = lambda_.Runtime.PYTHON_3_11
        self.lambda_architecture = lambda_.Architecture.ARM_64
        self.msk_cluster_arn = CfnParameter(self, "mskClusterArn", type="String",
            description="The ARN of the MSK cluster.")
        
        # Custom resource to lookup MSK cluster details
        msk_lookup = self.create_msk_lookup_custom_resource()
        topic = self.get_topic()
        msk_layer = self.create_lambda_layer("msk_layer")
        LAMBDAS_LAYER_ARN: str = (
            f"arn:aws:lambda:{Aws.REGION}:017000801446:layer:AWSLambdaPowertoolsPythonV2:68"
        )
        pandas_layer = lambda_.LayerVersion.from_layer_version_arn(
            self, id="PandasLayer", layer_version_arn=LAMBDAS_LAYER_ARN
        )

        _ = self.create_lambda_functions(
            msk_layer, pandas_layer, topic, msk_lookup
        )

    def get_topic(self):
        # Use existing SNS topic created by workshop CFN
        topic = sns.Topic.from_topic_arn(
            self,
            self.topic_name,
            topic_arn=f"arn:aws:sns:{REGION_NAME}:{ACCOUNT_ID}:{self.topic_name}"
        )
        return topic

    def create_msk_lookup_custom_resource(self):
        msk_lookup_role = iam.Role(
            self, "MSKLookupRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )
        msk_lookup_role.add_to_policy(
            iam.PolicyStatement(
                actions=["kafka:DescribeCluster", "kafka:DescribeClusterV2", "kafka:GetBootstrapBrokers", "ec2:DescribeSubnets"],
                resources=["*"]
            )
        )
        
        msk_lookup_lambda = lambda_.Function(
            self, "MSKLookupLambda",
            runtime=self.lambda_runtime,
            handler="index.handler",
            role=msk_lookup_role,
            timeout=Duration.seconds(60),
            code=lambda_.Code.from_inline("""
import boto3
import cfnresponse
def handler(event, context):
    try:
        if event['RequestType'] == 'Delete':
            return {'Status': 'SUCCESS', 'PhysicalResourceId': 'msk-lookup'}
        cluster_arn = event['ResourceProperties']['ClusterArn']
        kafka = boto3.client('kafka')
        response = kafka.describe_cluster_v2(ClusterArn=cluster_arn)
        cluster = response['ClusterInfo']
        brokers_response = kafka.get_bootstrap_brokers(ClusterArn=cluster_arn)
        bootstrap_address = brokers_response.get('BootstrapBrokerStringSaslIam', '')
        if 'Serverless' in cluster:
            vpc_config = cluster['Serverless']['VpcConfigs'][0]
            ec2 = boto3.client('ec2')
            subnet_response = ec2.describe_subnets(SubnetIds=[vpc_config['SubnetIds'][0]])
            vpc_id = subnet_response['Subnets'][0]['VpcId']
            response = {
                'VpcId': vpc_id,
                'SubnetId': vpc_config['SubnetIds'][0],
                'SecurityGroupId': vpc_config['SecurityGroupIds'][0],
                'BootstrapBrokers': bootstrap_address
            }
            cfnresponse.send(event, context, cfnresponse.SUCCESS, response, event['LogicalResourceId'])
    except Exception as e:
        print("Failed getting bootstrap brokers:", e)
        cfnresponse.send(event, context, cfnresponse.FAILED, response, event['LogicalResourceId'])
""")
        )
        
        return CustomResource(
            self, "MSKLookup",
            service_token=msk_lookup_lambda.function_arn,
            properties={'ClusterArn': self.msk_cluster_arn.value_as_string}
        )

    def create_lambda_functions(self, msk_layer, pandas_layer, topic, msk_lookup):
        """
        Create lambda functions
        """

        bedrock_policy = iam.Policy(
            self,
            "BedrockPolicy",
            policy_name="AmazonBedrockAccessPolicy",
            statements=[
                iam.PolicyStatement(
                    actions=["bedrock:*"],
                    resources=["*"],
                    effect=iam.Effect.ALLOW,
                )
            ],
        )
        s3_policy = iam.Policy(
            self,
            "S3Policy",
            policy_name="WriteToS3BucketPolicy",
            statements=[
                iam.PolicyStatement(
                    actions=["s3:*Object", "s3:ListBucket"],
                    resources=["*"],
                    effect=iam.Effect.ALLOW,
                )
            ],
        )
        xray_policy = iam.Policy(
            self,
            "XRayPolicy",
            policy_name="XRayAccessPolicy",
            statements=[
                iam.PolicyStatement(
                    actions=["xray:PutTraceSegments",
                             "xray:PutTelemetryRecords"],
                    resources=["*"],
                    effect=iam.Effect.ALLOW,
                )
            ],
        )

        sns_policy = iam.Policy(
            self,
            "SnsPolicy",
            policy_name="AllowPublishToSns",
            statements=[
                iam.PolicyStatement(
                    actions=["sns:Publish"],
                    resources=[topic.topic_arn],
                    effect=iam.Effect.ALLOW,
                )
            ],
        )

        publish_firehose_role = iam.Role(
            self,
            "PublishFirehoseRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Role with multiple permissions",
        )

        # Policy 1: Log Permissions
        publish_firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup"],
                resources=[f"arn:aws:logs:{REGION_NAME}:{ACCOUNT_ID}:*"],
            )
        )

        producer_role = iam.Role(
            self,
            "ProducerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Role with multiple permissions",
        )

        # Policy 1: S3 and S3 Object Lambda
        producer_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:*", "s3-object-lambda:*"], resources=["*"])
        )

        # Policy 2: Logs permissions
        producer_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup"],
                resources=[f"arn:aws:logs:{REGION_NAME}:{ACCOUNT_ID}:*"],
            )
        )
        producer_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                resources=[
                    f"arn:aws:logs:{REGION_NAME}:{ACCOUNT_ID}:log-group:/aws/lambda/msf-anomaly-detection-workshop-producer:*"
                ],
            )
        )

        # Policy 3: Kafka and EC2 permissions
        producer_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka:DescribeCluster",
                    "kafka:DescribeClusterV2",
                    "kafka:GetBootstrapBrokers",
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DescribeVpcs",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeSecurityGroups",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        # Policy 4: EC2 network interface permissions
        producer_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:CreateNetworkInterface",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                ],
                resources=["*"],
            )
        )

        # Policy 5: Kafka cluster and topic permissions
        producer_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:Connect",
                    "kafka-cluster:AlterCluster",
                    "kafka-cluster:DescribeCluster",
                ],
                resources=[
                    f"arn:aws:kafka:{REGION_NAME}:{ACCOUNT_ID}:cluster/*"],
            )
        )
        producer_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:*Topic*",
                    "kafka-cluster:WriteData",
                    "kafka-cluster:ReadData",
                ],
                resources=[
                    f"arn:aws:kafka:{REGION_NAME}:{ACCOUNT_ID}:topic/*"],
            )
        )
        producer_role.add_to_policy(
            iam.PolicyStatement(
                actions=["kafka-cluster:AlterGroup",
                         "kafka-cluster:DescribeGroup"],
                resources=[
                    f"arn:aws:kafka:{REGION_NAME}:{ACCOUNT_ID}:group/*"],
            )
        )

        # Policy 6: Kafka cluster extended permissions
        producer_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:DeleteGroup",
                    "kafka-cluster:WriteDataIdempotently",
                    "kafka-cluster:DescribeCluster",
                    "kafka-cluster:ReadData",
                    "kafka-cluster:DescribeTransactionalId",
                    "kafka-cluster:AlterTransactionalId",
                    "kafka-cluster:DescribeTopicDynamicConfiguration",
                    "kafka-cluster:AlterTopicDynamicConfiguration",
                    "kafka-cluster:AlterGroup",
                    "kafka-cluster:AlterClusterDynamicConfiguration",
                    "kafka-cluster:AlterTopic",
                    "kafka-cluster:CreateTopic",
                    "kafka-cluster:DescribeTopic",
                    "kafka-cluster:AlterCluster",
                    "kafka-cluster:DescribeGroup",
                    "kafka-cluster:DescribeClusterDynamicConfiguration",
                    "kafka-cluster:Connect",
                    "kafka-cluster:DeleteTopic",
                    "kafka-cluster:WriteData",
                ],
                resources=["*"],
            )
        )

        # Policy 7: SNS Publish permission
        producer_role.add_to_policy(
            iam.PolicyStatement(actions=["sns:Publish"], resources=["*"])
        )

        lambda_function_fragmentation_attack = lambda_.Function(
            self,
            "fragmentation-attack",
            function_name=f"{Aws.STACK_NAME}-fragmentation-attack",
            description="Lambda code for generating a fragmentation attack.",
            architecture=self.lambda_architecture,
            handler="lambda_handler.lambda_handler",
            runtime=self.lambda_runtime,
            code=lambda_.Code.from_asset(
                path.join(os.getcwd(), LAMBDA_PATH, "fragmentation_attack")
            ),
            environment={
                "POWERTOOLS_SERVICE_NAME": "app-fragmentation",
                "POWERTOOLS_METRICS_NAMESPACE": f"{Aws.STACK_NAME}-ns",
                "POWERTOOLS_LOG_LEVEL": APP_LOG_LEVEL,
                "BOOTSTRAP_SERVER": msk_lookup.get_att_string("BootstrapBrokers"),
                "TOPIC_NAME": "flow-log-ingest",
            },
            layers=[msk_layer],
            role=producer_role,
            timeout=Duration.minutes(15),
            memory_size=2048,
            tracing=lambda_.Tracing.ACTIVE,
            vpc=ec2.Vpc.from_vpc_attributes(self, "MSKVpc", vpc_id=msk_lookup.get_att_string("VpcId"), availability_zones=[f"{REGION_NAME}a", f"{REGION_NAME}b"]),
            vpc_subnets=ec2.SubnetSelection(subnets=[ec2.Subnet.from_subnet_id(self, "MSKSubnet", msk_lookup.get_att_string("SubnetId"))]),
            security_groups=[ec2.SecurityGroup.from_security_group_id(self, "MSKSecurityGroup", msk_lookup.get_att_string("SecurityGroupId"))]
        )

        firehose_json_parse_function = lambda_.Function(
            self,
            "firehose_json_parse",
            function_name=f"{Aws.STACK_NAME}-firehose-json-parse",
            description="Lambda code for parsing JSON messages from MSK to Amazon Firehose.",
            architecture=self.lambda_architecture,
            handler="parse_json.lambda_handler",
            runtime=self.lambda_runtime,
            code=lambda_.Code.from_asset(
                path.join(os.getcwd(), LAMBDA_PATH, "parse_json")
            ),
            environment={},
            layers=[],
            role=publish_firehose_role,
            timeout=Duration.seconds(60),
            memory_size=512,
        )

        # EventBridge rule to trigger fragmentation-attack lambda every 2 minutes
        fragmentation_rule = events.Rule(
            self,
            "FragmentationAttackRule",
            schedule=events.Schedule.rate(Duration.minutes(2)),
            enabled=False
        )
        
        fragmentation_rule.add_target(targets.LambdaFunction(lambda_function_fragmentation_attack))
        
        # Grant EventBridge permission to invoke the lambda
        lambda_function_fragmentation_attack.add_permission(
            "AllowEventBridge",
            principal=iam.ServicePrincipal("events.amazonaws.com"),
            source_arn=fragmentation_rule.rule_arn
        )

        # Bedrock agent permissions for new lambdas
        bedrock_agent_policy = iam.Policy(
            self,
            "BedrockAgentPolicy",
            policy_name="BedrockAgentAccessPolicy",
            statements=[
                iam.PolicyStatement(
                    actions=["bedrock:InvokeAgent"],
                    resources=["*"],
                    effect=iam.Effect.ALLOW,
                )
            ],
        )

        # Role for agent action group lambda
        agent_action_role = iam.Role(
            self,
            "AgentActionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        agent_action_role.attach_inline_policy(bedrock_policy)
        agent_action_role.attach_inline_policy(sns_policy)
        
        # Add KMS permissions for SNS topic encryption
        agent_action_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kms:GenerateDataKey",
                    "kms:Decrypt"
                ],
                resources=["*"]
            )
        )

        # Attach Bedrock agent policy to producer role for invoke agent lambda
        producer_role.attach_inline_policy(bedrock_agent_policy)

        # Agent action group lambda
        agent_action_group_function = lambda_.Function(
            self,
            "AgentActionGroupLambda",
            function_name="SecurityTools-agent-action-group",
            description="Lambda function for Bedrock agent action group.",
            architecture=self.lambda_architecture,
            handler="action_group.lambda_handler",
            runtime=self.lambda_runtime,
            code=lambda_.Code.from_asset(
                path.join(os.getcwd(), LAMBDA_PATH, "agent_action_group")
            ),
            environment={
                "TOPIC_ARN": topic.topic_arn,
                "REGION_NAME": REGION_NAME,
            },
            role=agent_action_role,
            timeout=Duration.minutes(5),
            memory_size=1024,
            tracing=lambda_.Tracing.ACTIVE,
        )
        
        # Allow Bedrock to invoke the agent action group lambda
        agent_action_group_function.add_permission(
            "AllowBedrock",
            principal=iam.ServicePrincipal("bedrock.amazonaws.com"),
            action="lambda:InvokeFunction"
        )

        # Invoke agent lambda (replaces GenerateReportLambda)
        invoke_agent_function = lambda_.Function(
            self,
            "InvokeAgentLambda",
            function_name=f"{Aws.STACK_NAME}-invoke-agent",
            description="Lambda function to invoke Bedrock agent for anomaly analysis.",
            architecture=self.lambda_architecture,
            handler="summarization.lambda_handler",
            runtime=self.lambda_runtime,
            code=lambda_.Code.from_asset(
                path.join(os.getcwd(), LAMBDA_PATH, "invoke_agent")
            ),
            environment={
                "POWERTOOLS_SERVICE_NAME": "invoke-agent",
                "POWERTOOLS_METRICS_NAMESPACE": f"{Aws.STACK_NAME}-ns",
                "POWERTOOLS_LOG_LEVEL": APP_LOG_LEVEL,
                "AGENT_ID": "PLACEHOLDER_AGENT_ID",
                "AGENT_ALIAS_ID": "PLACEHOLDER_AGENT_ALIAS_ID",
                "REGION_NAME": REGION_NAME,
            },
            layers=[pandas_layer],
            role=producer_role,
            timeout=Duration.minutes(10),
            memory_size=1024,
            tracing=lambda_.Tracing.ACTIVE,
        )

        # MSK Event Source Mapping - now targets invoke_agent_function instead of generate_report
        lambda_.EventSourceMapping(
            self,
            "AmazonMSKLambdaLLMReportSourceMapping",
            event_source_arn=self.msk_cluster_arn.value_as_string,
            target=invoke_agent_function,
            batch_size=1000,
            enabled=False,
            max_batching_window=Duration.seconds(60),
            starting_position=lambda_.StartingPosition.TRIM_HORIZON,
            kafka_consumer_group_id=f"{Aws.STACK_NAME}-llm-report",
            kafka_topic="flow-log-egress"
        )

    def create_lambda_layer(self, layer_name):
        """
        Create a Lambda layer with necessary dependencies.
        """
        # Create the Lambda layer
        layer = PythonLayerVersion(
            self,
            layer_name,
            entry=path.join(os.getcwd(), LAMBDA_PATH, layer_name),
            compatible_runtimes=[self.lambda_runtime],
            compatible_architectures=[self.lambda_architecture],
            layer_version_name=layer_name,
        )

        return layer