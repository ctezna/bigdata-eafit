import boto3

region_name='us-east-1'

def get_security_group_id(group_name, region_name):
    ec2 = boto3.client('ec2', region_name=region_name)
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response['SecurityGroups'][0]['GroupId']
    
emr = boto3.client('emr', region_name=region_name)

cluster_response = emr.run_job_flow(
        Name='all-the-news', # update the value
        ReleaseLabel='emr-6.1.0,
        LogUri='s3://aws-logs-847119573351-us-east-1/elasticmapreduce/', # update the value
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'SPOT',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm4.xlarge', # change according to the requirement
                    'InstanceCount': 1 #for master node High Availabiltiy, set count more than 1
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm4.xlarge', # change according to the requirement
                    'InstanceCount': 2
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName' : 'bigdata', # update the value
            'EmrManagedMasterSecurityGroup': get_security_group_id('ElasticMapReduce-master', region_name=region_name)
            'EmrManagedSlaveSecurityGroup': get_security_group_id('ElasticMapReduce-master', region_name=region_name)
        },
        BootstrapActions=[    {
                    'Name':'install_dependencies',
                    'ScriptBootstrapAction':{
                            'Args':[],
                            'Path':'s3://ctezna-doc-collection/emr-scripts/bootstrap-emr.bash' 
                            }
                }],
        Steps = [],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Applications=[
            { 'Name': 'hadoop' },
            { 'Name': 'spark' },
            { 'Name': 'hive' },
            { 'Name': 'zeppelin' },
            { 'Name': 'Hue' },
            { 'Name': 'Tez' },
            { 'Name': 'HBase' },
            { 'Name': 'Sqoop' },
            { 'Name': 'Livy' }
        ],
        Configurations=[
            # YARN
            {
                "Classification": "yarn-site", 
                "Properties": {"yarn.nodemanager.vmem-pmem-ratio": "4",
                               "yarn.nodemanager.pmem-check-enabled": "false",
                               "yarn.nodemanager.vmem-check-enabled": "false"}
            },
            
            # HADOOP
            {
                "Classification": "hadoop-env", 
                "Configurations": [
                        {
                            "Classification": "export", 
                            "Configurations": [], 
                            "Properties": {"JAVA_HOME": "/usr/lib/jvm/java-1.8.0"}
                        }
                    ], 
                "Properties": {}
            },
            
            # SPARK
            {
                "Classification": "spark-env", 
                "Configurations": [
                        {
                            "Classification": "export", 
                            "Configurations": [], 
                            "Properties": {"PYSPARK_PYTHON":"/usr/bin/python3",
                                           "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"}
                        }
                    ], 
                "Properties": {}
            },
            {
                "Classification": "spark",
                "Properties": {"maximizeResourceAllocation": "true"},
                "Configurations": []
             },
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.dynamicAllocation.enabled": "true" #default is also true
                }
            }
        ]
    )