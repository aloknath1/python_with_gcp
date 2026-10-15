from google.cloud import compute_v1
import os
import random

# Authentication
# This file must exist and contain valid service account credentials.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

# GCP configuration
PROJECT_ID = "test-project-511411"
ZONE = "asia-south2-a"
REGION = "asia-south2"

random_number = random.randint(100, 999)
INSTANCE_NAME = f"my-neww-instance-{random_number}"
print(f'INSTANCE NAME:{INSTANCE_NAME}')
MACHINE_TYPE = f"zones/{ZONE}/machineTypes/e2-medium"
SUBNETWORK = f"regions/{REGION}/subnetworks/default"
SOURCE_IMAGE = "projects/debian-cloud/global/images/family/debian-12"

def create_vm():
    instances_client = compute_v1.InstancesClient()

    # Configure the boot disk
    boot_disk = compute_v1.AttachedDisk(
        boot= True,
        auto_delete= True,
        initialize_params= compute_v1.AttachedDiskInitializeParams(
            source_image= SOURCE_IMAGE,
            disk_size_gb=20,
        )
    )
    
    # Configure the network interface
    network_interface = compute_v1.NetworkInterface(
        subnetwork=SUBNETWORK,
        access_configs=[
            compute_v1.AccessConfig(                
                name= "External NAT",
                type="ONE_TO_ONE_NAT",                
            )
        ]
    )

    # Configure the VM instance
    instance = compute_v1.Instance(
        name=INSTANCE_NAME,
        machine_type= MACHINE_TYPE,
        disks=[boot_disk],
        network_interfaces=[network_interface]
    )
   
   # Create the VM
    operation = instances_client.insert(
        project=PROJECT_ID,
        zone=ZONE,
        instance_resource=instance
    )

    print("Creating VM instance...")
    operation.result(timeout=300)
    print(f'Created VM Instance:{INSTANCE_NAME}')

if __name__ == "__main__":
    create_vm()