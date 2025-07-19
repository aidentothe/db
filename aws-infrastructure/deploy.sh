#!/bin/bash

# Deployment script for AWS Spark infrastructure
set -e

echo "=== AWS Spark Infrastructure Deployment ==="

# Configuration
TERRAFORM_DIR="./terraform"
KEY_NAME="spark-cluster-key"

# Check if required tools are installed
check_dependencies() {
    echo "Checking dependencies..."
    
    if ! command -v terraform &> /dev/null; then
        echo "Error: Terraform is not installed"
        exit 1
    fi
    
    if ! command -v aws &> /dev/null; then
        echo "Error: AWS CLI is not installed"
        exit 1
    fi
    
    echo "Dependencies check passed"
}

# Generate SSH key pair if it doesn't exist
setup_ssh_key() {
    if [ ! -f ~/.ssh/id_rsa ]; then
        echo "Generating SSH key pair..."
        ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
    else
        echo "SSH key pair already exists"
    fi
}

# Initialize and apply Terraform
deploy_infrastructure() {
    echo "Deploying infrastructure with Terraform..."
    
    cd $TERRAFORM_DIR
    
    # Initialize Terraform
    terraform init
    
    # Plan the deployment
    terraform plan -out=tfplan
    
    # Ask for confirmation
    echo "Do you want to apply the Terraform plan? (y/N)"
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        # Apply the plan
        terraform apply tfplan
        
        # Output important information
        echo ""
        echo "=== Deployment Complete ==="
        echo "Spark Master IP: $(terraform output -raw spark_master_public_ip)"
        echo "Spark Web UI: $(terraform output -raw spark_master_web_ui)"
        echo "S3 Bucket: $(terraform output -raw s3_bucket_name)"
        echo "SSH Command: $(terraform output -raw ssh_command)"
        echo ""
        
        # Save outputs to file
        terraform output -json > ../outputs.json
        
    else
        echo "Deployment cancelled"
        rm -f tfplan
        exit 0
    fi
    
    cd ..
}

# Upload application code to EC2
upload_application() {
    echo "Uploading application code..."
    
    # Get the master IP from Terraform outputs
    MASTER_IP=$(cd $TERRAFORM_DIR && terraform output -raw spark_master_public_ip)
    
    if [ -z "$MASTER_IP" ]; then
        echo "Error: Could not get master IP address"
        exit 1
    fi
    
    # Wait for instance to be ready
    echo "Waiting for EC2 instance to be ready..."
    sleep 30
    
    # Test SSH connection
    echo "Testing SSH connection..."
    for i in {1..10}; do
        if ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no ubuntu@$MASTER_IP "echo 'SSH connection successful'" 2>/dev/null; then
            break
        fi
        echo "SSH attempt $i failed, retrying in 30 seconds..."
        sleep 30
    done
    
    # Create application directory on EC2
    ssh -o StrictHostKeyChecking=no ubuntu@$MASTER_IP "mkdir -p ~/app"
    
    # Upload backend code
    echo "Uploading backend code..."
    scp -r -o StrictHostKeyChecking=no ../backend/ ubuntu@$MASTER_IP:~/app/
    
    # Upload requirements
    scp -o StrictHostKeyChecking=no ../backend/requirements.txt ubuntu@$MASTER_IP:~/app/
    
    # Install Python dependencies
    echo "Installing Python dependencies on EC2..."
    ssh -o StrictHostKeyChecking=no ubuntu@$MASTER_IP "
        cd ~/app
        python3 -m pip install -r requirements.txt
        echo 'Python dependencies installed successfully'
    "
}

# Setup environment variables on EC2
setup_environment() {
    echo "Setting up environment variables..."
    
    MASTER_IP=$(cd $TERRAFORM_DIR && terraform output -raw spark_master_public_ip)
    S3_BUCKET=$(cd $TERRAFORM_DIR && terraform output -raw s3_bucket_name)
    
    # Create environment file
    ssh -o StrictHostKeyChecking=no ubuntu@$MASTER_IP "
        cat > ~/app/.env << EOF
FLASK_ENV=production
FLASK_DEBUG=false
PORT=5000
HOST=0.0.0.0
LOG_LEVEL=INFO

SPARK_MASTER_URL=spark://localhost:7077
S3_BUCKET=$S3_BUCKET
AWS_REGION=us-west-2

ENVIRONMENT=production
EOF
        echo 'Environment file created'
    "
}

# Create systemd service for the application
setup_systemd_service() {
    echo "Setting up systemd service..."
    
    MASTER_IP=$(cd $TERRAFORM_DIR && terraform output -raw spark_master_public_ip)
    
    ssh -o StrictHostKeyChecking=no ubuntu@$MASTER_IP "
        sudo tee /etc/systemd/system/spark-api.service << EOF
[Unit]
Description=Spark API Service
After=network.target spark-master.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/app
Environment=PATH=/usr/bin:/usr/local/bin
ExecStart=/usr/bin/python3 app_main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

        sudo systemctl daemon-reload
        sudo systemctl enable spark-api
        sudo systemctl start spark-api
        
        echo 'Systemd service created and started'
    "
}

# Test the deployment
test_deployment() {
    echo "Testing deployment..."
    
    MASTER_IP=$(cd $TERRAFORM_DIR && terraform output -raw spark_master_public_ip)
    
    # Test Spark Web UI
    echo "Testing Spark Web UI at http://$MASTER_IP:8080"
    
    # Test API health check
    echo "Testing API health check..."
    if curl -f "http://$MASTER_IP:5000/health" > /dev/null 2>&1; then
        echo "✓ API health check passed"
    else
        echo "✗ API health check failed"
    fi
    
    # Test Spark health check
    echo "Testing Spark health check..."
    if curl -f "http://$MASTER_IP:5000/api/spark/health" > /dev/null 2>&1; then
        echo "✓ Spark health check passed"
    else
        echo "✗ Spark health check failed"
    fi
}

# Cleanup function
cleanup() {
    echo "Do you want to destroy the infrastructure? (y/N)"
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        cd $TERRAFORM_DIR
        terraform destroy -auto-approve
        cd ..
        echo "Infrastructure destroyed"
    fi
}

# Main deployment flow
main() {
    case "${1:-deploy}" in
        "deploy")
            check_dependencies
            setup_ssh_key
            deploy_infrastructure
            upload_application
            setup_environment
            setup_systemd_service
            test_deployment
            echo ""
            echo "=== Deployment Summary ==="
            echo "Infrastructure deployed successfully!"
            echo "Access your Spark cluster at: $(cd $TERRAFORM_DIR && terraform output -raw spark_master_web_ui)"
            echo "API endpoint: http://$(cd $TERRAFORM_DIR && terraform output -raw spark_master_public_ip):5000"
            ;;
        "destroy")
            cleanup
            ;;
        "test")
            test_deployment
            ;;
        *)
            echo "Usage: $0 [deploy|destroy|test]"
            echo "  deploy  - Deploy the infrastructure and application"
            echo "  destroy - Destroy the infrastructure"
            echo "  test    - Test the deployed application"
            exit 1
            ;;
    esac
}

main "$@"