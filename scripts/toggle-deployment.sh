#!/bin/bash

# Toggle Deployment Mode Script
# Switches between local_small and aws_large deployment modes

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BACKEND_DIR="../backend"
ENV_FILE="../.env"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}    DB Spark Deployment Mode Toggle    ${NC}"
echo -e "${BLUE}========================================${NC}"

# Check if .env file exists
if [ ! -f "$ENV_FILE" ]; then
    echo -e "${YELLOW}No .env file found. Creating from .env.example...${NC}"
    if [ -f "../.env.example" ]; then
        cp "../.env.example" "$ENV_FILE"
        echo -e "${GREEN}.env file created from .env.example${NC}"
    else
        echo -e "${RED}Error: .env.example not found${NC}"
        exit 1
    fi
fi

# Get current deployment mode
get_current_mode() {
    if grep -q "DEPLOYMENT_MODE=" "$ENV_FILE"; then
        grep "DEPLOYMENT_MODE=" "$ENV_FILE" | cut -d'=' -f2 | tr -d '"' | tr -d "'"
    else
        echo "local_small"
    fi
}

# Set deployment mode in .env file
set_deployment_mode() {
    local new_mode=$1
    
    if grep -q "DEPLOYMENT_MODE=" "$ENV_FILE"; then
        # Update existing line
        sed -i.bak "s/DEPLOYMENT_MODE=.*/DEPLOYMENT_MODE=$new_mode/" "$ENV_FILE"
    else
        # Add new line
        echo "DEPLOYMENT_MODE=$new_mode" >> "$ENV_FILE"
    fi
    
    # Remove backup file if created
    [ -f "$ENV_FILE.bak" ] && rm "$ENV_FILE.bak"
}

# Display current configuration
show_current_config() {
    local mode=$1
    echo -e "\n${BLUE}Current Configuration:${NC}"
    echo -e "Mode: ${YELLOW}$mode${NC}"
    
    case $mode in
        "local_small")
            echo -e "• Spark: ${GREEN}Local cluster (minimal resources)${NC}"
            echo -e "• Data: ${GREEN}Local file system${NC}"
            echo -e "• Memory: ${GREEN}512MB driver, 512MB executor${NC}"
            echo -e "• S3: ${YELLOW}Disabled${NC}"
            echo -e "• Best for: ${GREEN}Development, testing${NC}"
            ;;
        "aws_large")
            echo -e "• Spark: ${GREEN}AWS EC2 cluster${NC}"
            echo -e "• Data: ${GREEN}S3 storage${NC}"
            echo -e "• Memory: ${GREEN}4GB driver, 8GB executor${NC}"
            echo -e "• S3: ${GREEN}Enabled${NC}"
            echo -e "• Best for: ${GREEN}Production workloads${NC}"
            ;;
        *)
            echo -e "• ${RED}Unknown mode${NC}"
            ;;
    esac
}

# Toggle mode
toggle_mode() {
    local current_mode=$1
    
    case $current_mode in
        "local_small")
            echo "aws_large"
            ;;
        "aws_large")
            echo "local_small"
            ;;
        *)
            echo "local_small"
            ;;
    esac
}

# Validate AWS configuration for aws_large mode
validate_aws_config() {
    echo -e "\n${BLUE}Validating AWS Configuration...${NC}"
    
    local missing_vars=()
    
    # Check for required AWS environment variables
    if ! grep -q "S3_BUCKET=" "$ENV_FILE" || [ -z "$(grep "S3_BUCKET=" "$ENV_FILE" | cut -d'=' -f2)" ]; then
        missing_vars+=("S3_BUCKET")
    fi
    
    if ! grep -q "AWS_REGION=" "$ENV_FILE" || [ -z "$(grep "AWS_REGION=" "$ENV_FILE" | cut -d'=' -f2)" ]; then
        missing_vars+=("AWS_REGION")
    fi
    
    if [ ${#missing_vars[@]} -gt 0 ]; then
        echo -e "${YELLOW}Warning: The following AWS configuration variables are missing:${NC}"
        for var in "${missing_vars[@]}"; do
            echo -e "  • ${RED}$var${NC}"
        done
        echo -e "\n${YELLOW}Please configure these in $ENV_FILE before using aws_large mode${NC}"
        return 1
    else
        echo -e "${GREEN}AWS configuration looks good!${NC}"
        return 0
    fi
}

# Main execution
main() {
    current_mode=$(get_current_mode)
    
    show_current_config "$current_mode"
    
    # Get target mode
    if [ "$1" = "auto" ]; then
        new_mode=$(toggle_mode "$current_mode")
        echo -e "\n${BLUE}Auto-toggling to: ${YELLOW}$new_mode${NC}"
    elif [ -n "$1" ]; then
        new_mode="$1"
        if [ "$new_mode" != "local_small" ] && [ "$new_mode" != "aws_large" ]; then
            echo -e "${RED}Error: Invalid mode '$new_mode'. Use 'local_small' or 'aws_large'${NC}"
            exit 1
        fi
    else
        # Interactive mode
        echo -e "\n${BLUE}Available modes:${NC}"
        echo -e "1) ${GREEN}local_small${NC} - Development mode with local Spark"
        echo -e "2) ${GREEN}aws_large${NC} - Production mode with AWS EC2 Spark cluster"
        echo -e "3) ${YELLOW}Toggle${NC} - Switch to the other mode"
        echo -e "4) ${YELLOW}Cancel${NC}"
        
        read -p "$(echo -e "\nSelect option (1-4): ")" choice
        
        case $choice in
            1)
                new_mode="local_small"
                ;;
            2)
                new_mode="aws_large"
                ;;
            3)
                new_mode=$(toggle_mode "$current_mode")
                ;;
            4)
                echo -e "${YELLOW}Cancelled${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}Invalid choice${NC}"
                exit 1
                ;;
        esac
    fi
    
    if [ "$current_mode" = "$new_mode" ]; then
        echo -e "\n${YELLOW}Already in $new_mode mode. No changes needed.${NC}"
        exit 0
    fi
    
    # Validate AWS config if switching to aws_large
    if [ "$new_mode" = "aws_large" ]; then
        if ! validate_aws_config; then
            echo -e "\n${RED}Cannot switch to aws_large mode due to missing AWS configuration${NC}"
            exit 1
        fi
    fi
    
    # Make the change
    echo -e "\n${BLUE}Switching from ${YELLOW}$current_mode${BLUE} to ${YELLOW}$new_mode${NC}"
    set_deployment_mode "$new_mode"
    
    echo -e "${GREEN}✓ Deployment mode updated in $ENV_FILE${NC}"
    
    # Show new configuration
    show_current_config "$new_mode"
    
    echo -e "\n${BLUE}Next Steps:${NC}"
    echo -e "1. ${YELLOW}Restart your backend application${NC}"
    echo -e "   cd $BACKEND_DIR && python app_main.py"
    echo -e ""
    echo -e "2. ${YELLOW}Or use the API to restart Spark session:${NC}"
    echo -e "   curl -X POST http://localhost:5000/api/deployment/restart-spark"
    echo -e ""
    
    if [ "$new_mode" = "aws_large" ]; then
        echo -e "3. ${YELLOW}Deploy AWS infrastructure if not already done:${NC}"
        echo -e "   cd ../aws-infrastructure && ./deploy.sh deploy"
        echo -e ""
    fi
    
    echo -e "${GREEN}Deployment mode switch completed!${NC}"
}

# Help function
show_help() {
    echo "Usage: $0 [MODE|auto]"
    echo ""
    echo "Modes:"
    echo "  local_small  - Switch to local development mode"
    echo "  aws_large    - Switch to AWS production mode"
    echo "  auto         - Toggle between modes automatically"
    echo ""
    echo "Examples:"
    echo "  $0                    # Interactive mode"
    echo "  $0 local_small        # Switch to local mode"
    echo "  $0 aws_large          # Switch to AWS mode"
    echo "  $0 auto               # Toggle current mode"
    echo ""
}

# Handle command line arguments
case "${1:-}" in
    -h|--help|help)
        show_help
        exit 0
        ;;
    *)
        main "$@"
        ;;
esac