#!/bin/bash

# Fraud Data Pipeline - Deployment Script (template-aligned: under scripts/)
# Usage: ./scripts/deploy.sh [dev|prod] [validate|deploy|run|destroy]

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Default values
TARGET="${1:-dev}"
ACTION="${2:-deploy}"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Fraud Data Pipeline - Asset Bundle${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${YELLOW}Target:${NC} $TARGET"
echo -e "${YELLOW}Action:${NC} $ACTION"
echo ""

case $ACTION in
  validate)
    echo -e "${GREEN}Validating bundle configuration...${NC}"
    databricks bundle validate -t "$TARGET"
    echo -e "${GREEN}✓ Validation complete${NC}"
    ;;
    
  deploy)
    echo -e "${GREEN}Validating bundle...${NC}"
    databricks bundle validate -t "$TARGET"
    echo ""
    echo -e "${GREEN}Deploying to $TARGET...${NC}"
    databricks bundle deploy -t "$TARGET"
    echo -e "${GREEN}✓ Deployment complete${NC}"
    ;;
    
  run)
    echo -e "${GREEN}Running fraud_data_pipeline in $TARGET...${NC}"
    databricks bundle run fraud_data_pipeline -t "$TARGET"
    echo -e "${GREEN}✓ Job triggered${NC}"
    ;;
    
  destroy)
    echo -e "${RED}WARNING: This will destroy all resources in $TARGET${NC}"
    read -p "Are you sure? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
      databricks bundle destroy -t "$TARGET"
      echo -e "${GREEN}✓ Resources destroyed${NC}"
    else
      echo -e "${YELLOW}Cancelled${NC}"
    fi
    ;;
    
  *)
    echo -e "${RED}Unknown action: $ACTION${NC}"
    echo ""
    echo "Usage: ./scripts/deploy.sh [dev|prod] [validate|deploy|run|destroy]"
    echo ""
    echo "Examples:"
    echo "  ./scripts/deploy.sh dev deploy     # Deploy to dev environment"
    echo "  ./scripts/deploy.sh prod deploy    # Deploy to prod environment"
    echo "  ./scripts/deploy.sh dev run        # Run pipeline in dev"
    echo "  ./scripts/deploy.sh dev validate   # Validate configuration"
    exit 1
    ;;
esac

echo ""
echo -e "${BLUE}========================================${NC}"
