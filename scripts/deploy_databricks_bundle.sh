#!/bin/bash
# Databricks Asset Bundle Deployment Script
ECHO is on.
set -e
ECHO is on.
ENVIRONMENT=\${1:-development}
ECHO is on.
echo "Deploying Databricks Asset Bundle for environment: \$ENVIRONMENT"
ECHO is on.
# Navigate to bundle directory
cd databricks/bundles/banking_data
ECHO is on.
# Validate bundle configuration
echo "Validating bundle configuration..."
databricks bundle validate
ECHO is on.
# Deploy the bundle
echo "Deploying to \$ENVIRONMENT environment..."
databricks bundle deploy -t \$ENVIRONMENT
ECHO is on.
# Test deployment by running the job
if [ "\$ENVIRONMENT" != "production" ]; then
    echo "Testing deployment with a manual run..."
    databricks bundle run -t \$ENVIRONMENT
fi
ECHO is on.
echo "Deployment completed successfully!"
