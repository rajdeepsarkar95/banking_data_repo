# Databricks Asset Bundle Setup
ECHO is on.
## Prerequisites
ECHO is on.
1. Install Databricks CLI:
   \`\`\`bash
   pip install databricks-cli
   \`\`\`
ECHO is on.
2. Configure Databricks CLI:
   \`\`\`bash
   databricks configure --token
   \`\`\`
ECHO is on.
3. Install Databricks Asset Bundle plugin:
   \`\`\`bash
   databricks bundles install
   \`\`\`
ECHO is on.
## Deployment Commands
ECHO is on.
### Validate Bundle
\`\`\`bash
cd databricks/bundles/banking_data
databricks bundle validate
\`\`\`
ECHO is on.
### Deploy to Development
\`\`\`bash
databricks bundle deploy -t development
\`\`\`
ECHO is on.
### Deploy to Staging
\`\`\`bash
databricks bundle deploy -t staging
\`\`\`
ECHO is on.
### Deploy to Production
\`\`\`bash
databricks bundle deploy -t production
\`\`\`
ECHO is on.
### Run Job Manually
\`\`\`bash
databricks bundle run -t development
\`\`\`
ECHO is on.
## Configuration
ECHO is on.
Update the following in \`databricks.yml\`:
- Replace \`your-development-cluster-id\` with your actual cluster ID
- Update cluster configurations for each environment
- Adjust schedule cron expressions as needed
ECHO is on.
## GitHub Secrets Required
- \`DATABRICKS_HOST\`: Your Databricks workspace URL
- \`DATABRICKS_TOKEN\`: Your Databricks personal access token
