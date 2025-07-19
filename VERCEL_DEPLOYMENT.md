# Vercel Deployment Instructions

## Prerequisites
- Vercel CLI installed: `npm i -g vercel`
- Vercel account connected to your GitHub repository

## Deployment Steps

1. **Install Vercel CLI** (if not already installed):
   ```bash
   npm install -g vercel
   ```

2. **Login to Vercel**:
   ```bash
   vercel login
   ```

3. **Deploy from the root directory**:
   ```bash
   vercel
   ```

4. **Follow the prompts**:
   - Set up and deploy? **Y**
   - Which scope? Select your account
   - Link to existing project? **N** (first time)
   - What's your project's name? **db-analytics-system**
   - In which directory is your code located? **.**

## Configuration Files Added

- `vercel.json` - Main Vercel configuration
- `backend/requirements.txt` - Python dependencies for the Flask backend
- Updated `frontend/package.json` with vercel-build script

## Important Notes

⚠️ **Spark Limitation**: PySpark is not supported on Vercel serverless functions due to JVM requirements. Consider these alternatives:

1. **For Analytics**: Use pandas for data processing instead of Spark
2. **External Spark**: Deploy Spark backend separately (AWS EMR, Databricks, etc.)
3. **Alternative Platforms**: Consider Railway, Render, or AWS for full Spark support

## Environment Variables

Set these in Vercel dashboard if needed:
- Database connection strings
- API keys
- Other configuration variables

## Manual Deployment

Alternatively, connect your GitHub repository to Vercel dashboard for automatic deployments on push.