# Data Warehouse ETL Infrastructure

> Source: Confluence - https://filterbuy.atlassian.net/wiki/spaces/FA/pages/389447683

## Infrastructure

- **ETL Platform:** Matillion ETL (https://matillion.filterbuy.com)
- **Architecture:** Hybrid SaaS (Matillion on AWS EC2)
- **EC2 Instance:** `i-0a5182d61a22ede85`
- **Version:** 1.74.5 (CRITICAL: Will cease to work after April 1, 2026)

### AWS Resources

| Resource | Details |
|----------|---------|
| EC2 Instance | [i-0a5182d61a22ede85](https://us-east-1.console.aws.amazon.com/ec2/home?region=us-east-1#InstanceDetails:instanceId=i-0a5182d61a22ede85) |
| S3 Bucket | [filterbuy-datawarehouse](https://us-east-1.console.aws.amazon.com/s3/buckets/filterbuy-datawarehouse) |
| Redshift Cluster | [redshift-cluster-filterbuy](https://us-east-1.console.aws.amazon.com/redshiftv2/home?region=us-east-1#/cluster-details?cluster=redshift-cluster-filterbuy) |
| SFTP Server | [s-16506baa258543209](https://us-east-2.console.aws.amazon.com/transfer/home?region=us-east-2#/servers/s-16506baa258543209) |

All Analytics resources tagged: `app:analytics`

## SSH Access to Matillion EC2

```bash
# 1. Get SSH key from AWS Secrets Manager
# Secret: analytics/matillion_ec2_key
# Save as .pem file locally

# 2. Connect
ssh -i "ec2-matillion.pem" root@ec2-44-193-88-225.compute-1.amazonaws.com

# 3. Restart Matillion service (if needed)
sudo service tomcat restart
```

## Data Source Integrations

| Source | Method | Auth |
|--------|--------|------|
| Google Sheets | OAuth component | reporting@filterbuy.com |
| Supplybuy | RDS Query | postgres user, Password Manager |
| Airfilterbuy | RDS Query | airfilterbuy / analytics users |
| QuickBooks | OAuth | david.ansel@filterbuy.com (RISK) |
| Routable | API Bearer Token | david.ansel@filterbuy.com (RISK) |
| Paylocity | SFTP | AWS Transfer Family |
| Quick Suite | Python + AWS API | EC2 instance credentials |
| Pipedrive | Python API | reporting@filterbuy.com |
| UPS | EDI via SFTP | Complex Python parser |
| Keepa | Python API | API key in job variable |

**WARNING:** QuickBooks and Routable integrations use David Ansel's account - may break if deactivated.

## Table Naming Standards

| Prefix | Source |
|--------|--------|
| `ab_` | Airfilterbuy |
| `dw_` | Data warehouse derived |
| `gs_` | Google Sheets |
| `ka_` | Keepa |
| `pd_` | Pipedrive |
| `ps_` | Production Scheduling Tool |
| `qa_` | Quality Assurance |
| `qbo_` | QuickBooks |
| `qs_` | Quick Suite |
| `rtb_` | Routable |
| `sb_` | Supplybuy |
| `ups_` | UPS |

## Scheduled Jobs

| Job | Purpose | Criticality |
|-----|---------|-------------|
| Incremental | Main DWH update | CRITICAL |
| IntraDay | Operations data | Medium |
| Keepa Data Extraction | Keepa data (12-14 hours) | Low |
| Production Schedule | Manufacturing schedules | CRITICAL |

## Credentials

Stored in AWS Secrets Manager with prefix `analytics/` and tag `app:analytics`.

## Contacts

- **Enterprise Account Executive:** Yuriy Moskovoy (yuriy.moskovoy@matillion.com)
- **Technical Account Manager:** Kevin Kirkpatrick (kevin.kirkpatrick@matillion.com)

## Backups

Daily snapshots via Matillion built-in backup feature, stored in AWS.
