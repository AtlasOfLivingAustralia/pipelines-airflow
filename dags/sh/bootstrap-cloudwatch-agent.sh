#!/bin/bash
# Bootstrap script to install and start the Amazon CloudWatch Agent on EMR cluster nodes.
# This script requires no arguments; it always installs the CloudWatch Agent with a
# default configuration collecting CPU, memory, disk, and swap metrics at 60s interval.

set -euo pipefail

TMP_DIR="/tmp/cwagent"
CONFIG_PATH="/tmp/amazon-cloudwatch-agent.json"

log() { echo "[cloudwatch-agent-bootstrap] $*"; }

log "Installing CloudWatch Agent with default config (no arguments expected)"

# Install dependencies
if ! command -v amazon-cloudwatch-agent-ctl >/dev/null 2>&1; then
  log "Installing CloudWatch Agent"
  if [[ $EUID -ne 0 ]]; then
    sudo yum install -y amazon-cloudwatch-agent || true
  else
    yum install -y amazon-cloudwatch-agent || true
  fi
fi

mkdir -p "$TMP_DIR"

log "Generating default CloudWatch Agent config"
cat > "$CONFIG_PATH" <<'EOF'
{
  "agent": {
    "metrics_collection_interval": 30,
    "logfile": "/var/log/amazon-cloudwatch-agent.log"
  },
  "metrics": {
    "append_dimensions": {
      "InstanceId": "${aws:InstanceId}",
      "InstanceType": "${aws:InstanceType}"
    },
    "metrics_collected": {
      "cpu": {"resources": ["*"], "measurement": ["cpu_usage_idle", "cpu_usage_iowait", "cpu_usage_system", "cpu_usage_user"], "totalcpu": true},
      "mem": {"measurement": ["mem_used_percent", "mem_available", "mem_used"], "metrics_collection_interval": 30},
      "disk": {"resources": ["/"], "measurement": ["used_percent", "inodes_free"], "ignore_file_system_types": ["sysfs", "devtmpfs", "overlay"]},
      "swap": {"measurement": ["swap_used", "swap_used_percent"]}
    }
  }
}
EOF


log "Setting permissions and staging config"
if [[ $EUID -ne 0 ]]; then
  sudo cp "$CONFIG_PATH" /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
  sudo chown root:root /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
else
  cp "$CONFIG_PATH" /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
  chown root:root /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
fi

log "Starting CloudWatch Agent"
if [[ $EUID -ne 0 ]]; then
  sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
    -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s || {
      log "First start attempt failed; retrying after 5s"; sleep 5;
      sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s;
    }
  sudo systemctl enable amazon-cloudwatch-agent || true
  sudo systemctl status amazon-cloudwatch-agent --no-pager || true
else
  /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
    -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s || {
      log "First start attempt failed; retrying after 5s"; sleep 5;
      /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s;
    }
  systemctl enable amazon-cloudwatch-agent || true
  systemctl status amazon-cloudwatch-agent --no-pager || true
fi

log "CloudWatch Agent bootstrap complete"
