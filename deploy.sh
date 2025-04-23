#!/bin/bash
set -e

# 프로젝트 디렉토리 생성
ssh ec2 "mkdir -p ~/funding-alert-bot"

# 파일 복사
scp -r ./* ec2:~/funding-alert-bot/

# 필요한 패키지 설치
ssh ec2 "cd ~/funding-alert-bot && pip3 install -r requirements.txt"

# 환경 변수 설정
ssh ec2 "echo 'TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}' > ~/funding-alert-bot/.env"
ssh ec2 "echo 'TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}' >> ~/funding-alert-bot/.env"

# systemd 서비스 파일 생성
ssh ec2 "cat > /tmp/funding-alert.service" <<'EOF'
[Unit]
Description=Funding Rate Alert Bot
After=network.target

[Service]
Type=oneshot
User=ec2-user
WorkingDirectory=/home/ec2-user/funding-alert-bot
ExecStart=/usr/bin/python3 /home/ec2-user/funding-alert-bot/funding_alert_bot.py

[Install]
WantedBy=multi-user.target
EOF
ssh ec2 "sudo mv /tmp/funding-alert.service /etc/systemd/system/funding-alert.service"

# systemd 타이머 파일 생성
ssh ec2 "cat > /tmp/funding-alert.timer" <<'EOF'
[Unit]
Description=Run Funding Alert Bot at 35 and 55 minutes past every hour

[Timer]
OnCalendar=*-*-* *:35:00
OnCalendar=*-*-* *:55:00
Persistent=true

[Install]
WantedBy=timers.target
EOF
ssh ec2 "sudo mv /tmp/funding-alert.timer /etc/systemd/system/funding-alert.timer"

# 서비스 및 타이머 활성화
ssh ec2 "sudo systemctl daemon-reload"
ssh ec2 "sudo systemctl enable funding-alert.timer"
ssh ec2 "sudo systemctl start funding-alert.timer"
ssh ec2 "sudo systemctl start funding-alert.service"
