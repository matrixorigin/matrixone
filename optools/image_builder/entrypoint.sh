#!/bin/bash

MATRIXONE_LOG="/var/log/mo-server.log"

cat << EOF > /opt/matrixone/mo-logrotate.conf
$MATRIXONE_LOG
{
    daily
    rotate 60
    size 100M
    compress
    delaycompress
    dateext
    missingok
    notifyempty
    sharedscripts
    postrotate
        cat /dev/null > $MATRIXONE_LOG
    endscript
}
EOF

if crontab -l | grep mo-logrotate.conf >/dev/null 2>&1; then
	echo "logratate alreay set."
else
	crontab <<-EOF
`crontab -l` 
00 * * * * /usr/sbin/logrotate -vf /opt/matrixone/mo-logrotate.conf
EOF
fi

[[ -f $MATRIXONE_LOG ]] || touch $MATRIXONE_LOG
nohup ./mo-server system_vars_config.toml >$MATRIXONE_LOG 2>&1 &
tail -f $MATRIXONE_LOG

exit 0
