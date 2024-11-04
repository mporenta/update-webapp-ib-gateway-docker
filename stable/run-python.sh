
#!/bin/sh
sleep 60 
python3 /home/tbot/develop/github/portfolio-monitor/src/run_pnl_monitor.py
sleep 60
python3 /home/tbot/develop/github/portfolio-monitor/src/pnl_web_service.py
