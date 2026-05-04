#!/bin/bash
pkill -f "Xvfb.*:99" 2>/dev/null
pkill -f x11vnc 2>/dev/null
pkill -f "login_manual.py\|renew_google_session" 2>/dev/null
sleep 1
Xvfb :99 -screen 0 1280x900x24 &
sleep 2
x11vnc -display :99 -forever -shared -rfbport 5900 -quiet &
sleep 1
echo "Xvfb e VNC prontos em :99 (porta 5900)"
