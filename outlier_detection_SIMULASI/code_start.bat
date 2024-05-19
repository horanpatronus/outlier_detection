@echo off
start python streaming/producer.py
timeout /T 0 /nobreak > NUL
start python streaming/kafka.py