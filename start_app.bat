@echo off
title Tactix App Launcher
echo ===================================================
echo ⚽ TACTIX: Sistema de Analisis en Vivo y Tactico
echo ===================================================
echo.
echo [1/2] Iniciando Backend (FastAPI)...
start "Tactix Backend" cmd /k ".\venvfutbol\Scripts\python.exe -m uvicorn src.api.main:app --host 127.0.0.1 --port 8000"

echo [2/2] Iniciando Frontend (React/Vite)...
start "Tactix Frontend" cmd /k "cd frontend && npm run dev"

echo.
echo ===================================================
echo 🟢 Ambos servicios estan arrancando en ventanas separadas.
echo.
echo - Backend API: http://127.0.0.1:8000
echo - Frontend App: http://localhost:5173/
echo ===================================================
echo.
pause
