@echo off
echo 正在安装 PyInstaller...
pip install pyinstaller

echo 开始打包...
pyinstaller ^
  --name 连板 ^
  --windowed ^
  --onedir ^
  --collect-all akshare ^
  --collect-data akshare ^
  --hidden-import PyQt6.sip ^
  --hidden-import PyQt6.QtCore ^
  --hidden-import PyQt6.QtWidgets ^
  --hidden-import PyQt6.QtGui ^
  --hidden-import bs4 ^
  --hidden-import lxml ^
  --hidden-import lxml.etree ^
  --hidden-import lxml._elementpath ^
  --hidden-import tqdm ^
  --hidden-import requests ^
  --hidden-import sqlite3 ^
  --add-data "app.py;." ^
  app.py

echo.
echo 打包完成！exe 在 dist\连板\ 文件夹中
pause
