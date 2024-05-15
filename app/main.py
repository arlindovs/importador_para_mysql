# main.py
import fdb
import mysql.connector
import subprocess
import tkinter as tk

from datetime import datetime
from mysql.connector import errors

from gui import create_gui


def main():
    create_gui()

if __name__ == "__main__":
    main()
